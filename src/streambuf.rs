//! 流式 buffer: session 写入, axum Body 读取, 切换到已预读的新档时可清掉旧积压。
//! 同时测量真实 drain 速率 (reader 取走的字节 EWMA), 作为拥塞真实信号。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

struct Inner {
    queue: Mutex<VecDeque<Vec<u8>>>,
    closed: AtomicBool,
    not_empty: Notify,
    not_full: Notify,
    /// reader 累计取走的字节 (诊断用)
    drained_bytes: AtomicU64,
    /// pool 当前字节总量 (供 state 算斜率 d/dt = 诚实拥塞信号)
    backlog_bytes: AtomicU64,
}

pub struct StreamBuf;

impl StreamBuf {
    pub fn channel() -> (StreamWriter, StreamReader) {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::new()),
            closed: AtomicBool::new(false),
            not_empty: Notify::new(),
            not_full: Notify::new(),
            drained_bytes: AtomicU64::new(0),
            backlog_bytes: AtomicU64::new(0),
        });
        (
            StreamWriter {
                inner: inner.clone(),
            },
            StreamReader { inner },
        )
    }
}

#[derive(Clone)]
pub struct StreamWriter {
    inner: Arc<Inner>,
}

pub struct StreamReader {
    inner: Arc<Inner>,
}

#[derive(Debug, PartialEq)]
pub enum SendResult {
    Sent,
    Closed,
    Full(Vec<u8>),
}

/// 队列容量 (chunk 数)。背压用: 超过则阻塞 writer。
pub const BUF_CAP: usize = 64;

impl StreamWriter {
    pub async fn send_timeout(&self, data: Vec<u8>, timeout: Duration) -> SendResult {
        loop {
            if self.inner.closed.load(Ordering::Acquire) {
                return SendResult::Closed;
            }
            {
                let mut q = self.inner.queue.lock().await;
                if q.len() < BUF_CAP {
                    self.inner
                        .backlog_bytes
                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                    q.push_back(data);
                    self.inner.not_empty.notify_one();
                    return SendResult::Sent;
                }
            }
            if tokio::time::timeout(timeout, self.inner.not_full.notified())
                .await
                .is_err()
            {
                return SendResult::Full(data);
            }
        }
    }

    pub async fn backlog(&self) -> usize {
        self.inner.queue.lock().await.len()
    }

    /// pool 当前字节总量 (供 state 算斜率)。无锁读 atomic。
    pub fn backlog_bytes(&self) -> u64 {
        self.inner.backlog_bytes.load(Ordering::Relaxed)
    }

    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
        self.inner.not_empty.notify_one();
        self.inner.not_full.notify_one();
    }

    pub async fn clear(&self) {
        let mut q = self.inner.queue.lock().await;
        q.clear();
        self.inner.backlog_bytes.store(0, Ordering::Relaxed);
        self.inner.not_full.notify_waiters();
        self.inner.not_full.notify_one();
    }

    /// 取走并清零 reader 累计字节, 用于 session 做周期 drain 速率 EWMA
    pub fn take_drained(&self) -> u64 {
        self.inner.drained_bytes.swap(0, Ordering::AcqRel)
    }
}

impl StreamReader {
    pub async fn recv(&self) -> Option<Vec<u8>> {
        loop {
            {
                let mut q = self.inner.queue.lock().await;
                if let Some(data) = q.pop_front() {
                    let len = data.len() as u64;
                    self.inner.drained_bytes.fetch_add(len, Ordering::Relaxed);
                    self.inner.backlog_bytes.fetch_sub(len, Ordering::Relaxed);
                    self.inner.not_full.notify_one();
                    return Some(data);
                }
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return None;
            }
            self.inner.not_empty.notified().await;
        }
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
        self.inner.not_empty.notify_one();
        self.inner.not_full.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_timeout_returns_full_without_dropping_chunk() {
        let (writer, _reader) = StreamBuf::channel();
        for _ in 0..BUF_CAP {
            assert_eq!(
                writer
                    .send_timeout(vec![1; 4], Duration::from_millis(5))
                    .await,
                SendResult::Sent
            );
        }

        let chunk = vec![9; 3];
        let result = writer
            .send_timeout(chunk.clone(), Duration::from_millis(5))
            .await;

        assert_eq!(result, SendResult::Full(chunk));
        assert_eq!(writer.backlog().await, BUF_CAP);
    }

    #[tokio::test]
    async fn send_timeout_reports_closed_reader() {
        let (writer, reader) = StreamBuf::channel();
        drop(reader);

        let result = writer.send_timeout(vec![1], Duration::from_millis(5)).await;

        assert_eq!(result, SendResult::Closed);
    }

    #[tokio::test]
    async fn send_timeout_unblocks_when_reader_drains_queue() {
        let (writer, reader) = StreamBuf::channel();
        for _ in 0..BUF_CAP {
            assert_eq!(
                writer
                    .send_timeout(vec![1; 4], Duration::from_millis(5))
                    .await,
                SendResult::Sent
            );
        }

        let waiting_writer = writer.clone();
        let send_task = tokio::spawn(async move {
            waiting_writer
                .send_timeout(vec![9; 3], Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;

        assert_eq!(reader.recv().await.as_deref(), Some(&[1, 1, 1, 1][..]));

        assert_eq!(send_task.await.unwrap(), SendResult::Sent);
        assert_eq!(writer.backlog().await, BUF_CAP);
    }

    #[tokio::test]
    async fn clear_drops_queue_and_resets_backlog_bytes() {
        let (writer, reader) = StreamBuf::channel();
        assert_eq!(
            writer
                .send_timeout(vec![1; 10], Duration::from_millis(5))
                .await,
            SendResult::Sent
        );
        assert_eq!(
            writer
                .send_timeout(vec![2; 20], Duration::from_millis(5))
                .await,
            SendResult::Sent
        );
        assert_eq!(writer.backlog().await, 2);
        assert_eq!(writer.backlog_bytes(), 30);

        writer.clear().await;

        assert_eq!(writer.backlog().await, 0);
        assert_eq!(writer.backlog_bytes(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), reader.recv())
                .await
                .is_err()
        );
    }
}
