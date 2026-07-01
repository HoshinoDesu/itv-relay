//! 可清空的流式 buffer: session 写入, axum Body 读取, 切档时可清空积压。
//! 同时测量真实 drain 速率 (reader 取走的字节 EWMA), 作为拥塞真实信号。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

struct Inner {
    queue: Mutex<VecDeque<Vec<u8>>>,
    closed: AtomicBool,
    notify: Notify,
    /// reader 累计取走的字节 (诊断用)
    drained_bytes: AtomicU64,
    /// pool 当前字节总量 (供 state 算斜率 d/dt = 诚实拥塞信号)
    backlog_bytes: AtomicU64,
}

pub struct StreamBuf;

impl StreamBuf {
    pub fn new() -> (StreamWriter, StreamReader) {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::new()),
            closed: AtomicBool::new(false),
            notify: Notify::new(),
            drained_bytes: AtomicU64::new(0),
            backlog_bytes: AtomicU64::new(0),
        });
        (
            StreamWriter { inner: inner.clone() },
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

/// 队列容量 (chunk 数)。背压用: 超过则阻塞 writer。
pub const BUF_CAP: usize = 64;

impl StreamWriter {
    pub async fn send(&self, data: Vec<u8>) -> bool {
        loop {
            if self.inner.closed.load(Ordering::Acquire) {
                return false;
            }
            {
                let mut q = self.inner.queue.lock().await;
                if q.len() < BUF_CAP {
                    self.inner.backlog_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                    q.push_back(data);
                    self.inner.notify.notify_waiters();
                    return true;
                }
            }
            self.inner.notify.notified().await;
        }
    }

    pub async fn clear(&self) -> usize {
        let mut q = self.inner.queue.lock().await;
        let n = q.len();
        // 清空时 backlog_bytes 归零
        self.inner.backlog_bytes.store(0, Ordering::Relaxed);
        q.clear();
        self.inner.notify.notify_waiters();
        n
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
        self.inner.notify.notify_waiters();
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
                    self.inner.notify.notify_waiters();
                    return Some(data);
                }
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return None;
            }
            self.inner.notify.notified().await;
        }
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }
}
