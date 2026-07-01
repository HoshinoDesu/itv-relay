//! 单个播放会话: ffmpeg pipe → HTTP 响应, 含动态档位切换。
//!
//! v6 策略 (无卡顿切档):
//! - 新 ffmpeg 在后台 spawn + 等首帧, 主循环持续读旧 ffmpeg 不中断
//! - 新进程就绪后无缝替换 active child, 不清空 pool
//! - MPEG-TS 自同步 + -copyts 保证 PTS 连续, 播放器无感切换

use crate::config::{Config, Run};
use crate::ffmpeg;
use crate::state::{CongestionCfg, Decision, StateMachine};
use crate::streambuf::StreamWriter;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

pub struct Session {
    pub channel_idx: usize,
    pub source_url: String,
    pub cfg: Arc<Config>,
}

impl Session {
    pub fn new(channel_idx: usize, source_url: String, cfg: Arc<Config>) -> Self {
        Self { channel_idx, source_url, cfg }
    }

    pub async fn run(self, writer: StreamWriter) {
        let cfg = self.cfg.clone();
        let source = self.source_url.clone();
        let channel_idx = self.channel_idx;
        let max_index = cfg.ladder.len().saturating_sub(1);

        let ladder_bps: Vec<u64> = cfg
            .ladder
            .iter()
            .map(|r| {
                if r.mode == "copy" {
                    800_000
                } else {
                    ((r.bitrate + r.audio_bitrate) * 1000 / 8) as u64
                }
            })
            .collect();

        let mut cur_ladder = cfg.startup_ladder.min(max_index);
        let mut sm = StateMachine::new(CongestionCfg::from(cfg.as_ref()), max_index, ladder_bps.clone(), cur_ladder);

        let mut active: Option<Child>;
        let mut active_bps = ladder_bps[cur_ladder];

        match spawn_and_rate(&cfg.ladder[cur_ladder], &source).await {
            Ok((c, bps)) => {
                active = Some(c);
                active_bps = bps;
                info!(target: "session", "tv-{} 档{} 启动 prod={}B/s", channel_idx, cur_ladder, active_bps);
            }
            Err(e) => {
                warn!(target: "session", "tv-{} 初始 spawn 失败: {e}", channel_idx);
                writer.close().await;
                return;
            }
        }

        let sample_iv = Duration::from_secs_f64(cfg.sample_interval_s.max(0.5));
        let mut last_sample = Instant::now();
        let mut prev_backlog_bytes: u64 = 0;

        let mut buf = vec![0u8; 64 * 1024];

        let mut pending_switch: Option<oneshot::Receiver<Result<(Child, Vec<u8>, u64)>>> = None;
        let mut pending_target: Option<usize> = None;

        loop {
            // 检查后台切档是否就绪
            if let Some(mut rx) = pending_switch.take() {
                match rx.try_recv() {
                    Ok(Ok((new_child, first_frame, rate))) => {
                        let t = pending_target.take().unwrap();
                        if let Some(mut old) = active.take() {
                            tokio::spawn(async move {
                                ffmpeg::kill_process_group(&mut old).await;
                            });
                        }
                        writer.send(first_frame).await;
                        active = Some(new_child);
                        cur_ladder = t;
                        active_bps = rate;
                        sm.confirm_switch();
                        info!(target: "session", "tv-{} 无缝切档完成 → 档{}", channel_idx, t);
                    }
                    Ok(Err(e)) => {
                        warn!(target: "session", "tv-{} 后台切档失败: {e}", channel_idx);
                        sm.rollback_switch();
                        pending_target = None;
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {
                        pending_switch = Some(rx);
                    }
                    Err(oneshot::error::TryRecvError::Closed) => {
                        warn!(target: "session", "tv-{} 切档任务异常退出", channel_idx);
                        sm.rollback_switch();
                        pending_target = None;
                    }
                }
            }

            let child = match active.as_mut() {
                Some(c) => c,
                None => break,
            };
            let n = match child.stdout.as_mut() {
                Some(stdout) => stdout.read(&mut buf).await,
                None => break,
            };
            match n {
                Ok(0) => {
                    info!(target: "session", "tv-{} ffmpeg stdout EOF, 会话结束", channel_idx);
                    break;
                }
                Ok(n) => {
                    let chunk = buf[..n].to_vec();
                    match tokio::time::timeout(Duration::from_millis(50), writer.send(chunk)).await {
                        Ok(false) => {
                            debug!(target: "session", "客户端断开, 终止会话");
                            break;
                        }
                        Ok(true) => {}
                        Err(_) => {} // pool 满超时, 丢帧继续采样
                    }
                }
                Err(e) => {
                    warn!(target: "session", "读 ffmpeg 失败: {e}");
                    break;
                }
            }

            // 切档进行中时跳过采样 (confirm_switch 会重置 EWMA)
            if pending_switch.is_some() {
                continue;
            }

            let now = Instant::now();
            if now.duration_since(last_sample) >= sample_iv {
                let dt = now.duration_since(last_sample);
                last_sample = now;

                let backlog_chunks = writer.backlog().await;
                let backlog_bytes = writer.backlog_bytes();
                let backlog_ratio = backlog_chunks as f64 / crate::streambuf::BUF_CAP as f64;
                let drained = writer.take_drained() as f64;
                let drain_bps = (drained / dt.as_secs_f64()) as u64;
                let backlog_delta = backlog_bytes as i64 - prev_backlog_bytes as i64;
                prev_backlog_bytes = backlog_bytes;

                let has_active = drained > 0.0 || backlog_chunks > 0;
                let sample = crate::congestion::Sample {
                    backlog_ratio,
                    backlog_bytes,
                    backlog_delta,
                    drain_bps,
                    prod_bps: active_bps,
                    has_active_clients: has_active,
                };

                tracing::debug!(target: "sample",
                    "tv-{} 档{} chunks={}/{} ({:.0}%) bytes={}B delta={}B drain={}B/s prod={}B/s",
                    channel_idx, cur_ladder, backlog_chunks, crate::streambuf::BUF_CAP,
                    backlog_ratio * 100.0, backlog_bytes, backlog_delta, drain_bps, active_bps);

                let decision = sm.update(&sample, dt, now);
                match decision {
                    Decision::StepDown(t) | Decision::RevertDown(t) => {
                        if t != cur_ladder {
                            let tag = matches!(decision, Decision::RevertDown(_));
                            info!(target: "session", "tv-{} {} {}→{} (drain={}B/s pool={:.0}%)",
                                channel_idx, if tag {"回退"} else {"降档"}, cur_ladder, t, drain_bps, backlog_ratio * 100.0);
                            start_bg_switch(&cfg.ladder[t], &source, &mut pending_switch, &mut pending_target, t);
                        }
                    }
                    Decision::StepUp(t) => {
                        if t != cur_ladder {
                            info!(target: "session", "tv-{} 升档探测 {}→{} (drain={}B/s)",
                                channel_idx, cur_ladder, t, drain_bps);
                            start_bg_switch(&cfg.ladder[t], &source, &mut pending_switch, &mut pending_target, t);
                        }
                    }
                    Decision::Hold => {}
                }
            }
        }

        if let Some(mut c) = active.take() {
            ffmpeg::kill_process_group(&mut c).await;
        }
        writer.close().await;
    }
}

fn start_bg_switch(
    run: &Run,
    source: &str,
    pending: &mut Option<oneshot::Receiver<Result<(Child, Vec<u8>, u64)>>>,
    target: &mut Option<usize>,
    ladder: usize,
) {
    let (tx, rx) = oneshot::channel();
    let run = run.clone();
    let src = source.to_string();
    tokio::spawn(async move {
        let _ = tx.send(prepare_switch(&run, &src).await);
    });
    *pending = Some(rx);
    *target = Some(ladder);
}

/// 后台准备新 ffmpeg: spawn + 等首帧, 不阻塞主读循环。
async fn prepare_switch(run: &Run, source: &str) -> Result<(Child, Vec<u8>, u64)> {
    let (mut child, rate) = spawn_and_rate(run, source).await?;
    let mut first = vec![0u8; 64 * 1024];
    let n = match child.stdout.as_mut() {
        Some(stdout) => tokio::time::timeout(Duration::from_secs(15), stdout.read(&mut first)).await,
        None => anyhow::bail!("no stdout"),
    };
    match n {
        Ok(Ok(n)) if n > 0 => Ok((child, first[..n].to_vec(), rate)),
        Ok(Ok(_)) => { ffmpeg::kill_process_group(&mut child).await; anyhow::bail!("EOF immediately"); }
        Ok(Err(e)) => { ffmpeg::kill_process_group(&mut child).await; anyhow::bail!("read err: {e}"); }
        Err(_) => { ffmpeg::kill_process_group(&mut child).await; anyhow::bail!("first frame timeout"); }
    }
}

async fn spawn_and_rate(run: &Run, source: &str) -> Result<(Child, u64)> {
    let mut cmd = ffmpeg::build_cmd(run, source);
    let child = cmd.spawn()?;
    let rate = if run.mode == "copy" { 800_000 } else { ((run.bitrate + run.audio_bitrate) * 1000 / 8) as u64 };
    Ok((child, rate))
}
