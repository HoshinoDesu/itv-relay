//! 单个播放会话: ffmpeg pipe → HTTP 响应, 含动态档位切换。
//!
//! v5 策略 (pool 字节斜率驱动):
//! - Direct(直通): 测链路 link_estimate, 首降按带宽直跳合适档
//! - Encode: 降只单步; 升单步试探 + 观察期回退到 last_stable + 退避锁
//! - 切档统一走 hot_switch (spawn新→等首帧→clear池→kill旧)
//! - 连续 3 次升档失败 → 暂停升档 30min

use crate::config::{Config, Run};
use crate::ffmpeg;
use crate::state::{CongestionCfg, Decision, StateMachine};
use crate::streambuf::StreamWriter;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::Child;
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
                    800_000 // 直通源 ~7.8Mbps 取保守
                } else {
                    ((r.bitrate + r.audio_bitrate) * 1000 / 8) as u64
                }
            })
            .collect();

        let mut cur_ladder = cfg.startup_ladder.min(max_index);
        let mut sm = StateMachine::new(CongestionCfg::from(cfg.as_ref()), max_index, ladder_bps.clone(), cur_ladder);

        // active ffmpeg + 当前档
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

        loop {
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
                    if !writer.send(chunk).await {
                        debug!(target: "session", "客户端断开, 终止会话");
                        break;
                    }
                }
                Err(e) => {
                    warn!(target: "session", "读 ffmpeg 失败: {e}");
                    break;
                }
            }

            // 周期采样: pool 字节量 + 斜率 (诚实信号)
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
                            if let Err(e) = do_hot_switch(&mut active, &cfg.ladder[t], &source, &writer).await {
                                warn!(target: "session", "切档 spawn 失败: {e}");
                                sm.rollback_switch();
                            } else {
                                cur_ladder = t; active_bps = ladder_bps[t];
                                sm.confirm_switch();
                            }
                        }
                    }
                    Decision::StepUp(t) => {
                        if t != cur_ladder {
                            info!(target: "session", "tv-{} 升档探测 {}→{} (drain={}B/s)",
                                channel_idx, cur_ladder, t, drain_bps);
                            if let Err(e) = do_hot_switch(&mut active, &cfg.ladder[t], &source, &writer).await {
                                warn!(target: "session", "升档 spawn 失败: {e}");
                                sm.rollback_switch();
                            } else {
                                cur_ladder = t; active_bps = ladder_bps[t];
                                sm.confirm_switch();
                            }
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

/// hot_switch: spawn 新 ffmpeg → 等首帧 → clear 池 → kill 旧 → 发首帧。
async fn do_hot_switch(
    active: &mut Option<Child>,
    run: &Run,
    source: &str,
    writer: &StreamWriter,
) -> Result<()> {
    let (mut new_child, _rate) = spawn_and_rate(run, source).await?;
    let mut first = vec![0u8; 64 * 1024];
    let n = match new_child.stdout.as_mut() {
        Some(stdout) => tokio::time::timeout(Duration::from_secs(15), stdout.read(&mut first)).await,
        None => anyhow::bail!("no stdout"),
    };
    match n {
        Ok(Ok(n)) if n > 0 => {
            let drained = writer.clear().await;
            if drained > 0 { info!(target:"session","hot_switch: 清空 {} 旧 chunk", drained); }
            if let Some(mut old) = active.take() {
                ffmpeg::kill_process_group(&mut old).await;
            }
            writer.send(first[..n].to_vec()).await;
            *active = Some(new_child);
            Ok(())
        }
        Ok(Ok(_)) => { ffmpeg::kill_process_group(&mut new_child).await; anyhow::bail!("EOF immediately"); }
        Ok(Err(e)) => { ffmpeg::kill_process_group(&mut new_child).await; anyhow::bail!("read err: {e}"); }
        Err(_) => { ffmpeg::kill_process_group(&mut new_child).await; anyhow::bail!("first frame timeout"); }
    }
}

async fn spawn_and_rate(run: &Run, source: &str) -> Result<(Child, u64)> {
    let mut cmd = ffmpeg::build_cmd(run, source);
    let child = cmd.spawn()?;
    let rate = if run.mode == "copy" { 800_000 } else { ((run.bitrate + run.audio_bitrate) * 1000 / 8) as u64 };
    Ok((child, rate))
}
