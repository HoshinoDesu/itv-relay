//! 单个播放会话: ffmpeg pipe → HTTP 响应, 含动态档位切换。
//!
//! v6 策略 (无卡顿切档):
//! - 新 ffmpeg 在后台 spawn + 等首帧, 主循环持续读旧 ffmpeg 不中断
//! - 新进程就绪后替换 active child, 清掉旧积压并送入新档预读数据
//! - MPEG-TS 自同步 + -copyts 保证 PTS 连续, 播放器无感切换

use crate::config::{Config, Run};
use crate::ffmpeg;
use crate::state::{CongestionCfg, Decision, StateMachine};
use crate::streambuf::{SendResult, StreamWriter};
use anyhow::Result;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

type PreparedStream = (Child, Vec<Vec<u8>>, Vec<u8>, u64);
type SwitchPayload = Result<PreparedStream>;
type SwitchReceiver = oneshot::Receiver<SwitchPayload>;
type SwitchResult = std::result::Result<SwitchPayload, oneshot::error::RecvError>;
type RecoveryPayload = Result<(usize, Child, Vec<Vec<u8>>, Vec<u8>, u64)>;
type RecoveryReceiver = oneshot::Receiver<RecoveryPayload>;
type RecoveryResult = std::result::Result<RecoveryPayload, oneshot::error::RecvError>;

const SWITCH_PREROLL_IDLE: Duration = Duration::from_millis(250);
const INITIAL_COPY_OUTPUT_TIMEOUT_MAX: Duration = Duration::from_secs(3);
const INITIAL_ENCODE_OUTPUT_TIMEOUT_MIN: Duration = Duration::from_secs(8);
const INITIAL_ENCODE_OUTPUT_TIMEOUT_MAX: Duration = Duration::from_secs(12);
const SEND_BACKPRESSURE_TICK: Duration = Duration::from_millis(100);
const TS_PACKET_SIZE: usize = 188;
const COPY_PREROLL_BYTE_LIMIT: usize = 8 * 1024 * 1024;
const VIDEO_PID_BASE: u16 = 0x0200;
const VIDEO_PID_COUNT: u16 = 0x0600;

static NEXT_VIDEO_PID: AtomicU16 = AtomicU16::new(0);

#[derive(Clone, Copy)]
enum VideoCodec {
    H264,
    H265,
}

struct VideoStream {
    pid: u16,
    codec: VideoCodec,
}

struct KeyframeSpan {
    prelude_offset: usize,
    keyframe_offset: usize,
    parameter_ranges: Vec<(usize, usize)>,
    video_pid: Option<u16>,
}

struct PendingSwitch {
    rx: SwitchReceiver,
    task: JoinHandle<()>,
    target: usize,
    started: Instant,
}

impl PendingSwitch {
    fn abort(self) {
        self.task.abort();
    }
}

struct PendingRecovery {
    rx: RecoveryReceiver,
    task: JoinHandle<()>,
    started: Instant,
}

impl PendingRecovery {
    fn abort(self) {
        self.task.abort();
    }
}

struct StreamApply<'a> {
    active: &'a mut Option<Child>,
    pending_writes: &'a mut VecDeque<Vec<u8>>,
    writer: &'a StreamWriter,
    sm: &'a mut StateMachine,
    ts_tail: &'a mut Vec<u8>,
    cur_ladder: &'a mut usize,
    active_bps: &'a mut u64,
    prev_backlog_bytes: &'a mut u64,
    channel_idx: usize,
}

struct RecoveryApply<'a> {
    common: StreamApply<'a>,
    last_sample: &'a mut Instant,
    startup_direct_attempted: &'a mut bool,
    cfg: &'a Config,
    ladder_bps: &'a [u64],
    max_index: usize,
}

struct RecoveryStart<'a> {
    active: &'a mut Option<Child>,
    pending_switch: &'a mut Option<PendingSwitch>,
    pending_recovery: &'a mut Option<PendingRecovery>,
    ladder: &'a [Run],
    source: &'a str,
    current_ladder: usize,
    switch_timeout_s: f64,
    switch_preroll_bytes: usize,
    channel_idx: usize,
}

enum LoopEvent {
    Read(std::io::Result<usize>),
    Switch {
        result: Box<SwitchResult>,
        target: usize,
        started: Instant,
    },
}

pub struct Session {
    pub channel_idx: usize,
    pub source_url: String,
    pub cfg: Arc<Config>,
}

impl Session {
    pub fn new(channel_idx: usize, source_url: String, cfg: Arc<Config>) -> Self {
        Self {
            channel_idx,
            source_url,
            cfg,
        }
    }

    pub async fn run(self, writer: StreamWriter) {
        let cfg = self.cfg.clone();
        let source = self.source_url.clone();
        let channel_idx = self.channel_idx;
        let max_index = cfg.ladder.len().saturating_sub(1);

        let ladder_bps: Vec<u64> = cfg.ladder.iter().map(Run::output_bps).collect();

        let requested_startup_ladder = cfg.startup_ladder.min(max_index);
        let (mut cur_ladder, mut active, initial_writes, mut ts_tail, mut active_bps) =
            match spawn_initial_ladder(
                &cfg.ladder,
                &source,
                requested_startup_ladder,
                cfg.switch_timeout_s,
                cfg.switch_preroll_bytes,
                channel_idx,
            )
            .await
            {
                Ok((ladder, child, chunks, tail, bps)) => (ladder, Some(child), chunks, tail, bps),
                Err(e) => {
                    warn!(target: "session", "tv-{} 所有起播档准备失败: {e}", channel_idx);
                    writer.close().await;
                    return;
                }
            };
        let mut sm = StateMachine::new(
            CongestionCfg::from(cfg.as_ref()),
            max_index,
            ladder_bps.clone(),
            cur_ladder,
        );

        let sample_iv = Duration::from_secs_f64(cfg.sample_interval_s.max(0.5));
        let started_at = Instant::now();
        let mut last_sample = Instant::now();
        let mut prev_backlog_bytes: u64 = 0;
        let mut startup_direct_attempted = cur_ladder == 0 || cfg.startup_hold_s <= 0.0;

        let mut buf = vec![0u8; 64 * 1024];

        let mut pending_switch: Option<PendingSwitch> = None;
        let mut pending_recovery: Option<PendingRecovery> = None;
        let mut pending_writes: VecDeque<Vec<u8>> = initial_writes.into();

        loop {
            if let Some(mut pending) = pending_switch.take() {
                match pending.rx.try_recv() {
                    Ok(payload) => {
                        apply_switch_result(
                            Ok(payload),
                            pending.target,
                            pending.started,
                            StreamApply {
                                active: &mut active,
                                pending_writes: &mut pending_writes,
                                writer: &writer,
                                sm: &mut sm,
                                ts_tail: &mut ts_tail,
                                cur_ladder: &mut cur_ladder,
                                active_bps: &mut active_bps,
                                prev_backlog_bytes: &mut prev_backlog_bytes,
                                channel_idx,
                            },
                        )
                        .await;
                    }
                    Err(TryRecvError::Empty) => {
                        pending_switch = Some(pending);
                    }
                    Err(TryRecvError::Closed) => {
                        warn!(target: "session", "tv-{} 切档任务异常退出", channel_idx);
                        sm.rollback_failed_switch(pending.target, Instant::now());
                    }
                }
            }
            if let Some(mut pending) = pending_recovery.take() {
                match pending.rx.try_recv() {
                    Ok(payload) => {
                        if !apply_recovery_result(
                            Ok(payload),
                            pending.started,
                            RecoveryApply {
                                common: StreamApply {
                                    active: &mut active,
                                    pending_writes: &mut pending_writes,
                                    writer: &writer,
                                    sm: &mut sm,
                                    ts_tail: &mut ts_tail,
                                    cur_ladder: &mut cur_ladder,
                                    active_bps: &mut active_bps,
                                    prev_backlog_bytes: &mut prev_backlog_bytes,
                                    channel_idx,
                                },
                                last_sample: &mut last_sample,
                                startup_direct_attempted: &mut startup_direct_attempted,
                                cfg: cfg.as_ref(),
                                ladder_bps: &ladder_bps,
                                max_index,
                            },
                        )
                        .await
                        {
                            break;
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        pending_recovery = Some(pending);
                    }
                    Err(TryRecvError::Closed) => {
                        warn!(target: "session", "tv-{} 播放中断恢复任务异常退出", channel_idx);
                        break;
                    }
                }
            }

            if let Some(chunk) = pending_writes.pop_front() {
                match writer.send_timeout(chunk, SEND_BACKPRESSURE_TICK).await {
                    SendResult::Sent => {}
                    SendResult::Closed => {
                        debug!(target: "session", "客户端断开, 终止会话");
                        break;
                    }
                    SendResult::Full(chunk) => {
                        pending_writes.push_front(chunk);
                    }
                }
            } else {
                if active.is_none() {
                    let Some(pending) = pending_recovery.take() else {
                        break;
                    };
                    let started = pending.started;
                    let result = pending.rx.await;
                    if !apply_recovery_result(
                        result,
                        started,
                        RecoveryApply {
                            common: StreamApply {
                                active: &mut active,
                                pending_writes: &mut pending_writes,
                                writer: &writer,
                                sm: &mut sm,
                                ts_tail: &mut ts_tail,
                                cur_ladder: &mut cur_ladder,
                                active_bps: &mut active_bps,
                                prev_backlog_bytes: &mut prev_backlog_bytes,
                                channel_idx,
                            },
                            last_sample: &mut last_sample,
                            startup_direct_attempted: &mut startup_direct_attempted,
                            cfg: cfg.as_ref(),
                            ladder_bps: &ladder_bps,
                            max_index,
                        },
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }
                let event = {
                    let child = match active.as_mut() {
                        Some(c) => c,
                        None => break,
                    };
                    let stdout = match child.stdout.as_mut() {
                        Some(stdout) => stdout,
                        None => break,
                    };

                    if let Some(mut pending) = pending_switch.take() {
                        tokio::select! {
                            biased;
                            result = &mut pending.rx => LoopEvent::Switch {
                                result: Box::new(result),
                                target: pending.target,
                                started: pending.started,
                            },
                            read_result = stdout.read(&mut buf) => {
                                pending_switch = Some(pending);
                                LoopEvent::Read(read_result)
                            }
                        }
                    } else {
                        LoopEvent::Read(stdout.read(&mut buf).await)
                    }
                };

                match event {
                    LoopEvent::Switch {
                        result,
                        target,
                        started,
                    } => {
                        apply_switch_result(
                            *result,
                            target,
                            started,
                            StreamApply {
                                active: &mut active,
                                pending_writes: &mut pending_writes,
                                writer: &writer,
                                sm: &mut sm,
                                ts_tail: &mut ts_tail,
                                cur_ladder: &mut cur_ladder,
                                active_bps: &mut active_bps,
                                prev_backlog_bytes: &mut prev_backlog_bytes,
                                channel_idx,
                            },
                        )
                        .await
                    }
                    LoopEvent::Read(n) => match n {
                        Ok(0) => {
                            warn!(target: "session", "tv-{} ffmpeg stdout EOF, 后台恢复中", channel_idx);
                            start_active_recovery_if_needed(RecoveryStart {
                                active: &mut active,
                                pending_switch: &mut pending_switch,
                                pending_recovery: &mut pending_recovery,
                                ladder: &cfg.ladder,
                                source: &source,
                                current_ladder: cur_ladder,
                                switch_timeout_s: cfg.switch_timeout_s,
                                switch_preroll_bytes: cfg.switch_preroll_bytes,
                                channel_idx,
                            });
                        }
                        Ok(n) => {
                            enqueue_complete_ts_packets(
                                &buf[..n],
                                &mut ts_tail,
                                &mut pending_writes,
                            );
                        }
                        Err(e) => {
                            warn!(target: "session", "读 ffmpeg 失败: {e}, 后台恢复中");
                            start_active_recovery_if_needed(RecoveryStart {
                                active: &mut active,
                                pending_switch: &mut pending_switch,
                                pending_recovery: &mut pending_recovery,
                                ladder: &cfg.ladder,
                                source: &source,
                                current_ladder: cur_ladder,
                                switch_timeout_s: cfg.switch_timeout_s,
                                switch_preroll_bytes: cfg.switch_preroll_bytes,
                                channel_idx,
                            });
                        }
                    },
                }
            }

            let now = Instant::now();
            if now.duration_since(last_sample) >= sample_iv {
                let dt = now.duration_since(last_sample);
                last_sample = now;

                let queued_chunks = writer.backlog().await;
                let pending_chunks = pending_writes.len();
                let backlog_chunks = queued_chunks
                    .saturating_add(pending_chunks)
                    .min(crate::streambuf::BUF_CAP);
                let backlog_bytes = writer.backlog_bytes() + pending_write_bytes(&pending_writes);
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
                    "tv-{} 档{} chunks={}/{} queued={} pending={} ({:.0}%) bytes={}B delta={}B drain={}B/s prod={}B/s",
                    channel_idx, cur_ladder, backlog_chunks, crate::streambuf::BUF_CAP,
                    queued_chunks, pending_chunks,
                    backlog_ratio * 100.0, backlog_bytes, backlog_delta, drain_bps, active_bps,
                );

                // 切档进行中继续采样和刷新本地基线, 但不触发新的状态机决策。
                if pending_switch.is_some() || pending_recovery.is_some() {
                    continue;
                }

                let decision = if !startup_direct_attempted
                    && cur_ladder > 0
                    && now.duration_since(started_at).as_secs_f64() >= cfg.startup_hold_s
                    && drained > 0.0
                    && backlog_ratio < 0.20
                    && backlog_delta <= 0
                {
                    startup_direct_attempted = true;
                    match sm.request_startup_probe(0, backlog_bytes, now) {
                        Some(decision) => decision,
                        None => sm.update(&sample, dt, now),
                    }
                } else {
                    sm.update(&sample, dt, now)
                };
                match decision {
                    Decision::StepDown(t) | Decision::RevertDown(t) => {
                        if t != cur_ladder {
                            let tag = matches!(decision, Decision::RevertDown(_));
                            info!(target: "session", "tv-{} {} {}→{} (drain={}B/s pool={:.0}%)",
                                channel_idx, if tag {"回退"} else {"降档"}, cur_ladder, t, drain_bps, backlog_ratio * 100.0);
                            pending_switch = Some(start_bg_switch(
                                &cfg.ladder[t],
                                &source,
                                cfg.switch_timeout_s,
                                cfg.switch_preroll_bytes,
                                t,
                            ));
                        }
                    }
                    Decision::StepUp(t) => {
                        if t != cur_ladder {
                            info!(target: "session", "tv-{} 升档探测 {}→{} (drain={}B/s)",
                                channel_idx, cur_ladder, t, drain_bps);
                            pending_switch = Some(start_bg_switch(
                                &cfg.ladder[t],
                                &source,
                                cfg.switch_timeout_s,
                                cfg.switch_preroll_bytes,
                                t,
                            ));
                        }
                    }
                    Decision::Hold => {}
                }
            }
        }

        if let Some(pending) = pending_switch.take() {
            pending.abort();
        }
        if let Some(pending) = pending_recovery.take() {
            pending.abort();
        }
        if let Some(mut c) = active.take() {
            ffmpeg::kill_process_group(&mut c).await;
        }
        writer.close().await;
    }
}

async fn apply_switch_result(
    result: SwitchResult,
    target: usize,
    started: Instant,
    ctx: StreamApply<'_>,
) {
    match result {
        Ok(Ok((new_child, preroll_chunks, preroll_tail, rate))) => {
            let elapsed = started.elapsed().as_millis();
            let preroll_chunk_count = preroll_chunks.len();
            let preroll_bytes: usize = preroll_chunks.iter().map(Vec::len).sum();
            if let Some(mut old) = ctx.active.replace(new_child) {
                tokio::spawn(async move {
                    ffmpeg::kill_process_group(&mut old).await;
                });
            }
            ctx.pending_writes.clear();
            ctx.writer.clear().await;
            ctx.pending_writes.extend(preroll_chunks);
            *ctx.ts_tail = preroll_tail;
            *ctx.cur_ladder = target;
            *ctx.active_bps = rate;
            let backlog_bytes =
                ctx.writer.backlog_bytes() + pending_write_bytes(ctx.pending_writes);
            *ctx.prev_backlog_bytes = backlog_bytes;
            ctx.sm.confirm_switch(backlog_bytes);
            info!(
                target: "session",
                "tv-{} 无缝切档完成 → 档{} ({}ms preroll={} chunks/{}B)",
                ctx.channel_idx,
                target,
                elapsed,
                preroll_chunk_count,
                preroll_bytes
            );
        }
        Ok(Err(e)) => {
            warn!(target: "session", "tv-{} 后台切档失败: {e}", ctx.channel_idx);
            ctx.sm.rollback_failed_switch(target, Instant::now());
        }
        Err(_) => {
            warn!(target: "session", "tv-{} 切档任务异常退出", ctx.channel_idx);
            ctx.sm.rollback_failed_switch(target, Instant::now());
        }
    }
}

fn start_active_recovery_if_needed(ctx: RecoveryStart<'_>) {
    if ctx.pending_recovery.is_some() {
        return;
    }
    if let Some(pending) = ctx.pending_switch.take() {
        pending.abort();
    }
    if let Some(mut old) = ctx.active.take() {
        tokio::spawn(async move {
            ffmpeg::kill_process_group(&mut old).await;
        });
    }
    *ctx.pending_recovery = Some(start_bg_recovery(
        ctx.ladder,
        ctx.source,
        ctx.current_ladder,
        ctx.switch_timeout_s,
        ctx.switch_preroll_bytes,
        ctx.channel_idx,
    ));
}

async fn apply_recovery_result(
    result: RecoveryResult,
    started: Instant,
    ctx: RecoveryApply<'_>,
) -> bool {
    match result {
        Ok(Ok((ladder, child, chunks, tail, bps))) => {
            let chunk_count = chunks.len();
            let bytes = chunks.iter().map(Vec::len).sum::<usize>() + tail.len();
            if let Some(mut old) = ctx.common.active.replace(child) {
                tokio::spawn(async move {
                    ffmpeg::kill_process_group(&mut old).await;
                });
            }
            ctx.common.pending_writes.extend(chunks);
            *ctx.common.ts_tail = tail;
            *ctx.common.cur_ladder = ladder;
            *ctx.common.active_bps = bps;
            *ctx.common.sm = StateMachine::new(
                CongestionCfg::from(ctx.cfg),
                ctx.max_index,
                ctx.ladder_bps.to_vec(),
                ladder,
            );
            *ctx.startup_direct_attempted = true;
            let backlog_bytes =
                ctx.common.writer.backlog_bytes() + pending_write_bytes(ctx.common.pending_writes);
            *ctx.common.prev_backlog_bytes = backlog_bytes;
            *ctx.last_sample = Instant::now();
            info!(
                target: "session",
                "tv-{} 播放中断恢复 → 档{} ({}ms initial={} chunks/{}B)",
                ctx.common.channel_idx,
                ladder,
                started.elapsed().as_millis(),
                chunk_count,
                bytes
            );
            true
        }
        Ok(Err(e)) => {
            warn!(target: "session", "tv-{} 播放中断恢复失败: {e}", ctx.common.channel_idx);
            false
        }
        Err(_) => {
            warn!(target: "session", "tv-{} 播放中断恢复任务异常退出", ctx.common.channel_idx);
            false
        }
    }
}

fn start_bg_switch(
    run: &Run,
    source: &str,
    timeout_s: f64,
    preroll_bytes: usize,
    ladder: usize,
) -> PendingSwitch {
    let (tx, rx) = oneshot::channel();
    let run = run.clone();
    let src = source.to_string();
    let timeout = Duration::from_secs_f64(timeout_s.clamp(2.0, 30.0));
    let preroll_bytes = preroll_bytes.clamp(64 * 1024, 2 * 1024 * 1024);
    let task = tokio::spawn(async move {
        let _ = tx.send(prepare_switch(&run, &src, timeout, preroll_bytes).await);
    });
    PendingSwitch {
        rx,
        task,
        target: ladder,
        started: Instant::now(),
    }
}

fn start_bg_recovery(
    ladder: &[Run],
    source: &str,
    current_ladder: usize,
    switch_timeout_s: f64,
    switch_preroll_bytes: usize,
    channel_idx: usize,
) -> PendingRecovery {
    let (tx, rx) = oneshot::channel();
    let ladder = ladder.to_vec();
    let source = source.to_string();
    let task = tokio::spawn(async move {
        let _ = tx.send(
            recover_active_stream(
                &ladder,
                &source,
                current_ladder,
                switch_timeout_s,
                switch_preroll_bytes,
                channel_idx,
            )
            .await,
        );
    });
    PendingRecovery {
        rx,
        task,
        started: Instant::now(),
    }
}

fn pending_write_bytes(pending_writes: &VecDeque<Vec<u8>>) -> u64 {
    pending_writes.iter().map(|chunk| chunk.len() as u64).sum()
}

fn enqueue_complete_ts_packets(
    data: &[u8],
    tail: &mut Vec<u8>,
    pending_writes: &mut VecDeque<Vec<u8>>,
) {
    if tail.is_empty() && data.len() >= TS_PACKET_SIZE && data.len().is_multiple_of(TS_PACKET_SIZE)
    {
        pending_writes.push_back(data.to_vec());
        return;
    }

    tail.extend_from_slice(data);
    let complete_len = tail.len() / TS_PACKET_SIZE * TS_PACKET_SIZE;
    if complete_len > 0 {
        let rest = tail.split_off(complete_len);
        let complete = std::mem::replace(tail, rest);
        pending_writes.push_back(complete);
    }
}

fn required_preroll_keyframes(run: &Run) -> usize {
    if run.mode == "copy" {
        1
    } else {
        2
    }
}

fn pending_has_video_keyframes(pending: &VecDeque<Vec<u8>>, count: usize) -> bool {
    find_nth_video_keyframe_span(&flatten_pending(pending), count).is_some()
}

#[cfg(test)]
fn pending_has_video_keyframe(pending: &VecDeque<Vec<u8>>) -> bool {
    pending_has_video_keyframes(pending, 1)
}

#[cfg(test)]
fn trim_pending_to_video_keyframe(pending: VecDeque<Vec<u8>>) -> Option<VecDeque<Vec<u8>>> {
    trim_pending_to_nth_video_keyframe(pending, 1)
}

fn trim_pending_to_nth_video_keyframe(
    pending: VecDeque<Vec<u8>>,
    keyframe_count: usize,
) -> Option<VecDeque<Vec<u8>>> {
    let flat = flatten_pending(&pending);
    let span = find_nth_video_keyframe_span(&flat, keyframe_count)?;
    let keyframe_packet = span.keyframe_offset / TS_PACKET_SIZE * TS_PACKET_SIZE;
    let prelude_packet = span.prelude_offset / TS_PACKET_SIZE * TS_PACKET_SIZE;
    let mut trimmed = Vec::new();

    if let Some(pat_start) = latest_pat_before(&flat, keyframe_packet) {
        trimmed.extend_from_slice(flat.get(pat_start..pat_start + TS_PACKET_SIZE)?);
        if let Some(pmt_start) = latest_pmt_before(&flat, keyframe_packet, pat_start) {
            trimmed.extend_from_slice(flat.get(pmt_start..pmt_start + TS_PACKET_SIZE)?);
        }
    }
    append_parameter_ranges(&flat, &mut trimmed, &span.parameter_ranges, prelude_packet)?;
    trimmed.extend_from_slice(flat.get(prelude_packet..)?);

    Some(VecDeque::from(vec![trimmed]))
}

fn trim_pending_to_nth_video_keyframe_pes(
    pending: VecDeque<Vec<u8>>,
    keyframe_count: usize,
) -> Option<VecDeque<Vec<u8>>> {
    let flat = flatten_pending(&pending);
    let span = find_nth_video_keyframe_span(&flat, keyframe_count)?;
    let keyframe_packet = span.keyframe_offset / TS_PACKET_SIZE * TS_PACKET_SIZE;
    let video_pid = span.video_pid?;
    let pes_start = latest_pes_start_before(&flat, keyframe_packet, video_pid)?;
    let mut trimmed = Vec::new();

    if let Some(pat_start) = latest_pat_before(&flat, keyframe_packet) {
        trimmed.extend_from_slice(flat.get(pat_start..pat_start + TS_PACKET_SIZE)?);
        if let Some(pmt_start) = latest_pmt_before(&flat, keyframe_packet, pat_start) {
            trimmed.extend_from_slice(flat.get(pmt_start..pmt_start + TS_PACKET_SIZE)?);
        }
    }
    trimmed.extend_from_slice(flat.get(pes_start..)?);

    Some(VecDeque::from(vec![trimmed]))
}

fn append_parameter_ranges(
    flat: &[u8],
    trimmed: &mut Vec<u8>,
    ranges: &[(usize, usize)],
    before_packet: usize,
) -> Option<()> {
    let mut added = HashSet::new();
    for &(start, end) in ranges {
        if start >= before_packet {
            continue;
        }
        let mut packet_start = start;
        while packet_start < end.min(before_packet) {
            if added.insert(packet_start) {
                trimmed.extend_from_slice(flat.get(packet_start..packet_start + TS_PACKET_SIZE)?);
            }
            packet_start += TS_PACKET_SIZE;
        }
    }
    Some(())
}

fn latest_pat_before(data: &[u8], before_packet: usize) -> Option<usize> {
    (0..=before_packet / TS_PACKET_SIZE)
        .rev()
        .map(|idx| idx * TS_PACKET_SIZE)
        .find(|&packet_start| {
            let Some(packet) = data.get(packet_start..packet_start + TS_PACKET_SIZE) else {
                return false;
            };
            ts_pid(packet) == Some(0)
        })
}

fn latest_pmt_before(data: &[u8], before_packet: usize, pat_start: usize) -> Option<usize> {
    let pat = data.get(pat_start..pat_start + TS_PACKET_SIZE)?;
    let pmt_pid = parse_pat_for_pmt_pid(pat)?;
    (0..=before_packet / TS_PACKET_SIZE)
        .rev()
        .map(|idx| idx * TS_PACKET_SIZE)
        .find(|&packet_start| {
            let Some(packet) = data.get(packet_start..packet_start + TS_PACKET_SIZE) else {
                return false;
            };
            section_from_packet(packet, pmt_pid, 0x02).is_some()
        })
}

fn latest_pes_start_before(data: &[u8], before_packet: usize, pid: u16) -> Option<usize> {
    (0..=before_packet / TS_PACKET_SIZE)
        .rev()
        .map(|idx| idx * TS_PACKET_SIZE)
        .find(|&packet_start| {
            let Some(packet) = data.get(packet_start..packet_start + TS_PACKET_SIZE) else {
                return false;
            };
            ts_pid(packet) == Some(pid) && packet[1] & 0x40 != 0
        })
}

fn flatten_pending(pending: &VecDeque<Vec<u8>>) -> Vec<u8> {
    let total = pending.iter().map(Vec::len).sum();
    let mut flat = Vec::with_capacity(total);
    for chunk in pending {
        flat.extend_from_slice(chunk);
    }
    flat
}

fn drop_initial_non_video_elementary_continuations(
    pending: VecDeque<Vec<u8>>,
) -> VecDeque<Vec<u8>> {
    let flat = flatten_pending(&pending);
    let Some(video) = video_stream(&flat) else {
        return pending;
    };
    let mut pids = elementary_pids(&flat);
    pids.remove(&video.pid);
    if pids.is_empty() {
        return pending;
    }

    let mut started = HashSet::new();
    let mut cleaned = Vec::with_capacity(flat.len());
    for packet in flat.chunks_exact(TS_PACKET_SIZE) {
        let Some(pid) = ts_pid(packet) else {
            continue;
        };
        if !pids.contains(&pid) {
            cleaned.extend_from_slice(packet);
            continue;
        }
        if started.contains(&pid) || packet[1] & 0x40 != 0 {
            if packet[1] & 0x40 != 0 {
                started.insert(pid);
            }
            cleaned.extend_from_slice(packet);
        }
    }

    VecDeque::from(vec![cleaned])
}

fn mark_initial_discontinuity(pending: &mut VecDeque<Vec<u8>>) {
    let mut seen_pids = HashSet::new();
    for chunk in pending {
        for packet in chunk.chunks_exact_mut(TS_PACKET_SIZE) {
            let Some(pid) = ts_pid(packet) else {
                continue;
            };
            if seen_pids.insert(pid) {
                set_discontinuity_indicator(packet);
            }
        }
    }
}

fn prepend_discontinuity_packets(pending: &mut VecDeque<Vec<u8>>) {
    let mut seen_pids = HashSet::new();
    let mut first_packets = Vec::new();
    for chunk in pending.iter() {
        for packet in chunk.chunks_exact(TS_PACKET_SIZE) {
            let Some(pid) = ts_pid(packet) else {
                continue;
            };
            if seen_pids.insert(pid) {
                first_packets.push(discontinuity_packet(pid, packet[3] & 0x0f));
            }
        }
    }
    if first_packets.is_empty() {
        return;
    }

    let mut prefix = Vec::with_capacity(first_packets.len() * TS_PACKET_SIZE);
    for packet in first_packets {
        prefix.extend_from_slice(&packet);
    }
    pending.push_front(prefix);
}

fn discontinuity_packet(pid: u16, continuity_counter: u8) -> [u8; TS_PACKET_SIZE] {
    let mut packet = [0xff; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet[1] = ((pid >> 8) as u8) & 0x1f;
    packet[2] = pid as u8;
    packet[3] = 0x20 | (continuity_counter & 0x0f);
    packet[4] = (TS_PACKET_SIZE - 5) as u8;
    packet[5] = 0x80;
    packet
}

fn set_discontinuity_indicator(packet: &mut [u8]) -> bool {
    if packet.len() < TS_PACKET_SIZE || packet.first() != Some(&0x47) {
        return false;
    }
    let adaptation_control = (packet[3] >> 4) & 0x03;
    if !matches!(adaptation_control, 2 | 3) {
        return false;
    }
    let adaptation_len = packet[4] as usize;
    if adaptation_len < 1 {
        return false;
    }
    packet[5] |= 0x80;
    true
}

fn ts_pid(packet: &[u8]) -> Option<u16> {
    if packet.len() < TS_PACKET_SIZE || packet.first() != Some(&0x47) {
        return None;
    }
    Some((((packet[1] & 0x1f) as u16) << 8) | packet[2] as u16)
}

fn find_nth_video_keyframe_span(data: &[u8], keyframe_count: usize) -> Option<KeyframeSpan> {
    if keyframe_count == 0 {
        return None;
    }
    let video = video_stream(data);
    let video_pid = video.as_ref().map(|v| v.pid);
    let (mut payload, mut offsets) = ts_payload_bytes_with_offsets(data, video_pid);
    if payload.is_empty() && video.is_some() {
        (payload, offsets) = ts_payload_bytes_with_offsets(data, None);
    }

    let codec = video.map(|v| v.codec).unwrap_or(VideoCodec::H264);
    let nal_starts = nal_start_codes(&payload);
    let mut prelude_start: Option<usize> = None;
    let mut latest_param_ranges: Vec<(usize, usize)> = Vec::new();
    let mut seen_keyframes = 0;
    for (idx, &(i, start_code_len)) in nal_starts.iter().enumerate() {
        let nal = &payload[i + start_code_len..];
        if is_keyframe_nal(codec, nal) {
            seen_keyframes += 1;
            if seen_keyframes < keyframe_count {
                prelude_start = None;
                continue;
            }
            let prelude = prelude_start.unwrap_or(i);
            return Some(KeyframeSpan {
                prelude_offset: offsets.get(prelude).copied()?,
                keyframe_offset: offsets.get(i).copied()?,
                parameter_ranges: latest_param_ranges,
                video_pid,
            });
        }
        if is_parameter_set_nal(codec, nal) {
            if starts_parameter_sequence(codec, nal) {
                latest_param_ranges.clear();
            }
            let next = nal_starts
                .get(idx + 1)
                .map(|&(next, _)| next)
                .unwrap_or(payload.len());
            latest_param_ranges.push(packet_range_for_payload_span(&offsets, i, next)?);
            prelude_start.get_or_insert(i);
        } else if is_keyframe_prelude_nal(codec, nal) {
            prelude_start.get_or_insert(i);
        } else {
            prelude_start = None;
        }
    }
    None
}

fn nal_start_codes(payload: &[u8]) -> Vec<(usize, usize)> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 5 < payload.len() {
        let start_code_len = if payload[i] == 0 && payload[i + 1] == 0 && payload[i + 2] == 1 {
            3
        } else if i + 6 < payload.len()
            && payload[i] == 0
            && payload[i + 1] == 0
            && payload[i + 2] == 0
            && payload[i + 3] == 1
        {
            4
        } else {
            i += 1;
            continue;
        };
        starts.push((i, start_code_len));
        i += start_code_len;
    }
    starts
}

fn packet_range_for_payload_span(
    offsets: &[usize],
    start_payload: usize,
    end_payload: usize,
) -> Option<(usize, usize)> {
    let start = *offsets.get(start_payload)?;
    let last_payload = end_payload
        .saturating_sub(1)
        .min(offsets.len().saturating_sub(1));
    let end = *offsets.get(last_payload)?;
    Some((
        start / TS_PACKET_SIZE * TS_PACKET_SIZE,
        (end / TS_PACKET_SIZE + 1) * TS_PACKET_SIZE,
    ))
}

fn is_keyframe_nal(codec: VideoCodec, nal: &[u8]) -> bool {
    match codec {
        VideoCodec::H264 => nal.first().is_some_and(|byte| byte & 0x1f == 5),
        VideoCodec::H265 => nal
            .first()
            .map(|byte| (byte >> 1) & 0x3f)
            .is_some_and(|nal_type| matches!(nal_type, 19..=21)),
    }
}

fn is_keyframe_prelude_nal(codec: VideoCodec, nal: &[u8]) -> bool {
    match codec {
        VideoCodec::H264 => nal
            .first()
            .map(|byte| byte & 0x1f)
            .is_some_and(|nal_type| matches!(nal_type, 6..=9)),
        VideoCodec::H265 => nal
            .first()
            .map(|byte| (byte >> 1) & 0x3f)
            .is_some_and(|nal_type| matches!(nal_type, 32..=35 | 39 | 40)),
    }
}

fn is_parameter_set_nal(codec: VideoCodec, nal: &[u8]) -> bool {
    match codec {
        VideoCodec::H264 => nal
            .first()
            .map(|byte| byte & 0x1f)
            .is_some_and(|nal_type| matches!(nal_type, 7 | 8)),
        VideoCodec::H265 => nal
            .first()
            .map(|byte| (byte >> 1) & 0x3f)
            .is_some_and(|nal_type| matches!(nal_type, 32..=34)),
    }
}

fn starts_parameter_sequence(codec: VideoCodec, nal: &[u8]) -> bool {
    match codec {
        VideoCodec::H264 => nal.first().map(|byte| byte & 0x1f) == Some(7),
        VideoCodec::H265 => nal
            .first()
            .map(|byte| (byte >> 1) & 0x3f)
            .is_some_and(|nal_type| matches!(nal_type, 32 | 33)),
    }
}

fn ts_payload_bytes_with_offsets(data: &[u8], pid_filter: Option<u16>) -> (Vec<u8>, Vec<usize>) {
    let packet_count = data.len() / TS_PACKET_SIZE;
    let mut payload = Vec::with_capacity(packet_count * (TS_PACKET_SIZE - 4));
    let mut offsets = Vec::with_capacity(payload.capacity());

    for packet_idx in 0..packet_count {
        let packet_start = packet_idx * TS_PACKET_SIZE;
        let packet = &data[packet_start..packet_start + TS_PACKET_SIZE];
        if packet.first() != Some(&0x47) {
            continue;
        }
        if pid_filter.is_some_and(|pid| ts_pid(packet) != Some(pid)) {
            continue;
        }

        let Some(payload_start) = ts_payload_start(packet) else {
            continue;
        };

        for (offset, byte) in packet.iter().enumerate().skip(payload_start) {
            payload.push(*byte);
            offsets.push(packet_start + offset);
        }
    }

    (payload, offsets)
}

fn ts_payload_start(packet: &[u8]) -> Option<usize> {
    if packet.len() < TS_PACKET_SIZE || packet.first() != Some(&0x47) {
        return None;
    }

    let adaptation_control = (packet[3] >> 4) & 0x03;
    match adaptation_control {
        0 | 2 => None,
        1 => Some(4),
        3 => {
            let adaptation_len = packet[4] as usize;
            let start = 5 + adaptation_len;
            (start < TS_PACKET_SIZE).then_some(start)
        }
        _ => None,
    }
}

fn video_stream(data: &[u8]) -> Option<VideoStream> {
    let pmt_pid = data
        .chunks_exact(TS_PACKET_SIZE)
        .find_map(parse_pat_for_pmt_pid)?;

    data.chunks_exact(TS_PACKET_SIZE)
        .find_map(|packet| parse_pmt_for_video_stream(packet, pmt_pid))
}

fn section_from_packet(packet: &[u8], expected_pid: u16, table_id: u8) -> Option<&[u8]> {
    if ts_pid(packet) != Some(expected_pid) {
        return None;
    }
    let mut start = ts_payload_start(packet)?;
    if packet[1] & 0x40 != 0 {
        let pointer = *packet.get(start)? as usize;
        start = start.checked_add(1 + pointer)?;
    }
    let section = packet.get(start..)?;
    if section.first() != Some(&table_id) || section.len() < 3 {
        return None;
    }
    let section_len = (((section[1] & 0x0f) as usize) << 8) | section[2] as usize;
    section.get(..3 + section_len)
}

fn parse_pat_for_pmt_pid(packet: &[u8]) -> Option<u16> {
    let section = section_from_packet(packet, 0, 0x00)?;
    let section_len = (((section[1] & 0x0f) as usize) << 8) | section[2] as usize;
    let entries_end = 3 + section_len.checked_sub(4)?;
    let mut pos = 8;
    while pos + 4 <= entries_end {
        let program_number = u16::from_be_bytes([section[pos], section[pos + 1]]);
        let pid = (((section[pos + 2] & 0x1f) as u16) << 8) | section[pos + 3] as u16;
        if program_number != 0 {
            return Some(pid);
        }
        pos += 4;
    }
    None
}

fn parse_pmt_for_video_stream(packet: &[u8], pmt_pid: u16) -> Option<VideoStream> {
    let section = section_from_packet(packet, pmt_pid, 0x02)?;
    if section.len() < 12 {
        return None;
    }

    let section_len = (((section[1] & 0x0f) as usize) << 8) | section[2] as usize;
    let entries_end = 3 + section_len.checked_sub(4)?;
    let program_info_len = (((section[10] & 0x0f) as usize) << 8) | section[11] as usize;
    let mut pos = 12 + program_info_len;
    while pos + 5 <= entries_end {
        let stream_type = section[pos];
        let elementary_pid = (((section[pos + 1] & 0x1f) as u16) << 8) | section[pos + 2] as u16;
        let es_info_len = (((section[pos + 3] & 0x0f) as usize) << 8) | section[pos + 4] as usize;
        let codec = match stream_type {
            0x1b => Some(VideoCodec::H264),
            0x24 => Some(VideoCodec::H265),
            _ => None,
        };
        if let Some(codec) = codec {
            return Some(VideoStream {
                pid: elementary_pid,
                codec,
            });
        }
        pos += 5 + es_info_len;
    }
    None
}

fn elementary_pids(data: &[u8]) -> HashSet<u16> {
    let Some(pmt_pid) = data
        .chunks_exact(TS_PACKET_SIZE)
        .find_map(parse_pat_for_pmt_pid)
    else {
        return HashSet::new();
    };

    data.chunks_exact(TS_PACKET_SIZE)
        .find_map(|packet| parse_pmt_for_elementary_pids(packet, pmt_pid))
        .unwrap_or_default()
}

fn parse_pmt_for_elementary_pids(packet: &[u8], pmt_pid: u16) -> Option<HashSet<u16>> {
    let section = section_from_packet(packet, pmt_pid, 0x02)?;
    if section.len() < 12 {
        return None;
    }

    let section_len = (((section[1] & 0x0f) as usize) << 8) | section[2] as usize;
    let entries_end = 3 + section_len.checked_sub(4)?;
    let program_info_len = (((section[10] & 0x0f) as usize) << 8) | section[11] as usize;
    let mut pos = 12 + program_info_len;
    let mut pids = HashSet::new();
    while pos + 5 <= entries_end {
        let elementary_pid = (((section[pos + 1] & 0x1f) as u16) << 8) | section[pos + 2] as u16;
        let es_info_len = (((section[pos + 3] & 0x0f) as usize) << 8) | section[pos + 4] as usize;
        pids.insert(elementary_pid);
        pos += 5 + es_info_len;
    }
    Some(pids)
}

/// 后台准备新 ffmpeg: spawn + 等首帧, 不阻塞主读循环。
async fn prepare_switch(
    run: &Run,
    source: &str,
    timeout: Duration,
    preroll_bytes: usize,
) -> Result<(Child, Vec<Vec<u8>>, Vec<u8>, u64)> {
    let (mut child, rate) = spawn_and_rate(run, source).await?;
    let started = Instant::now();
    let mut first = vec![0u8; 64 * 1024];
    let n = match child.stdout.as_mut() {
        Some(stdout) => tokio::time::timeout(timeout, stdout.read(&mut first)).await,
        None => anyhow::bail!("no stdout"),
    };
    match n {
        Ok(Ok(n)) if n > 0 => {
            let mut preroll = VecDeque::new();
            let mut preroll_tail = Vec::new();
            let mut total = n;
            enqueue_complete_ts_packets(&first[..n], &mut preroll_tail, &mut preroll);
            let required_keyframes = required_preroll_keyframes(run);
            let preroll_goal = preroll_bytes;
            let preroll_limit = if required_keyframes > 0 {
                copy_preroll_limit(preroll_goal)
            } else {
                preroll_goal
            };
            let mut buf = vec![0u8; 64 * 1024];
            while total < preroll_goal
                || (required_keyframes > 0
                    && !pending_has_video_keyframes(&preroll, required_keyframes))
            {
                if total >= preroll_limit {
                    ffmpeg::kill_process_group(&mut child).await;
                    anyhow::bail!("preroll exceeded {} bytes before IDR", preroll_limit);
                }
                if started.elapsed() >= timeout {
                    break;
                }
                let remaining = timeout.saturating_sub(started.elapsed());
                let read_timeout = SWITCH_PREROLL_IDLE.min(remaining);
                let read = match child.stdout.as_mut() {
                    Some(stdout) => tokio::time::timeout(read_timeout, stdout.read(&mut buf)).await,
                    None => anyhow::bail!("no stdout"),
                };
                match read {
                    Ok(Ok(n)) if n > 0 => {
                        total += n;
                        enqueue_complete_ts_packets(&buf[..n], &mut preroll_tail, &mut preroll);
                    }
                    Ok(Ok(_)) => {
                        ffmpeg::kill_process_group(&mut child).await;
                        anyhow::bail!("EOF during preroll");
                    }
                    Ok(Err(e)) => {
                        ffmpeg::kill_process_group(&mut child).await;
                        anyhow::bail!("read err during preroll: {e}");
                    }
                    Err(_) => {
                        if required_keyframes > 0
                            && !pending_has_video_keyframes(&preroll, required_keyframes)
                        {
                            continue;
                        }
                        break;
                    }
                }
            }
            if required_keyframes > 0 {
                let trimmed = if run.mode == "copy" {
                    trim_pending_to_nth_video_keyframe(preroll, required_keyframes)
                } else {
                    trim_pending_to_nth_video_keyframe_pes(preroll, required_keyframes)
                };
                match trimmed {
                    Some(trimmed) => {
                        preroll = drop_initial_non_video_elementary_continuations(trimmed)
                    }
                    None => {
                        ffmpeg::kill_process_group(&mut child).await;
                        anyhow::bail!("preroll reached timeout before IDR");
                    }
                }
            }
            mark_initial_discontinuity(&mut preroll);
            prepend_discontinuity_packets(&mut preroll);
            Ok((child, preroll.into(), preroll_tail, rate))
        }
        Ok(Ok(_)) => {
            ffmpeg::kill_process_group(&mut child).await;
            anyhow::bail!("EOF immediately");
        }
        Ok(Err(e)) => {
            ffmpeg::kill_process_group(&mut child).await;
            anyhow::bail!("read err: {e}");
        }
        Err(_) => {
            ffmpeg::kill_process_group(&mut child).await;
            anyhow::bail!("first frame timeout");
        }
    }
}

async fn spawn_initial_ladder(
    ladder: &[Run],
    source: &str,
    startup_ladder: usize,
    switch_timeout_s: f64,
    switch_preroll_bytes: usize,
    channel_idx: usize,
) -> Result<(usize, Child, Vec<Vec<u8>>, Vec<u8>, u64)> {
    let mut last_error = None;
    for idx in startup_order(ladder.len(), startup_ladder) {
        let timeout = initial_output_timeout(&ladder[idx], switch_timeout_s);
        match prepare_initial_stream(&ladder[idx], source, timeout, switch_preroll_bytes).await {
            Ok((child, chunks, tail, bps)) => {
                let bytes: usize = chunks.iter().map(Vec::len).sum::<usize>() + tail.len();
                if idx == startup_ladder {
                    info!(target: "session", "tv-{} 档{} 启动 prod={}B/s initial={}B", channel_idx, idx, bps, bytes);
                } else {
                    info!(target: "session", "tv-{} 档{} 启动 prod={}B/s initial={}B (起播档{}失败后兜底)",
                        channel_idx, idx, bps, bytes, startup_ladder);
                }
                return Ok((idx, child, chunks, tail, bps));
            }
            Err(e) => {
                warn!(target: "session", "tv-{} 起播档{} 准备失败: {e}", channel_idx, idx);
                last_error = Some(e);
            }
        }
    }
    match last_error {
        Some(e) => Err(e),
        None => anyhow::bail!("empty ladder"),
    }
}

async fn recover_active_stream(
    ladder: &[Run],
    source: &str,
    current_ladder: usize,
    switch_timeout_s: f64,
    switch_preroll_bytes: usize,
    channel_idx: usize,
) -> Result<(usize, Child, Vec<Vec<u8>>, Vec<u8>, u64)> {
    let mut last_error = None;
    for idx in recovery_order(ladder.len(), current_ladder) {
        let timeout = initial_output_timeout(&ladder[idx], switch_timeout_s);
        match prepare_initial_stream(&ladder[idx], source, timeout, switch_preroll_bytes).await {
            Ok((child, chunks, tail, bps)) => {
                let bytes: usize = chunks.iter().map(Vec::len).sum::<usize>() + tail.len();
                info!(
                    target: "session",
                    "tv-{} 恢复档{} 准备完成 prod={}B/s initial={}B",
                    channel_idx,
                    idx,
                    bps,
                    bytes
                );
                return Ok((idx, child, chunks, tail, bps));
            }
            Err(e) => {
                warn!(target: "session", "tv-{} 恢复档{} 准备失败: {e}", channel_idx, idx);
                last_error = Some(e);
            }
        }
    }
    match last_error {
        Some(e) => Err(e),
        None => anyhow::bail!("empty ladder"),
    }
}

async fn prepare_initial_stream(
    run: &Run,
    source: &str,
    timeout: Duration,
    preroll_bytes: usize,
) -> Result<PreparedStream> {
    prepare_switch(run, source, timeout, preroll_bytes).await
}

async fn spawn_and_rate(run: &Run, source: &str) -> Result<(Child, u64)> {
    let mut cmd = ffmpeg::build_cmd_with_video_pid(run, source, next_video_pid());
    let child = cmd.spawn()?;
    Ok((child, run.output_bps()))
}

fn next_video_pid() -> u16 {
    VIDEO_PID_BASE + (NEXT_VIDEO_PID.fetch_add(1, Ordering::Relaxed) % VIDEO_PID_COUNT) * 2
}

fn initial_output_timeout(run: &Run, switch_timeout_s: f64) -> Duration {
    let configured = Duration::from_secs_f64(switch_timeout_s.max(0.1));
    if run.mode == "copy" {
        configured.min(INITIAL_COPY_OUTPUT_TIMEOUT_MAX)
    } else {
        configured
            .max(INITIAL_ENCODE_OUTPUT_TIMEOUT_MIN)
            .min(INITIAL_ENCODE_OUTPUT_TIMEOUT_MAX)
    }
}

fn startup_order(len: usize, startup_ladder: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let start = startup_ladder.min(len - 1);
    let mut order = Vec::with_capacity(len);
    order.push(start);
    order.extend(start + 1..len);
    order.extend(0..start);
    order
}

fn recovery_order(len: usize, current_ladder: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let current = current_ladder.min(len - 1);
    let mut order = Vec::with_capacity(len);
    order.push(current);
    order.extend(current + 1..len);
    order.extend(0..current);
    order
}

fn copy_preroll_limit(preroll_goal: usize) -> usize {
    COPY_PREROLL_BYTE_LIMIT.max(preroll_goal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_preroll_limit_never_goes_below_configured_goal() {
        assert_eq!(copy_preroll_limit(64 * 1024), COPY_PREROLL_BYTE_LIMIT);
        assert_eq!(
            copy_preroll_limit(COPY_PREROLL_BYTE_LIMIT * 2),
            COPY_PREROLL_BYTE_LIMIT * 2
        );
    }

    #[test]
    fn startup_order_tries_configured_then_lower_bitrates_then_higher() {
        assert_eq!(startup_order(5, 1), vec![1, 2, 3, 4, 0]);
        assert_eq!(startup_order(5, 0), vec![0, 1, 2, 3, 4]);
        assert_eq!(startup_order(5, 4), vec![4, 0, 1, 2, 3]);
        assert_eq!(startup_order(2, 99), vec![1, 0]);
        assert!(startup_order(0, 1).is_empty());
    }

    #[test]
    fn recovery_order_tries_current_then_lower_bitrates_then_higher() {
        assert_eq!(recovery_order(5, 2), vec![2, 3, 4, 0, 1]);
        assert_eq!(recovery_order(5, 0), vec![0, 1, 2, 3, 4]);
        assert_eq!(recovery_order(5, 4), vec![4, 0, 1, 2, 3]);
        assert_eq!(recovery_order(2, 99), vec![1, 0]);
        assert!(recovery_order(0, 1).is_empty());
    }

    fn run(mode: &str) -> Run {
        Run {
            name: mode.into(),
            mode: mode.into(),
            width: 640,
            height: 360,
            bitrate: 500,
            maxrate: 650,
            bufsize: 1000,
            preset: "ultrafast".into(),
            audio_bitrate: 96,
        }
    }

    #[test]
    fn initial_output_timeout_is_short_for_copy_but_tolerates_encode_gop_wait() {
        assert_eq!(
            initial_output_timeout(&run("copy"), 8.0),
            Duration::from_secs(3)
        );
        assert_eq!(
            initial_output_timeout(&run("encode"), 4.0),
            Duration::from_secs(8)
        );
        assert_eq!(
            initial_output_timeout(&run("encode"), 30.0),
            Duration::from_secs(12)
        );
    }

    fn ts_packet(pid: u16, payload: &[u8]) -> Vec<u8> {
        let mut packet = vec![0xff; TS_PACKET_SIZE];
        packet[0] = 0x47;
        packet[1] = ((pid >> 8) as u8) & 0x1f;
        packet[2] = pid as u8;
        packet[3] = 0x10;
        let len = payload.len().min(TS_PACKET_SIZE - 4);
        packet[4..4 + len].copy_from_slice(&payload[..len]);
        packet
    }

    fn section_packet(pid: u16, section: &[u8]) -> Vec<u8> {
        let mut packet = ts_packet(pid, &[]);
        packet[1] |= 0x40;
        packet[4] = 0;
        let len = section.len().min(TS_PACKET_SIZE - 5);
        packet[5..5 + len].copy_from_slice(&section[..len]);
        packet
    }

    fn pat_packet(pmt_pid: u16) -> Vec<u8> {
        section_packet(
            0,
            &[
                0x00,
                0xb0,
                0x0d,
                0x00,
                0x01,
                0xc1,
                0x00,
                0x00,
                0x00,
                0x01,
                0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
                pmt_pid as u8,
                0,
                0,
                0,
                0,
            ],
        )
    }

    fn pmt_packet(pmt_pid: u16, video_pid: u16, audio_pid: u16) -> Vec<u8> {
        pmt_packet_with_video_type(pmt_pid, video_pid, audio_pid, 0x1b)
    }

    fn pmt_packet_with_video_type(
        pmt_pid: u16,
        video_pid: u16,
        audio_pid: u16,
        video_stream_type: u8,
    ) -> Vec<u8> {
        section_packet(
            pmt_pid,
            &[
                0x02,
                0xb0,
                0x17,
                0x00,
                0x01,
                0xc1,
                0x00,
                0x00,
                0xe0 | ((video_pid >> 8) as u8 & 0x1f),
                video_pid as u8,
                0xf0,
                0x00,
                0x0f,
                0xe0 | ((audio_pid >> 8) as u8 & 0x1f),
                audio_pid as u8,
                0xf0,
                0x00,
                video_stream_type,
                0xe0 | ((video_pid >> 8) as u8 & 0x1f),
                video_pid as u8,
                0xf0,
                0x00,
                0,
                0,
                0,
                0,
            ],
        )
    }

    #[test]
    fn packetizer_holds_partial_ts_packet() {
        let mut tail = Vec::new();
        let mut pending = VecDeque::new();

        enqueue_complete_ts_packets(&vec![1; 100], &mut tail, &mut pending);
        assert!(pending.is_empty());
        assert_eq!(tail.len(), 100);

        enqueue_complete_ts_packets(&vec![2; 100], &mut tail, &mut pending);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].len(), TS_PACKET_SIZE);
        assert_eq!(tail.len(), 12);
    }

    #[test]
    fn marks_first_packet_per_pid_as_discontinuous_when_possible() {
        fn packet_with_adaptation(pid: u16) -> Vec<u8> {
            let mut packet = vec![0xff; TS_PACKET_SIZE];
            packet[0] = 0x47;
            packet[1] = ((pid >> 8) as u8) & 0x1f;
            packet[2] = pid as u8;
            packet[3] = 0x30;
            packet[4] = 1;
            packet[5] = 0;
            packet
        }

        let mut pending = VecDeque::from(vec![
            packet_with_adaptation(256),
            packet_with_adaptation(257),
            packet_with_adaptation(256),
        ]);

        mark_initial_discontinuity(&mut pending);

        assert_eq!(pending[0][5] & 0x80, 0x80);
        assert_eq!(pending[1][5] & 0x80, 0x80);
        assert_eq!(pending[2][5] & 0x80, 0);
    }

    #[test]
    fn prepends_adaptation_only_discontinuity_packets_for_each_pid() {
        let mut first = ts_packet(256, b"video");
        first[3] = (first[3] & 0xf0) | 7;
        let mut second = ts_packet(257, b"audio");
        second[3] = (second[3] & 0xf0) | 3;
        let mut pending = VecDeque::from(vec![first, second]);

        prepend_discontinuity_packets(&mut pending);

        assert_eq!(pending[0].len(), TS_PACKET_SIZE * 2);
        assert_eq!(ts_pid(&pending[0][..TS_PACKET_SIZE]), Some(256));
        assert_eq!(pending[0][3] & 0x30, 0x20);
        assert_eq!(pending[0][3] & 0x0f, 7);
        assert_eq!(pending[0][5] & 0x80, 0x80);
        assert_eq!(ts_pid(&pending[0][TS_PACKET_SIZE..]), Some(257));
        assert_eq!(pending[0][TS_PACKET_SIZE + 3] & 0x0f, 3);
    }

    #[test]
    fn copy_preroll_trim_keeps_pat_but_drops_pre_idr_video() {
        let mut pending = VecDeque::new();
        pending.push_back(ts_packet(0, b"old-pat"));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x41]));
        pending.push_back(ts_packet(0, b"new-pat"));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x65]));

        let trimmed = trim_pending_to_video_keyframe(pending).expect("IDR should be found");

        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert!(trimmed[0][4..].starts_with(b"new-pat"));
        assert_eq!(trimmed[0].len(), TS_PACKET_SIZE * 2);
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE..]), Some(256));
        assert!(!trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x41]));
        assert!(pending_has_video_keyframe(&trimmed));
    }

    #[test]
    fn copy_preroll_trim_keeps_h264_parameter_sets_before_idr() {
        let mut pending = VecDeque::new();
        pending.push_back(ts_packet(0, b"pat"));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x41]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x67]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x68]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x65]));

        let trimmed = trim_pending_to_video_keyframe(pending).expect("IDR should be found");

        assert_eq!(trimmed[0].len(), TS_PACKET_SIZE * 4);
        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x67]));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x68]));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x65]));
        assert!(!trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x41]));
    }

    #[test]
    fn copy_preroll_trim_keeps_non_adjacent_h264_parameter_sets_before_idr() {
        let mut pending = VecDeque::new();
        pending.push_back(ts_packet(0, b"pat"));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x67]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x68]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x41]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x65]));

        let trimmed = trim_pending_to_video_keyframe(pending).expect("IDR should be found");

        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x67]));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x68]));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x65]));
        assert!(!trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x41]));
    }

    #[test]
    fn preroll_can_trim_to_second_keyframe_for_encode_startup() {
        let mut pending = VecDeque::new();
        pending.push_back(ts_packet(0, b"pat"));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x67]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x68]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x65, 0x01]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x41]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x67]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x68]));
        pending.push_back(ts_packet(256, &[0, 0, 1, 0x65, 0x02]));

        let trimmed =
            trim_pending_to_nth_video_keyframe(pending, 2).expect("second IDR should be found");

        assert!(!trimmed[0].windows(5).any(|w| w == [0, 0, 1, 0x65, 0x01]));
        assert!(trimmed[0].windows(5).any(|w| w == [0, 0, 1, 0x65, 0x02]));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x67]));
        assert!(trimmed[0].windows(4).any(|w| w == [0, 0, 1, 0x68]));
    }

    #[test]
    fn encode_preroll_trim_starts_at_pes_boundary_for_second_keyframe() {
        let pmt_pid = 100;
        let video_pid = 256;
        let audio_pid = 257;
        let mut dirty = ts_packet(video_pid, &[0, 0, 1, 0x41]);
        dirty[1] |= 0x40;
        let mut first_idr = ts_packet(video_pid, &[0, 0, 1, 0x65, 0x01]);
        first_idr[1] |= 0x40;
        let mut second_idr = ts_packet(video_pid, &[0, 0, 1, 0x65, 0x02]);
        second_idr[1] |= 0x40;
        let pending = VecDeque::from(vec![
            pat_packet(pmt_pid),
            pmt_packet(pmt_pid, video_pid, audio_pid),
            dirty,
            first_idr,
            second_idr,
        ]);

        let trimmed = trim_pending_to_nth_video_keyframe_pes(pending, 2)
            .expect("second IDR PES should be found");

        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE..]), Some(pmt_pid));
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE * 2..]), Some(video_pid));
        assert!(trimmed[0][TS_PACKET_SIZE * 2 + 1] & 0x40 != 0);
        assert!(!trimmed[0].windows(5).any(|w| w == [0, 0, 1, 0x65, 0x01]));
        assert!(trimmed[0].windows(5).any(|w| w == [0, 0, 1, 0x65, 0x02]));
    }

    #[test]
    fn preroll_drops_initial_audio_continuation_until_pes_start() {
        let pmt_pid = 100;
        let video_pid = 256;
        let audio_pid = 257;
        let mut video_idr = ts_packet(video_pid, &[0, 0, 1, 0x65]);
        video_idr[1] |= 0x40;
        let audio_continuation = ts_packet(audio_pid, b"partial-aac");
        let mut audio_start = ts_packet(audio_pid, b"fresh-aac");
        audio_start[1] |= 0x40;
        let pending = VecDeque::from(vec![
            pat_packet(pmt_pid),
            pmt_packet(pmt_pid, video_pid, audio_pid),
            video_idr,
            audio_continuation,
            audio_start,
        ]);

        let trimmed =
            trim_pending_to_nth_video_keyframe_pes(pending, 1).expect("video PES should be found");
        let cleaned = drop_initial_non_video_elementary_continuations(trimmed);

        assert!(!cleaned[0].windows(11).any(|w| w == b"partial-aac"));
        assert!(cleaned[0].windows(9).any(|w| w == b"fresh-aac"));
        assert!(pending_has_video_keyframe(&cleaned));
    }

    #[test]
    fn h264_idr_detection_handles_start_code_split_across_ts_packets() {
        let mut pending = VecDeque::new();
        pending.push_back(ts_packet(0, b"pat"));
        let mut first_payload = vec![0xaa; TS_PACKET_SIZE - 6];
        first_payload.extend_from_slice(&[0, 0]);
        pending.push_back(ts_packet(256, &first_payload));
        pending.push_back(ts_packet(256, &[1, 0x65, 0x88]));

        let trimmed = trim_pending_to_video_keyframe(pending).expect("split IDR should be found");

        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert!(pending_has_video_keyframe(&trimmed));
    }

    #[test]
    fn h264_idr_detection_uses_pmt_video_pid() {
        let pmt_pid = 100;
        let video_pid = 256;
        let audio_pid = 257;
        let mut pending = VecDeque::new();
        pending.push_back(pat_packet(pmt_pid));
        pending.push_back(pmt_packet(pmt_pid, video_pid, audio_pid));
        pending.push_back(ts_packet(audio_pid, &[0, 0, 1, 0x65]));
        pending.push_back(pat_packet(pmt_pid));
        pending.push_back(pmt_packet(pmt_pid, video_pid, audio_pid));
        pending.push_back(ts_packet(video_pid, &[0, 0, 1, 0x65]));

        let trimmed = trim_pending_to_video_keyframe(pending).expect("video IDR should be found");

        assert_eq!(trimmed[0].len(), TS_PACKET_SIZE * 3);
        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE..]), Some(pmt_pid));
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE * 2..]), Some(video_pid));
    }

    #[test]
    fn h265_idr_detection_uses_pmt_video_pid() {
        let pmt_pid = 100;
        let video_pid = 300;
        let audio_pid = 301;
        let mut pending = VecDeque::new();
        pending.push_back(pat_packet(pmt_pid));
        pending.push_back(pmt_packet_with_video_type(
            pmt_pid, video_pid, audio_pid, 0x24,
        ));
        pending.push_back(ts_packet(audio_pid, &[0, 0, 1, 0x26]));
        pending.push_back(pat_packet(pmt_pid));
        pending.push_back(pmt_packet_with_video_type(
            pmt_pid, video_pid, audio_pid, 0x24,
        ));
        pending.push_back(ts_packet(video_pid, &[0, 0, 1, 0x26, 0x01]));

        let trimmed = trim_pending_to_video_keyframe(pending).expect("HEVC IDR should be found");

        assert_eq!(trimmed[0].len(), TS_PACKET_SIZE * 3);
        assert_eq!(ts_pid(&trimmed[0]), Some(0));
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE..]), Some(pmt_pid));
        assert_eq!(ts_pid(&trimmed[0][TS_PACKET_SIZE * 2..]), Some(video_pid));
    }
}
