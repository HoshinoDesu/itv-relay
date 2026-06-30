//! 档位状态机 (v5): pool 字节斜率驱动, 直通期测链路 + 试探式升档。
//!
//! 信号: backlog_bytes 斜率 (EWMA) 是主信号——pool 涨=产>消=真拥塞, 不被TCP缓冲永久掩盖。
//! 三态: Direct(直通测链路+首降) / Encode(编码稳态+单步降+试探升) / Probing(升档观察期)。
//! 降档: Direct 首降用 link_estimate 直跳合适档; Encode 单步降 (不盲跳档max)。
//! 升档: 单步试探 + 观察期斜率回退 + 退避锁 + 回退到 last_stable (非档max)。

use crate::congestion::Sample;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const DOWN_SAFE: f64 = 0.75;
const LINK_CONFIRM_S: f64 = 2.0;
const FILL_RATIO: f64 = 0.04;
const STABLE_RATIO: f64 = 0.015;
const EWMA_ALPHA_FAST: f64 = 0.5;
const EWMA_ALPHA_SLOW: f64 = 0.2;
const STABLE_CONFIRM_S: f64 = 12.0;
const UP_PROBE_S: f64 = 12.0;
const UP_COOLDOWN_S: f64 = 20.0;
const STABLE_PROTECT_S: f64 = 15.0;
const PROBE_GRACE_S: f64 = 2.0;
const EMERGENCY_RATIO: f64 = 0.85;
const BACKOFF_BASE_S: f64 = 60.0;
const BACKOFF_MAX_S: f64 = 600.0;
const CONSECUTIVE_FAIL_LIMIT: u32 = 3;
const UP_DISABLE_S: f64 = 1800.0;

#[derive(Debug, Clone, PartialEq)]
enum Regime {
    Direct,
    Encode,
    Probing,
}

struct SwitchSnapshot {
    current: usize,
    last_stable: usize,
    regime: Regime,
    probe_until: Option<Instant>,
    probe_target: Option<usize>,
    prev_backlog_bytes: u64,
    rate_ewma: f64,
    stable_protect_until: Option<Instant>,
}

pub struct StateMachine {
    pub current: usize,
    pub max_index: usize,
    regime: Regime,
    rate_ewma: f64,
    prev_backlog_bytes: u64,
    source_bps: f64,
    link_estimate: f64,
    link_confirmed: bool,
    link_confirm_accum: f64,
    probe_until: Option<Instant>,
    probe_target: Option<usize>,
    probe_baseline_bytes: u64,
    stable_accum: f64,
    down_accum: f64,
    last_stable: usize,
    last_change_at: Instant,
    down_cooldown_until: Option<Instant>,
    stable_protect_until: Option<Instant>,
    locked_until: HashMap<usize, Instant>,
    fail_count: HashMap<usize, u32>,
    consecutive_up_fails: u32,
    up_disabled_until: Option<Instant>,
    ladder_bps: Vec<u64>,
    cfg: CongestionCfg,
    pending_snapshot: Option<SwitchSnapshot>,
}

pub enum Decision {
    StepDown(usize),
    RevertDown(usize),
    StepUp(usize),
    Hold,
}

impl StateMachine {
    pub fn new(cfg: CongestionCfg, max_index: usize, ladder_bps: Vec<u64>, startup_ladder: usize) -> Self {
        let source_bps = ladder_bps.first().copied().unwrap_or(800_000) as f64;
        let startup = startup_ladder.min(max_index);
        let regime = if startup == 0 { Regime::Direct } else { Regime::Encode };
        Self {
            current: startup,
            max_index,
            regime,
            rate_ewma: 0.0,
            prev_backlog_bytes: 0,
            source_bps,
            link_estimate: 0.0,
            link_confirmed: false,
            link_confirm_accum: 0.0,
            probe_until: None,
            probe_target: None,
            probe_baseline_bytes: 0,
            stable_accum: 0.0,
            down_accum: 0.0,
            last_stable: startup,
            last_change_at: Instant::now() - Duration::from_secs(1000),
            down_cooldown_until: None,
            stable_protect_until: None,
            locked_until: HashMap::new(),
            fail_count: HashMap::new(),
            consecutive_up_fails: 0,
            up_disabled_until: None,
            ladder_bps,
            cfg,
            pending_snapshot: None,
        }
    }

    /// 切档成功后调用: 确认状态变更 + 重置斜率基准 (pool 已被 clear)。
    pub fn confirm_switch(&mut self) {
        self.pending_snapshot = None;
        self.prev_backlog_bytes = 0;
        self.rate_ewma = 0.0;
    }

    /// 切档失败后调用: 回滚 update() 中预提交的 current/regime 等状态。
    pub fn rollback_switch(&mut self) {
        if let Some(snap) = self.pending_snapshot.take() {
            self.current = snap.current;
            self.last_stable = snap.last_stable;
            self.regime = snap.regime;
            self.probe_until = snap.probe_until;
            self.probe_target = snap.probe_target;
            self.prev_backlog_bytes = snap.prev_backlog_bytes;
            self.rate_ewma = snap.rate_ewma;
            self.stable_protect_until = snap.stable_protect_until;
        }
    }

    fn save_snapshot(&mut self) {
        self.pending_snapshot = Some(SwitchSnapshot {
            current: self.current,
            last_stable: self.last_stable,
            regime: self.regime.clone(),
            probe_until: self.probe_until,
            probe_target: self.probe_target,
            prev_backlog_bytes: self.prev_backlog_bytes,
            rate_ewma: self.rate_ewma,
            stable_protect_until: self.stable_protect_until,
        });
    }

    pub fn update(&mut self, s: &Sample, dt: Duration, now: Instant) -> Decision {
        let secs = dt.as_secs_f64();
        let current_bps = self.ladder_bps.get(self.current).copied().unwrap_or(800_000) as f64;

        // pool 斜率 EWMA (自适应: 拥塞时快响应, 稳态时多平滑)
        let delta = s.backlog_bytes as i64 - self.prev_backlog_bytes as i64;
        self.prev_backlog_bytes = s.backlog_bytes;
        let raw_rate = delta as f64 / secs.max(0.01);
        let alpha = if raw_rate.abs() > current_bps * 0.02 { EWMA_ALPHA_FAST } else { EWMA_ALPHA_SLOW };
        self.rate_ewma = self.rate_ewma * (1.0 - alpha) + raw_rate * alpha;

        // 测源码率: 直通且 pool 空 → drain ≈ 源产出, 用 EWMA 双向跟踪。
        let src_cap = self.ladder_bps.first().copied().unwrap_or(800_000) as f64;
        if self.current == 0 && s.backlog_ratio < 0.05 && s.drain_bps > 0 {
            let sample = (s.drain_bps as f64).min(src_cap);
            self.source_bps = self.source_bps * 0.8 + sample * 0.2;
        }

        tracing::debug!(target: "state",
            "档{} {:?} pool={}B slope={}B/s source={}B/s link={}B/{} drain={}B/s",
            self.current, self.regime, s.backlog_bytes, self.rate_ewma as i64,
            self.source_bps as u64, self.link_estimate as u64, self.link_confirmed,
            s.drain_bps);

        match self.regime {
            Regime::Direct => self.update_direct(s, secs, now),
            Regime::Probing => self.update_probing(s, secs, now),
            Regime::Encode => self.update_encode(s, dt, secs, now),
        }
    }

    fn update_direct(&mut self, s: &Sample, secs: f64, now: Instant) -> Decision {
        let current_bps = self.ladder_bps.get(self.current).copied().unwrap_or(800_000) as f64;
        let fill_thresh = current_bps * FILL_RATIO;

        // 紧急 (pool 物理满): 没机会干净测链路, 用保守估计首降
        if s.backlog_ratio >= EMERGENCY_RATIO {
            let est = if self.link_confirmed { self.link_estimate } else { self.source_bps * 0.3 };
            let target = self.pick_within(est * DOWN_SAFE).max(1).min(self.max_index);
            info!(target: "state", "↓首降(紧急) 0→{} (est={}B/s pool={:.0}%)", target, est as u64, s.backlog_ratio * 100.0);
            return self.enter_encode(target, now);
        }
        // 测链路: pool 在涨 (正斜率) 持续确认
        if self.rate_ewma > fill_thresh {
            self.link_confirm_accum += secs;
            if self.link_confirm_accum >= LINK_CONFIRM_S {
                self.link_estimate = (self.source_bps - self.rate_ewma).max(0.0);
                self.link_confirmed = true;
                let target = self.pick_within(self.link_estimate * DOWN_SAFE).max(1).min(self.max_index);
                info!(target: "state", "↓首降 0→{} (link={}B/s slope={}B/s source={}B/s pool={:.0}%)",
                    target, self.link_estimate as u64, self.rate_ewma as i64, self.source_bps as u64, s.backlog_ratio * 100.0);
                return self.enter_encode(target, now);
            }
        } else {
            self.link_confirm_accum = 0.0;
        }
        Decision::Hold
    }

    fn update_probing(&mut self, s: &Sample, secs: f64, now: Instant) -> Decision {
        let target = self.probe_target.unwrap();
        let probe_end = self.probe_until.unwrap();
        let since_change = now.duration_since(self.last_change_at).as_secs_f64();
        let grace = since_change < PROBE_GRACE_S;

        let target_bps = self.ladder_bps.get(target).copied().unwrap_or(800_000) as f64;
        let fill_thresh = target_bps * FILL_RATIO;
        let rise_limit = (target_bps * UP_PROBE_S * 0.05) as i64;

        // 失败: pool 字节相对 baseline 上涨超阈值, 或斜率持续>0
        let risen = s.backlog_bytes as i64 - self.probe_baseline_bytes as i64;
        let fail = !grace && (risen > rise_limit || self.rate_ewma > fill_thresh);
        if fail {
            warn!(target: "state", "升档观察期失败 {}→{} 回退到稳定档{} (risen={}B slope={}B/s pool={:.0}%)",
                target + 1, target, self.last_stable, risen, self.rate_ewma as i64, s.backlog_ratio * 100.0);
            self.lock(now, target);
            let revert = self.last_stable.max(1).min(self.max_index);
            self.save_snapshot();
            self.current = revert;
            self.regime = if revert == 0 { Regime::Direct } else { Regime::Encode };
            self.probe_until = None;
            self.probe_target = None;
            self.stable_accum = 0.0;
            self.down_accum = 0.0;
            self.link_confirm_accum = 0.0;
            self.stable_protect_until = Some(now + Duration::from_secs_f64(STABLE_PROTECT_S));
            self.last_change_at = now;
            return Decision::RevertDown(revert);
        }
        if now >= probe_end {
            self.regime = Regime::Encode;
            self.last_stable = target;
            // 观察期通过 = 升档成功, 重置全局连续失败计数 + 清该档退避
            self.consecutive_up_fails = 0;
            self.fail_count.remove(&target);
            self.locked_until.remove(&target);
            self.probe_until = None;
            self.probe_target = None;
            self.stable_accum = 0.0;
            info!(target: "state", "升档观察期通过, 稳定档 {}", target);
            return Decision::Hold;
        }
        let _ = secs;
        Decision::Hold
    }

    fn update_encode(&mut self, s: &Sample, dt: Duration, secs: f64, now: Instant) -> Decision {
        let current_bps = self.ladder_bps.get(self.current).copied().unwrap_or(800_000) as f64;
        let fill_thresh = current_bps * FILL_RATIO;
        let stable_thresh = current_bps * STABLE_RATIO;

        let protect = self.stable_protect_until.map(|t| now < t).unwrap_or(false);
        let cd_down = self.down_cooldown_until.map(|t| now < t).unwrap_or(false);
        // 紧急总是降
        let emergency = s.backlog_ratio >= EMERGENCY_RATIO;
        let congested = self.rate_ewma > fill_thresh;
        // 降档: 紧急 或 (非保护 且 非降档冷却 且 拥塞持续 hold)
        if (emergency || (congested && !protect && !cd_down)) && self.current < self.max_index {
            self.down_accum += secs;
            if emergency || self.down_accum >= self.cfg.down_hold_s {                // 编码 regime 链路不可测 → 只降一档
                let target = (self.current + 1).min(self.max_index);
                info!(target: "state", "↓降档 {}→{} (slope={}B/s pool={:.0}%{})",
                    self.current, target, self.rate_ewma as i64, s.backlog_ratio * 100.0,
                    if emergency {" [紧急]"} else {""});
                self.save_snapshot();
                self.current = target;
                self.last_stable = target;
                self.down_accum = 0.0;
                self.stable_accum = 0.0;
                self.down_cooldown_until = Some(now + Duration::from_secs_f64(self.cfg.down_cooldown_s));
                self.last_change_at = now;
                return Decision::StepDown(target);
            }
        } else {
            self.down_accum = 0.0;
        }

        // 升档试探: 稳态 (pool 平+空) 持续确认
        let stable = self.rate_ewma.abs() < stable_thresh && s.backlog_ratio < 0.10;
        let cd_up = now.duration_since(self.last_change_at).as_secs_f64() < UP_COOLDOWN_S;
        // 全局熔断: 连续 3 次升档失败 → 暂停升档 30min
        let up_disabled = self.up_disabled_until.map(|t| now < t).unwrap_or(false);
        if up_disabled {
            self.stable_accum = 0.0;
            return Decision::Hold;
        }
        if stable && !cd_up {
            self.stable_accum += secs;
            if self.stable_accum >= STABLE_CONFIRM_S && self.current > 0 {
                let target = self.current - 1; // 单步升
                let locked = matches!(self.locked_until.get(&target), Some(u) if now < *u);
                if !locked {
                    info!(target: "state", "↑升档 {}→{} (slope={}B/s pool={:.0}% stable={:.1}s)",
                        self.current, target, self.rate_ewma as i64, s.backlog_ratio * 100.0, self.stable_accum);
                    self.save_snapshot();
                    self.current = target;
                    self.regime = Regime::Probing;
                    self.probe_target = Some(target);
                    self.probe_until = Some(now + Duration::from_secs_f64(UP_PROBE_S));
                    self.probe_baseline_bytes = s.backlog_bytes;
                    self.stable_accum = 0.0;
                    self.last_change_at = now;
                    return Decision::StepUp(target);
                }
            }
        } else {
            self.stable_accum = 0.0;
        }
        let _ = dt;
        Decision::Hold
    }

    fn enter_encode(&mut self, target: usize, now: Instant) -> Decision {
        self.save_snapshot();
        self.current = target;
        self.last_stable = target;
        self.regime = Regime::Encode;
        self.link_confirm_accum = 0.0;
        self.down_cooldown_until = Some(now + Duration::from_secs_f64(self.cfg.down_cooldown_s));
        self.last_change_at = now;
        self.down_accum = 0.0;
        self.stable_accum = 0.0;
        self.prev_backlog_bytes = 0; // 切档重置斜率基准
        self.stable_protect_until = None;
        Decision::StepDown(target)
    }

    /// 取 bitrate ≤ cap 的最高档 (从档0往下找; 至少档1)
    fn pick_within(&self, cap: f64) -> usize {
        for i in 0..=self.max_index {
            if let Some(&bps) = self.ladder_bps.get(i) {
                if (bps as f64) <= cap {
                    return i.max(1).min(self.max_index);
                }
            }
        }
        self.max_index.max(1)
    }

    fn lock(&mut self, now: Instant, ladder: usize) {
        let count = self.fail_count.entry(ladder).and_modify(|c| *c += 1).or_insert(1);
        let backoff = (BACKOFF_BASE_S * 2f64.powi((*count - 1) as i32)).min(BACKOFF_MAX_S);
        self.locked_until.insert(ladder, now + Duration::from_secs_f64(backoff));
        info!(target: "state", "锁档 {} {}s (第{}次)", ladder, backoff as u64, count);

        // 全局连续升档失败计数: 达 3 → 暂停升档 30min
        self.consecutive_up_fails += 1;
        if self.consecutive_up_fails >= CONSECUTIVE_FAIL_LIMIT {
            self.up_disabled_until = Some(now + Duration::from_secs_f64(UP_DISABLE_S));
            warn!(target: "state", "连续 {} 次升档失败, 暂停升档 {}s", self.consecutive_up_fails, UP_DISABLE_S as u64);
        }
    }
}

// 兼容 config 字段 (供未来调参)
pub struct CongestionCfg {
    pub down_ratio: f64,
    pub down_hold_s: f64,
    pub up_ratio: f64,
    pub up_hold_s: f64,
    pub down_cooldown_s: f64,
}

impl From<&crate::config::Config> for CongestionCfg {
    fn from(c: &crate::config::Config) -> Self {
        let x = &c.congestion;
        Self {
            down_ratio: x.down_ratio,
            down_hold_s: x.down_hold_s,
            up_ratio: x.up_ratio,
            up_hold_s: x.up_hold_s,
            down_cooldown_s: x.down_cooldown_s,
        }
    }
}
