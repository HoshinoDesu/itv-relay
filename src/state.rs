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
const EWMA_ALPHA_FAST: f64 = 0.5;
const EWMA_ALPHA_SLOW: f64 = 0.2;
const FALLBACK_SOURCE_BPS: u64 = 1_000_000;
const UP_PROBE_S: f64 = 12.0;
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
    probe_baseline_bytes: u64,
    prev_backlog_bytes: u64,
    rate_ewma: f64,
    link_confirm_accum: f64,
    stable_accum: f64,
    down_accum: f64,
    last_change_at: Instant,
    down_cooldown_until: Option<Instant>,
    stable_protect_until: Option<Instant>,
    locked_until: HashMap<usize, Instant>,
    fail_count: HashMap<usize, u32>,
    consecutive_up_fails: u32,
    up_disabled_until: Option<Instant>,
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

#[derive(Debug, PartialEq)]
pub enum Decision {
    StepDown(usize),
    RevertDown(usize),
    StepUp(usize),
    Hold,
}

impl StateMachine {
    pub fn new(
        cfg: CongestionCfg,
        max_index: usize,
        ladder_bps: Vec<u64>,
        startup_ladder: usize,
    ) -> Self {
        let source_bps = ladder_bps.first().copied().unwrap_or(FALLBACK_SOURCE_BPS) as f64;
        let startup = startup_ladder.min(max_index);
        let regime = if startup == 0 {
            Regime::Direct
        } else {
            Regime::Encode
        };
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

    /// 切档成功后调用: 确认状态变更, 并用当前 pool 字节数作为新斜率基准。
    pub fn confirm_switch(&mut self, backlog_bytes: u64) {
        self.pending_snapshot = None;
        self.prev_backlog_bytes = backlog_bytes;
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
            self.probe_baseline_bytes = snap.probe_baseline_bytes;
            self.prev_backlog_bytes = snap.prev_backlog_bytes;
            self.rate_ewma = snap.rate_ewma;
            self.link_confirm_accum = snap.link_confirm_accum;
            self.stable_accum = snap.stable_accum;
            self.down_accum = snap.down_accum;
            self.last_change_at = snap.last_change_at;
            self.down_cooldown_until = snap.down_cooldown_until;
            self.stable_protect_until = snap.stable_protect_until;
            self.locked_until = snap.locked_until;
            self.fail_count = snap.fail_count;
            self.consecutive_up_fails = snap.consecutive_up_fails;
            self.up_disabled_until = snap.up_disabled_until;
        }
    }

    /// 后台切档准备失败: 回滚预提交状态, 并短期锁住失败目标避免反复 spawn。
    pub fn rollback_failed_switch(&mut self, target: usize, now: Instant) {
        let previous = self.pending_snapshot.as_ref().map(|snap| snap.current);
        self.rollback_switch();
        let was_upgrade = previous.is_some_and(|current| target < current);
        self.lock_target(now, target, was_upgrade);
    }

    fn save_snapshot(&mut self) {
        self.pending_snapshot = Some(SwitchSnapshot {
            current: self.current,
            last_stable: self.last_stable,
            regime: self.regime.clone(),
            probe_until: self.probe_until,
            probe_target: self.probe_target,
            probe_baseline_bytes: self.probe_baseline_bytes,
            prev_backlog_bytes: self.prev_backlog_bytes,
            rate_ewma: self.rate_ewma,
            link_confirm_accum: self.link_confirm_accum,
            stable_accum: self.stable_accum,
            down_accum: self.down_accum,
            last_change_at: self.last_change_at,
            down_cooldown_until: self.down_cooldown_until,
            stable_protect_until: self.stable_protect_until,
            locked_until: self.locked_until.clone(),
            fail_count: self.fail_count.clone(),
            consecutive_up_fails: self.consecutive_up_fails,
            up_disabled_until: self.up_disabled_until,
        });
    }

    pub fn request_startup_probe(
        &mut self,
        target: usize,
        backlog_bytes: u64,
        now: Instant,
    ) -> Option<Decision> {
        if target >= self.current || !matches!(self.regime, Regime::Encode) {
            return None;
        }
        let locked = self.is_locked(target, now);
        let up_disabled = self.up_disabled_until.map(|t| now < t).unwrap_or(false);
        if locked || up_disabled {
            return None;
        }

        info!(target: "state", "↑起播快速试探 {}→{}", self.current, target);
        self.save_snapshot();
        self.current = target;
        self.regime = Regime::Probing;
        self.probe_target = Some(target);
        self.probe_until = Some(now + Duration::from_secs_f64(UP_PROBE_S));
        self.probe_baseline_bytes = backlog_bytes;
        self.stable_accum = 0.0;
        self.down_accum = 0.0;
        self.last_change_at = now;
        Some(Decision::StepUp(target))
    }

    pub fn update(&mut self, s: &Sample, dt: Duration, now: Instant) -> Decision {
        let secs = dt.as_secs_f64();
        let ladder_current_bps = self
            .ladder_bps
            .get(self.current)
            .copied()
            .unwrap_or(FALLBACK_SOURCE_BPS);
        let current_bps = if matches!(self.regime, Regime::Direct) {
            self.source_bps.max(1.0)
        } else {
            s.prod_bps.max(ladder_current_bps) as f64
        };

        // pool 斜率 EWMA (自适应: 拥塞时快响应, 稳态时多平滑)
        let delta = s.backlog_bytes as i64 - self.prev_backlog_bytes as i64;
        self.prev_backlog_bytes = s.backlog_bytes;
        let raw_rate = delta as f64 / secs.max(0.01);
        let alpha = if raw_rate.abs() > current_bps * 0.02 {
            EWMA_ALPHA_FAST
        } else {
            EWMA_ALPHA_SLOW
        };
        self.rate_ewma = self.rate_ewma * (1.0 - alpha) + raw_rate * alpha;

        self.observe_direct_source_bps(s, raw_rate);

        tracing::debug!(target: "state",
            "档{} {:?} pool={}B delta={}B slope={}B/s source={}B/s link={}B/{} drain={}B/s",
            self.current, self.regime, s.backlog_bytes, s.backlog_delta, self.rate_ewma as i64,
            self.source_bps as u64, self.link_estimate as u64, self.link_confirmed,
            s.drain_bps);

        if !s.has_active_clients {
            self.stable_accum = 0.0;
            self.down_accum = 0.0;
            self.link_confirm_accum = 0.0;
            return Decision::Hold;
        }

        match self.regime {
            Regime::Direct => self.update_direct(s, secs, now),
            Regime::Probing => self.update_probing(s, secs, now),
            Regime::Encode => self.update_encode(s, secs, now),
        }
    }

    fn down_fill_ratio(&self) -> f64 {
        (1.0 - self.cfg.down_ratio).clamp(0.02, 0.50)
    }

    fn stable_slope_ratio(&self) -> f64 {
        (1.0 - self.cfg.up_ratio).clamp(0.005, 0.20)
    }

    fn observe_direct_source_bps(&mut self, s: &Sample, raw_rate: f64) {
        if self.current != 0 || s.drain_bps == 0 {
            return;
        }

        let growth_bps = raw_rate.max(self.rate_ewma).max(0.0);
        let sample = s.drain_bps as f64 + growth_bps;
        let alpha = if growth_bps > 0.0 { 0.50 } else { 0.20 };
        self.source_bps = self.source_bps * (1.0 - alpha) + sample * alpha;
    }

    fn update_direct(&mut self, s: &Sample, secs: f64, now: Instant) -> Decision {
        let fill_thresh = self.source_bps.max(1.0) * self.down_fill_ratio();

        // 紧急 (pool 物理满): 没机会干净测链路, 用保守估计首降
        if s.backlog_ratio >= EMERGENCY_RATIO {
            let est = if self.link_confirmed {
                self.link_estimate
            } else {
                self.source_bps * 0.3
            };
            if let Some(target) = self.pick_within_unlocked(est * DOWN_SAFE, now) {
                info!(target: "state", "↓首降(紧急) 0→{} (est={}B/s pool={:.0}%)", target, est as u64, s.backlog_ratio * 100.0);
                return self.enter_encode(target, now);
            }
            warn!(target: "state", "直通紧急拥塞, 但所有降档目标仍在退避中");
            return Decision::Hold;
        }
        // 测链路: pool 在涨 (正斜率) 持续确认
        if self.rate_ewma > fill_thresh {
            self.link_confirm_accum += secs;
            if self.link_confirm_accum >= LINK_CONFIRM_S {
                self.link_estimate = (self.source_bps - self.rate_ewma).max(0.0);
                self.link_confirmed = true;
                if let Some(target) = self.pick_within_unlocked(self.link_estimate * DOWN_SAFE, now)
                {
                    info!(target: "state", "↓首降 0→{} (link={}B/s slope={}B/s source={}B/s pool={:.0}%)",
                        target, self.link_estimate as u64, self.rate_ewma as i64, self.source_bps as u64, s.backlog_ratio * 100.0);
                    return self.enter_encode(target, now);
                }
                warn!(target: "state", "直通拥塞已确认, 但所有降档目标仍在退避中");
                return Decision::Hold;
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

        let target_bps = self.probe_target_bps(target);
        let fill_thresh = target_bps * self.down_fill_ratio();
        let rise_limit = (target_bps * UP_PROBE_S * 0.05) as i64;

        // 失败: pool 字节相对 baseline 上涨超阈值, 或斜率持续>0
        let risen = s.backlog_bytes as i64 - self.probe_baseline_bytes as i64;
        let fail = !grace && (risen > rise_limit || self.rate_ewma > fill_thresh);
        if fail {
            warn!(target: "state", "升档观察期失败 {}→{} 回退到稳定档{} (risen={}B slope={}B/s pool={:.0}%)",
                target + 1, target, self.last_stable, risen, self.rate_ewma as i64, s.backlog_ratio * 100.0);
            let revert = self.last_stable.max(1).min(self.max_index);
            self.save_snapshot();
            self.lock_target(now, target, true);
            self.current = revert;
            self.regime = if revert == 0 {
                Regime::Direct
            } else {
                Regime::Encode
            };
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
            self.regime = if target == 0 {
                Regime::Direct
            } else {
                Regime::Encode
            };
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

    fn update_encode(&mut self, s: &Sample, secs: f64, now: Instant) -> Decision {
        let current_bps = self
            .ladder_bps
            .get(self.current)
            .copied()
            .unwrap_or(FALLBACK_SOURCE_BPS) as f64;
        let fill_thresh = current_bps * self.down_fill_ratio();
        let stable_thresh = current_bps * self.stable_slope_ratio();

        let protect = self.stable_protect_until.map(|t| now < t).unwrap_or(false);
        let cd_down = self.down_cooldown_until.map(|t| now < t).unwrap_or(false);
        // 紧急总是降
        let emergency = s.backlog_ratio >= EMERGENCY_RATIO;
        let congested = self.rate_ewma > fill_thresh;
        // 降档: 紧急 或 (非保护 且 非降档冷却 且 拥塞持续 hold)
        if (emergency || (congested && !protect && !cd_down)) && self.current < self.max_index {
            self.down_accum += secs;
            if emergency || self.down_accum >= self.cfg.down_hold_s.max(0.5) {
                // 编码 regime 链路不可测 → 只降一档
                let Some(target) = self.next_unlocked_down(now) else {
                    warn!(target: "state", "拥塞持续, 但所有更低档目标仍在退避中");
                    return Decision::Hold;
                };
                info!(target: "state", "↓降档 {}→{} (slope={}B/s pool={:.0}%{})",
                    self.current, target, self.rate_ewma as i64, s.backlog_ratio * 100.0,
                    if emergency {" [紧急]"} else {""});
                self.save_snapshot();
                self.current = target;
                self.last_stable = target;
                self.down_accum = 0.0;
                self.stable_accum = 0.0;
                self.down_cooldown_until =
                    Some(now + Duration::from_secs_f64(self.cfg.down_cooldown_s));
                self.last_change_at = now;
                return Decision::StepDown(target);
            }
        } else {
            self.down_accum = 0.0;
        }

        // 升档试探: 稳态 (pool 平+空) 持续确认
        let stable = self.rate_ewma.abs() < stable_thresh && s.backlog_ratio < 0.10;
        let cd_up =
            now.duration_since(self.last_change_at).as_secs_f64() < self.cfg.up_cooldown_s.max(0.0);
        // 全局熔断: 连续 3 次升档失败 → 暂停升档 30min
        let up_disabled = self.up_disabled_until.map(|t| now < t).unwrap_or(false);
        if up_disabled {
            self.stable_accum = 0.0;
            return Decision::Hold;
        }
        if stable && !cd_up {
            self.stable_accum += secs;
            if self.stable_accum >= self.cfg.up_hold_s.max(1.0) && self.current > 0 {
                let target = self.current - 1; // 单步升
                let locked = self.is_locked(target, now);
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
        Decision::Hold
    }

    fn probe_target_bps(&self, target: usize) -> f64 {
        if target == 0 {
            self.source_bps.max(1.0)
        } else {
            self.ladder_bps
                .get(target)
                .copied()
                .unwrap_or(FALLBACK_SOURCE_BPS) as f64
        }
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
        self.stable_protect_until = None;
        Decision::StepDown(target)
    }

    /// 取 bitrate ≤ cap 的最高档; 如果没有满足 cap 的档, 退到可用的最低码率档。
    fn pick_within_unlocked(&self, cap: f64, now: Instant) -> Option<usize> {
        for i in 1..=self.max_index {
            if self.is_locked(i, now) {
                continue;
            }
            if let Some(&bps) = self.ladder_bps.get(i) {
                if (bps as f64) <= cap {
                    return Some(i);
                }
            }
        }
        (1..=self.max_index)
            .rev()
            .find(|&i| !self.is_locked(i, now))
    }

    fn next_unlocked_down(&self, now: Instant) -> Option<usize> {
        ((self.current + 1)..=self.max_index).find(|&i| !self.is_locked(i, now))
    }

    fn is_locked(&self, ladder: usize, now: Instant) -> bool {
        matches!(self.locked_until.get(&ladder), Some(until) if now < *until)
    }

    fn lock_target(&mut self, now: Instant, ladder: usize, counts_as_upgrade_fail: bool) {
        let count = self
            .fail_count
            .entry(ladder)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        let backoff = (BACKOFF_BASE_S * 2f64.powi((*count - 1) as i32)).min(BACKOFF_MAX_S);
        self.locked_until
            .insert(ladder, now + Duration::from_secs_f64(backoff));
        info!(target: "state", "锁档 {} {}s (第{}次)", ladder, backoff as u64, count);

        // 全局连续升档失败计数: 达 3 → 暂停升档 30min
        if !counts_as_upgrade_fail {
            return;
        }
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
    pub up_cooldown_s: f64,
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
            up_cooldown_s: x.up_cooldown_s,
            down_cooldown_s: x.down_cooldown_s,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CongestionCfg {
        CongestionCfg {
            down_ratio: 0.95,
            down_hold_s: 1.0,
            up_ratio: 0.95,
            up_hold_s: 2.0,
            up_cooldown_s: 20.0,
            down_cooldown_s: 8.0,
        }
    }

    fn sample(backlog_bytes: u64, backlog_ratio: f64, prod_bps: u64) -> Sample {
        Sample {
            backlog_ratio,
            backlog_bytes,
            backlog_delta: 0,
            drain_bps: prod_bps,
            prod_bps,
            has_active_clients: true,
        }
    }

    fn inactive_sample(backlog_bytes: u64, backlog_ratio: f64, prod_bps: u64) -> Sample {
        Sample {
            has_active_clients: false,
            ..sample(backlog_bytes, backlog_ratio, prod_bps)
        }
    }

    fn direct_sample(backlog_bytes: u64, backlog_ratio: f64, drain_bps: u64) -> Sample {
        Sample {
            backlog_ratio,
            backlog_bytes,
            backlog_delta: 0,
            drain_bps,
            prod_bps: 1_000_000,
            has_active_clients: true,
        }
    }

    #[test]
    fn confirm_switch_uses_existing_pool_as_next_baseline() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        sm.prev_backlog_bytes = 10_000;
        sm.rate_ewma = 120_000.0;

        sm.confirm_switch(80_000);
        let decision = sm.update(
            &sample(80_000, 0.05, 600_000),
            Duration::from_secs(1),
            Instant::now(),
        );

        assert_eq!(decision, Decision::Hold);
        assert_eq!(sm.prev_backlog_bytes, 80_000);
        assert_eq!(sm.rate_ewma, 0.0);
    }

    #[test]
    fn rollback_restores_switch_side_effects_after_failed_downshift() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let original_last_change = sm.last_change_at;
        sm.stable_accum = 3.0;

        let decision = sm.update(
            &sample(1_000_000, 0.50, 600_000),
            Duration::from_secs(1),
            Instant::now(),
        );
        assert_eq!(decision, Decision::StepDown(2));
        assert_eq!(sm.current, 2);
        assert!(sm.down_cooldown_until.is_some());

        sm.rollback_switch();
        assert_eq!(sm.current, 1);
        assert_eq!(sm.regime, Regime::Encode);
        assert_eq!(sm.last_change_at, original_last_change);
        assert!(sm.down_cooldown_until.is_none());
        assert_eq!(sm.stable_accum, 3.0);
    }

    #[test]
    fn configured_up_hold_controls_probe_timing() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let now = Instant::now();

        let first = sm.update(&sample(0, 0.0, 600_000), Duration::from_secs(1), now);
        let second = sm.update(
            &sample(0, 0.0, 600_000),
            Duration::from_secs(1),
            now + Duration::from_secs(1),
        );

        assert_eq!(first, Decision::Hold);
        assert_eq!(second, Decision::StepUp(0));
    }

    #[test]
    fn configured_up_cooldown_delays_recovery_probe() {
        let mut cfg = cfg();
        cfg.up_hold_s = 1.0;
        cfg.up_cooldown_s = 5.0;
        let mut sm = StateMachine::new(cfg, 2, vec![1_000_000, 600_000, 300_000], 1);
        let now = Instant::now();
        sm.last_change_at = now;

        let during_cooldown = sm.update(
            &sample(0, 0.0, 600_000),
            Duration::from_secs(1),
            now + Duration::from_secs(1),
        );
        let after_cooldown = sm.update(
            &sample(0, 0.0, 600_000),
            Duration::from_secs(1),
            now + Duration::from_secs(5),
        );

        assert_eq!(during_cooldown, Decision::Hold);
        assert_eq!(after_cooldown, Decision::StepUp(0));
    }

    #[test]
    fn configured_down_ratio_controls_downshift_sensitivity() {
        let now = Instant::now();
        let mut sensitive = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let mut tolerant_cfg = cfg();
        tolerant_cfg.down_ratio = 0.70;
        let mut tolerant = StateMachine::new(tolerant_cfg, 2, vec![1_000_000, 600_000, 300_000], 1);

        let congested = sample(150_000, 0.05, 600_000);

        assert_eq!(
            sensitive.update(&congested, Duration::from_secs(1), now),
            Decision::StepDown(2)
        );
        assert_eq!(
            tolerant.update(&congested, Duration::from_secs(1), now),
            Decision::Hold
        );
    }

    #[test]
    fn direct_congestion_first_downshift_uses_link_estimate() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 0);
        let now = Instant::now();

        assert_eq!(
            sm.update(
                &sample(200_000, 0.05, 1_000_000),
                Duration::from_secs(1),
                now,
            ),
            Decision::Hold
        );
        let decision = sm.update(
            &sample(400_000, 0.08, 1_000_000),
            Duration::from_secs(1),
            now + Duration::from_secs(1),
        );

        assert_eq!(decision, Decision::StepDown(1));
        assert_eq!(sm.current, 1);
        assert_eq!(sm.regime, Regime::Encode);
    }

    #[test]
    fn direct_congestion_threshold_uses_measured_source_rate() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 120_000, 60_000], 0);
        sm.source_bps = 200_000.0;
        let now = Instant::now();

        assert_eq!(
            sm.update(
                &direct_sample(30_000, 0.02, 200_000),
                Duration::from_secs(1),
                now,
            ),
            Decision::Hold
        );
        let decision = sm.update(
            &direct_sample(60_000, 0.04, 200_000),
            Duration::from_secs(1),
            now + Duration::from_secs(1),
        );

        assert_eq!(decision, Decision::StepDown(1));
        assert!(sm.link_confirmed);
    }

    #[test]
    fn direct_congestion_updates_source_rate_from_drain_plus_growth() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 0);
        let now = Instant::now();

        assert_eq!(
            sm.update(
                &direct_sample(50_000, 0.05, 200_000),
                Duration::from_secs(1),
                now,
            ),
            Decision::Hold
        );
        assert_eq!(
            sm.update(
                &direct_sample(100_000, 0.08, 200_000),
                Duration::from_secs(1),
                now + Duration::from_secs(1),
            ),
            Decision::Hold
        );
        let decision = sm.update(
            &direct_sample(100_000, 0.08, 200_000),
            Duration::from_secs(1),
            now + Duration::from_secs(2),
        );

        assert_eq!(decision, Decision::StepDown(2));
        assert!(sm.source_bps < 700_000.0);
        assert!(sm.link_confirmed);
        assert!(sm.link_estimate < 500_000.0);
    }

    #[test]
    fn direct_emergency_full_pool_jumps_to_safe_low_ladder() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 0);
        let decision = sm.update(
            &sample(1_000_000, 0.90, 1_000_000),
            Duration::from_secs(1),
            Instant::now(),
        );

        assert_eq!(decision, Decision::StepDown(2));
        assert_eq!(sm.current, 2);
        assert_eq!(sm.regime, Regime::Encode);
    }

    #[test]
    fn inactive_clients_do_not_trigger_ladder_changes() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        sm.down_accum = 1.0;
        sm.stable_accum = 1.0;
        sm.link_confirm_accum = 1.0;

        let decision = sm.update(
            &inactive_sample(1_000_000, 0.90, 600_000),
            Duration::from_secs(1),
            Instant::now(),
        );

        assert_eq!(decision, Decision::Hold);
        assert_eq!(sm.current, 1);
        assert_eq!(sm.down_accum, 0.0);
        assert_eq!(sm.stable_accum, 0.0);
        assert_eq!(sm.link_confirm_accum, 0.0);
    }

    #[test]
    fn failed_probe_reverts_to_last_stable_and_locks_target() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let now = Instant::now();

        assert_eq!(
            sm.request_startup_probe(0, 0, now),
            Some(Decision::StepUp(0))
        );
        let decision = sm.update(
            &sample(700_000, 0.30, 1_000_000),
            Duration::from_secs(3),
            now + Duration::from_secs(3),
        );

        assert_eq!(decision, Decision::RevertDown(1));
        assert_eq!(sm.current, 1);
        assert_eq!(sm.regime, Regime::Encode);
        assert!(matches!(sm.locked_until.get(&0), Some(until) if *until > now));
        assert_eq!(sm.consecutive_up_fails, 1);
        assert!(sm.stable_protect_until.is_some());
    }

    #[test]
    fn direct_probe_uses_observed_source_rate_for_failure_threshold() {
        let mut sm = StateMachine::new(cfg(), 1, vec![1_000_000, 300_000], 1);
        let now = Instant::now();

        assert_eq!(
            sm.request_startup_probe(0, 0, now),
            Some(Decision::StepUp(0))
        );
        assert_eq!(
            sm.update(
                &direct_sample(50_000, 0.04, 200_000),
                Duration::from_secs(1),
                now + Duration::from_secs(1),
            ),
            Decision::Hold
        );
        let decision = sm.update(
            &direct_sample(100_000, 0.08, 200_000),
            Duration::from_secs(2),
            now + Duration::from_secs(3),
        );

        assert_eq!(decision, Decision::RevertDown(1));
        assert_eq!(sm.current, 1);
        assert!(sm.source_bps < 700_000.0);
    }

    #[test]
    fn failed_upgrade_switch_rolls_back_and_locks_target() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let now = Instant::now();

        assert_eq!(
            sm.request_startup_probe(0, 0, now),
            Some(Decision::StepUp(0))
        );
        sm.rollback_failed_switch(0, now + Duration::from_secs(1));

        assert_eq!(sm.current, 1);
        assert_eq!(sm.regime, Regime::Encode);
        assert!(sm.is_locked(0, now + Duration::from_secs(2)));
        assert_eq!(sm.consecutive_up_fails, 1);
        assert_eq!(
            sm.request_startup_probe(0, 0, now + Duration::from_secs(2)),
            None
        );
    }

    #[test]
    fn failed_downshift_switch_locks_target_without_counting_upgrade_failures() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let now = Instant::now();

        assert_eq!(
            sm.update(
                &sample(1_000_000, 0.90, 600_000),
                Duration::from_secs(1),
                now,
            ),
            Decision::StepDown(2)
        );
        sm.rollback_failed_switch(2, now + Duration::from_secs(1));

        assert_eq!(sm.current, 1);
        assert_eq!(sm.regime, Regime::Encode);
        assert!(sm.is_locked(2, now + Duration::from_secs(2)));
        assert_eq!(sm.consecutive_up_fails, 0);
        assert_eq!(
            sm.update(
                &sample(1_000_000, 0.90, 600_000),
                Duration::from_secs(1),
                now + Duration::from_secs(2),
            ),
            Decision::Hold
        );
    }

    #[test]
    fn repeated_probe_failures_disable_upgrades_temporarily() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let start = Instant::now();

        for i in 0..3 {
            let probe_at = start + Duration::from_secs(i * 700);
            assert_eq!(
                sm.request_startup_probe(0, 0, probe_at),
                Some(Decision::StepUp(0))
            );
            assert_eq!(
                sm.update(
                    &sample(700_000, 0.30, 1_000_000),
                    Duration::from_secs(3),
                    probe_at + Duration::from_secs(3),
                ),
                Decision::RevertDown(1)
            );
        }

        assert_eq!(sm.consecutive_up_fails, 3);
        assert!(matches!(sm.up_disabled_until, Some(until) if until > start));
        assert_eq!(
            sm.request_startup_probe(0, 0, start + Duration::from_secs(2_000)),
            None
        );
    }

    #[test]
    fn startup_probe_to_direct_enters_direct_after_observation() {
        let mut sm = StateMachine::new(cfg(), 2, vec![1_000_000, 600_000, 300_000], 1);
        let now = Instant::now();

        assert_eq!(
            sm.request_startup_probe(0, 4_000, now),
            Some(Decision::StepUp(0))
        );
        assert_eq!(sm.current, 0);
        assert_eq!(sm.regime, Regime::Probing);
        sm.prev_backlog_bytes = 4_000;

        let decision = sm.update(
            &sample(4_000, 0.01, 1_000_000),
            Duration::from_secs(13),
            now + Duration::from_secs(13),
        );

        assert_eq!(decision, Decision::Hold);
        assert_eq!(sm.regime, Regime::Direct);
        assert_eq!(sm.last_stable, 0);
    }
}
