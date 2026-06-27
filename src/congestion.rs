//! 拥塞探测 (v5): pool 字节量斜率作主信号。
//!
//! 诚实性: backlog_bytes 的斜率 d/dt = (ffmpeg 产出) - (reader 取走)。
//! reader 取走 = axum→hyper 写 socket, 受内核 TCP 发送缓冲限制 (链路慢时 write 阻塞 → reader 不取 → pool 涨)。
//! 所以 pool 斜率稳态下诚实反映 prod vs 真实链路, 不被 TCP 缓冲/客户端缓冲永久掩盖
//! (内核缓冲有限, 吸收期过后 pool 必涨)。
//! drain_bps (reader pop 字节率) 被 TCP 缓冲虚高, 仅保留诊断。

#[derive(Debug, Clone, Default)]
pub struct Sample {
    /// backlog chunk 比例 (仅紧急阈值用, 不做趋势判据)
    pub backlog_ratio: f64,
    /// pool 字节量 (state 内做斜率 EWMA)
    pub backlog_bytes: u64,
    /// 本拍 pool 字节增量 (raw, state 内部 EWMA)。负值=pool 在缩。
    pub backlog_delta: i64,
    /// 诊断: reader pop 字节率
    pub drain_bps: u64,
    /// 诊断: 当前档应产字节率
    pub prod_bps: u64,
    pub has_active_clients: bool,
}
