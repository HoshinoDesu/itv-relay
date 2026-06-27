//! 配置结构与 toml 解析。

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// 频道清单 m3u 文件路径 (代替单一 source)
    pub playlist_path: String,
    /// 对外 HTTP 监听地址
    pub listen: String,
    /// 母列表里播放地址的对外 base URL (空则自动用本机IP:端口)
    pub base_url: Option<String>,
    /// 台标 logo 源 base (jsdelivr 加速 fanmingming/live)
    #[serde(default = "default_logo_base")]
    pub logo_base: String,
    /// HLS 段输出目录 (pipe模式已不用, 保留兼容旧配置)
    #[serde(default)]
    pub hls_dir: Option<PathBuf>,
    /// 段时长 (pipe模式已不用)
    #[serde(default)]
    pub segment_duration: Option<u64>,
    /// 滚动保留段数 (pipe模式已不用)
    #[serde(default)]
    pub rolling_segments: Option<usize>,
    /// 码率档位, 索引 0=直通, 越大越低
    pub ladder: Vec<Run>,
    /// 起播档位索引 (默认1=转码, 用快速IDR快速起播; 直通档需等源GOP关键帧, 起播慢5s)
    #[serde(default = "default_startup_ladder")]
    pub startup_ladder: usize,
    /// 起播后多少秒切到默认档0直通 (网络好时)
    #[serde(default = "default_startup_hold_s")]
    pub startup_hold_s: f64,
    /// 拥塞探测阈值
    pub congestion: Congestion,
    /// 拥塞探测采样间隔(秒)
    #[serde(default = "default_sample_interval")]
    pub sample_interval_s: f64,
}

fn default_sample_interval() -> f64 {
    1.0
}
fn default_logo_base() -> String {
    "https://fastly.jsdelivr.net/gh/fanmingming/live@main/tv".into()
}
fn default_startup_ladder() -> usize {
    1 // 档1转码起播 (libx264 -g 25 强制1s IDR, 起播快)
}
fn default_startup_hold_s() -> f64 {
    8.0 // 起播后8s切回档0直通
}

#[derive(Debug, Clone, Deserialize)]
pub struct Run {
    /// 档位名(日志用)
    pub name: String,
    /// "copy" 直通 remux, 或 "encode" 转码
    pub mode: String,
    /// encode 模式: 宽
    #[serde(default)]
    pub width: u32,
    /// encode 模式: 高
    #[serde(default)]
    pub height: u32,
    /// encode 模式: 目标码率 kbps
    #[serde(default)]
    pub bitrate: u32,
    #[serde(default)]
    pub maxrate: u32,
    #[serde(default)]
    pub bufsize: u32,
    #[serde(default = "default_preset")]
    pub preset: String,
    /// 音频码率 kbps (转码档)
    #[serde(default = "default_audio_bitrate")]
    pub audio_bitrate: u32,
}

fn default_preset() -> String {
    "veryfast".into()
}
fn default_audio_bitrate() -> u32 {
    128
}

#[derive(Debug, Clone, Deserialize)]
pub struct Congestion {
    /// 发送速率/产出速率 低于此比例触发降档
    pub down_ratio: f64,
    /// 低比例持续多久才降档(秒)
    pub down_hold_s: f64,
    /// 发送速率/产出速率 高于此比例触发升档
    pub up_ratio: f64,
    /// 高比例持续多久才升档(秒)
    pub up_hold_s: f64,
    /// 降档后冷却期(秒), 防雪崩
    pub down_cooldown_s: f64,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| format!("read config {path}"))?;
        toml::from_str(&raw).context("parse config").map(|c: Config| c)
    }
}
