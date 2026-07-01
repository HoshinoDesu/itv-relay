//! 配置结构与 toml 解析。

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_COPY_BITRATE_KBPS: u32 = 8_000;

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
    /// EPG 节目单 XML 地址 (写入 #EXTM3U x-tvg-url)
    #[serde(default)]
    pub epg_url: Option<String>,
    /// 码率档位, 索引 0=直通, 越大越低
    pub ladder: Vec<Run>,
    /// 起播档位索引 (默认1=转码, 用快速IDR快速起播; 直通档需等源GOP关键帧, 起播慢5s)
    #[serde(default = "default_startup_ladder")]
    pub startup_ladder: usize,
    /// 起播后多少秒切到默认档0直通 (网络好时)
    #[serde(default = "default_startup_hold_s")]
    pub startup_hold_s: f64,
    /// 切档后台 ffmpeg 等首帧超时(秒)
    #[serde(default = "default_switch_timeout_s")]
    pub switch_timeout_s: f64,
    /// 切档完成前预读的新档数据量(字节), 用于降低替换瞬间断续风险
    #[serde(default = "default_switch_preroll_bytes")]
    pub switch_preroll_bytes: usize,
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
    1
}
fn default_startup_hold_s() -> f64 {
    5.0 // 起播后5s切回档0直通
}
fn default_switch_timeout_s() -> f64 {
    8.0
}
fn default_switch_preroll_bytes() -> usize {
    256 * 1024
}
fn default_up_cooldown_s() -> f64 {
    20.0
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

impl Run {
    pub fn output_bps(&self) -> u64 {
        let kbps = if self.mode == "copy" {
            self.bitrate.max(DEFAULT_COPY_BITRATE_KBPS)
        } else {
            self.maxrate.max(self.bitrate) + self.audio_bitrate
        };
        (kbps as u64) * 1000 / 8
    }
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
    /// 切档后最短等待多久才允许再次升档(秒)
    #[serde(default = "default_up_cooldown_s")]
    pub up_cooldown_s: f64,
    /// 降档后冷却期(秒), 防雪崩
    pub down_cooldown_s: f64,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| format!("read config {path}"))?;
        let mut c: Config = toml::from_str(&raw).context("parse config")?;
        c.validate()?;
        c.normalize();
        Ok(c)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.ladder.is_empty(), "ladder must not be empty");
        anyhow::ensure!(
            self.sample_interval_s > 0.0,
            "sample_interval_s must be positive"
        );
        anyhow::ensure!(
            self.startup_hold_s >= 0.0,
            "startup_hold_s must be non-negative"
        );
        anyhow::ensure!(
            self.switch_timeout_s > 0.0,
            "switch_timeout_s must be positive"
        );
        anyhow::ensure!(
            self.switch_preroll_bytes > 0,
            "switch_preroll_bytes must be positive"
        );
        anyhow::ensure!(
            (0.0..1.0).contains(&self.congestion.down_ratio),
            "congestion.down_ratio must be in 0..1"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.congestion.up_ratio),
            "congestion.up_ratio must be in 0..=1"
        );
        anyhow::ensure!(
            self.congestion.down_hold_s > 0.0,
            "congestion.down_hold_s must be positive"
        );
        anyhow::ensure!(
            self.congestion.up_hold_s > 0.0,
            "congestion.up_hold_s must be positive"
        );
        anyhow::ensure!(
            self.congestion.up_cooldown_s >= 0.0,
            "congestion.up_cooldown_s must be non-negative"
        );
        anyhow::ensure!(
            self.congestion.down_cooldown_s >= 0.0,
            "congestion.down_cooldown_s must be non-negative"
        );

        let mut prev_bps = u64::MAX;
        for (i, run) in self.ladder.iter().enumerate() {
            anyhow::ensure!(
                matches!(run.mode.as_str(), "copy" | "encode" | "hwencode"),
                "ladder[{i}] mode must be copy, encode, or hwencode"
            );
            if run.mode != "copy" {
                anyhow::ensure!(
                    run.bitrate > 0,
                    "ladder[{i}] bitrate must be positive for encode modes"
                );
                anyhow::ensure!(
                    run.maxrate == 0 || run.maxrate >= run.bitrate,
                    "ladder[{i}] maxrate must be >= bitrate when set"
                );
                if run.mode == "encode" {
                    anyhow::ensure!(
                        run.width > 0 && run.height > 0,
                        "ladder[{i}] width and height must be positive for encode mode"
                    );
                    anyhow::ensure!(
                        run.bufsize > 0,
                        "ladder[{i}] bufsize must be positive for encode mode"
                    );
                    anyhow::ensure!(
                        run.audio_bitrate > 0,
                        "ladder[{i}] audio_bitrate must be positive for encode mode"
                    );
                    anyhow::ensure!(
                        !run.preset.trim().is_empty(),
                        "ladder[{i}] preset must not be empty for encode mode"
                    );
                }
            }

            let bps = run.output_bps();
            anyhow::ensure!(
                bps <= prev_bps,
                "ladder output bitrate must not increase with index"
            );
            prev_bps = bps;
        }

        Ok(())
    }

    fn normalize(&mut self) {
        if !self.ladder.is_empty() && self.startup_ladder >= self.ladder.len() {
            let clamped = self.ladder.len() - 1;
            tracing::warn!(
                target: "config",
                "startup_ladder {} out of range, using {}",
                self.startup_ladder,
                clamped
            );
            self.startup_ladder = clamped;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(mode: &str, bitrate: u32, maxrate: u32, audio_bitrate: u32) -> Run {
        Run {
            name: "test".into(),
            mode: mode.into(),
            width: 0,
            height: 0,
            bitrate,
            maxrate,
            bufsize: 0,
            preset: default_preset(),
            audio_bitrate,
        }
    }

    #[test]
    fn copy_output_bps_defaults_to_8mbps() {
        assert_eq!(run("copy", 0, 0, 128).output_bps(), 1_000_000);
    }

    #[test]
    fn encode_output_bps_uses_maxrate_plus_audio() {
        assert_eq!(run("encode", 5_000, 5_800, 128).output_bps(), 741_000);
    }

    #[test]
    fn encode_output_bps_falls_back_to_bitrate_when_maxrate_missing() {
        assert_eq!(run("encode", 5_000, 0, 128).output_bps(), 641_000);
    }

    fn valid_config() -> Config {
        toml::from_str(
            r#"
playlist_path = "playlist.m3u"
listen = "0.0.0.0:8088"
startup_ladder = 1

[[ladder]]
name = "copy"
mode = "copy"
bitrate = 8000

[[ladder]]
name = "5m"
mode = "encode"
width = 1920
height = 1080
bitrate = 5000
maxrate = 5800
bufsize = 11000
audio_bitrate = 128

[[ladder]]
name = "3m"
mode = "encode"
width = 1920
height = 1080
bitrate = 3000
maxrate = 3500
bufsize = 6500
audio_bitrate = 128

[congestion]
down_ratio = 0.90
down_hold_s = 2.0
up_ratio = 0.97
up_hold_s = 15.0
down_cooldown_s = 8.0
"#,
        )
        .unwrap()
    }

    #[test]
    fn validate_accepts_descending_ladder() {
        valid_config().validate().unwrap();
    }

    #[test]
    fn validate_rejects_ladder_with_increasing_bitrate() {
        let mut cfg = valid_config();
        cfg.ladder.swap(1, 2);

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("must not increase"));
    }

    #[test]
    fn validate_rejects_unknown_ladder_mode() {
        let mut cfg = valid_config();
        cfg.ladder[1].mode = "mystery".into();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("mode must be"));
    }

    #[test]
    fn validate_rejects_encode_ladder_without_dimensions() {
        let mut cfg = valid_config();
        cfg.ladder[1].width = 0;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("width and height"));
    }

    #[test]
    fn validate_rejects_encode_ladder_without_bufsize() {
        let mut cfg = valid_config();
        cfg.ladder[1].bufsize = 0;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("bufsize"));
    }

    #[test]
    fn validate_rejects_encode_ladder_without_audio_bitrate() {
        let mut cfg = valid_config();
        cfg.ladder[1].audio_bitrate = 0;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("audio_bitrate"));
    }

    #[test]
    fn validate_rejects_encode_ladder_without_preset() {
        let mut cfg = valid_config();
        cfg.ladder[1].preset = " ".into();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("preset"));
    }

    #[test]
    fn normalize_clamps_out_of_range_startup_ladder() {
        let mut cfg = valid_config();
        cfg.startup_ladder = cfg.ladder.len();

        cfg.normalize();
        cfg.validate().unwrap();
        assert_eq!(cfg.startup_ladder, cfg.ladder.len() - 1);
    }

    #[test]
    fn single_ladder_default_startup_can_be_normalized() {
        let mut cfg: Config = toml::from_str(
            r#"
playlist_path = "playlist.m3u"
listen = "0.0.0.0:8088"

[[ladder]]
name = "copy"
mode = "copy"

[congestion]
down_ratio = 0.90
down_hold_s = 2.0
up_ratio = 0.97
up_hold_s = 15.0
down_cooldown_s = 8.0
"#,
        )
        .unwrap();

        assert_eq!(cfg.startup_ladder, 1);
        cfg.normalize();
        cfg.validate().unwrap();
        assert_eq!(cfg.startup_ladder, 0);
    }

    #[test]
    fn switch_timeout_defaults_to_8_seconds() {
        let cfg: Config = toml::from_str(
            r#"
playlist_path = "playlist.m3u"
listen = "0.0.0.0:8088"

[[ladder]]
name = "copy"
mode = "copy"

[congestion]
down_ratio = 0.90
down_hold_s = 2.0
up_ratio = 0.97
up_hold_s = 15.0
down_cooldown_s = 8.0
"#,
        )
        .unwrap();

        assert_eq!(cfg.switch_timeout_s, 8.0);
    }

    #[test]
    fn startup_hold_defaults_to_5_seconds() {
        let cfg = valid_config();

        assert_eq!(cfg.startup_hold_s, 5.0);
    }

    #[test]
    fn switch_preroll_defaults_to_256kib() {
        let cfg: Config = toml::from_str(
            r#"
playlist_path = "playlist.m3u"
listen = "0.0.0.0:8088"

[[ladder]]
name = "copy"
mode = "copy"

[congestion]
down_ratio = 0.90
down_hold_s = 2.0
up_ratio = 0.97
up_hold_s = 15.0
down_cooldown_s = 8.0
"#,
        )
        .unwrap();

        assert_eq!(cfg.switch_preroll_bytes, 256 * 1024);
    }

    #[test]
    fn up_cooldown_defaults_to_20_seconds() {
        let cfg = valid_config();

        assert_eq!(cfg.congestion.up_cooldown_s, 20.0);
    }
}
