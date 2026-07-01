//! ffmpeg 命令构造 (pipe HTTP-TS 模式):
//! - copy: 直通 remux (零 CPU)
//! - encode: 软编 libx264 (x86/Pi5)
//! - hwencode: 硬编 h264_v4l2m2m (Pi3B+/Pi4 等 V4L2 mem2mem 设备, CPU~0%)
//!
//! v4l2m2m 实测约束 (Pi3B+ ffmpeg 7.1.5):
//! - 不能接 yadif+scale 双滤镜链 (VIDIOC_STREAMON failed, "No such process")
//!   → 硬编档不加 -vf (源 1080p 隔行 H264 直接硬编为隔行 H264, 播放器可解)
//! - 不能配 aac 软编 (audio encoder 线程拖累 v4l2m2m 节奏 → STREAMON 失败)
//!   → 音频用 -c:a copy 直通

use crate::config::Run;
use std::process::Stdio;
use tokio::process::Command;

/// 构造 ffmpeg 命令, 输出 mpegts 到 stdout (pipe:1)。
/// 返回未 spawn 的 Command (调用者要 spawn 并接管 stdout)。
pub fn build_cmd(run: &Run, source: &str) -> Command {
    let mut cmd = Command::new("ffmpeg");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0); // 独立进程组, 方便整体 kill
    }
    cmd.arg("-nostdin")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        // -fflags +discardcorrupt+genpts: 丢弃损坏包 + 生成缺失 PTS
        // 注意: probesize/analyzeduration 仅用于 copy/encode 软档起播加速,
        // hwencode 档不能省略分析 (v4l2m2m 需充分分析输入流才能 STREAMON, 否则失败)
        .arg("-fflags")
        .arg("+discardcorrupt+genpts")
        .arg("-i")
        .arg(source)
        // 时间戳连续: -copyts 让输出 PTS 锚定输入源时钟 (转码档也继承源时间戳),
        // 这样切档瞬间新旧 ffmpeg 的 PTS 在同一条时间线上连续递增, 播放器不卡不快进。
        .arg("-copyts")
        .arg("-muxdelay").arg("0")
        .arg("-muxpreload").arg("0");

    match run.mode.as_str() {
        "copy" => {
            cmd.arg("-probesize").arg("32768")
                .arg("-analyzeduration").arg("0");
            cmd.arg("-c:v").arg("copy").arg("-c:a").arg("copy");
        }
        "encode" => {
            cmd.arg("-probesize").arg("32768")
                .arg("-analyzeduration").arg("0");
            let vf = format!("yadif=0:-1:0,scale={}:{}", run.width, run.height);
            cmd.arg("-vf")
                .arg(&vf)
                .arg("-c:v")
                .arg("libx264")
                .arg("-preset")
                .arg(&run.preset)
                .arg("-tune")
                .arg("zerolatency")
                .arg("-b:v")
                .arg(format!("{}k", run.bitrate))
                .arg("-maxrate")
                .arg(format!("{}k", run.maxrate))
                .arg("-bufsize")
                .arg(format!("{}k", run.bufsize))
                .arg("-g")
                .arg("25") // 1s IDR @25fps, 低延迟
                .arg("-pix_fmt")
                .arg("yuv420p")
                .arg("-c:a")
                .arg("aac")
                .arg("-b:a")
                .arg(format!("{}k", run.audio_bitrate))
                .arg("-ac")
                .arg("2")
                .arg("-ar")
                .arg("48000");
        }
        "hwencode" => {
            // h264_v4l2m2m 硬编 (Pi3B+/Pi4 V4L2 mem2mem, CPU~0%)。
            // 关键约束 (实测 ffmpeg 7.1.5 + Pi3B+):
            // - 1080p 宏块数 8160 踩 H264 Level4.0 的 8192 临界边界 → VIDIOC_STREAMON failed。
            //   加 -level 5 (Level5 上限 22080 宏块) 突破, 1080p 稳定, 不降分辨率。
            // - 不加 yadif 去隔行 (隔行源直接硬编为隔行 H264, 播放器可解; 加 yadif 反而时不稳)
            // - 音频 aac 软编会拖垮 v4l2m2m 节奏 → 音频 copy 直通
            cmd.arg("-c:v")
                .arg("h264_v4l2m2m")
                .arg("-level")
                .arg("5")
                .arg("-b:v")
                .arg(format!("{}k", run.bitrate))
                .arg("-maxrate")
                .arg(format!("{}k", run.maxrate))
                .arg("-g")
                .arg("25")
                .arg("-c:a")
                .arg("copy");
        }
        other => {
            // 不应发生; 用 copy 兜底
            tracing::warn!(target: "ffmpeg", "unknown mode {other}, fallback copy");
            cmd.arg("-c:v").arg("copy").arg("-c:a").arg("copy");
        }
    }

    cmd.arg("-f")
        .arg("mpegts")
        .arg("-mpegts_flags")
        .arg("resend_headers")
        .arg("-flush_packets")
        .arg("1")
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .kill_on_drop(true);

    cmd
}

/// 优雅 kill ffmpeg 进程组: SIGTERM → 等待 → SIGKILL
pub async fn kill_process_group(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        if let Some(pid) = child.id() {
            let pgid = Pid::from_raw(-(pid as i32));
            let _ = kill(pgid, Signal::SIGTERM);
            if tokio::time::timeout(
                std::time::Duration::from_millis(1500),
                child.wait(),
            )
            .await
            .is_err()
            {
                let _ = kill(pgid, Signal::SIGKILL);
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
    let _ = child.wait().await;
}