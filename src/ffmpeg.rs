//! ffmpeg 命令构造 (pipe HTTP-TS 模式): 直通 copy 或 转码 libx264 四档。

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
        // 关键冷启优化: probesize 32k + analyzeduration 0 让 ffmpeg 不等大量数据就开始输出
        // 注意: -fflags +nobuffer 会拖慢首字节(实测 0.4s→2.3s), 不用! 只保留 discardcorrupt+genpts
        .arg("-fflags")
        .arg("+discardcorrupt+genpts")
        .arg("-probesize")
        .arg("32768")
        .arg("-analyzeduration")
        .arg("0")
        .arg("-i")
        .arg(source)
        // 时间戳连续: -copyts 让输出 PTS 锚定输入源时钟 (转码档也继承源时间戳),
        // 这样切档瞬间新旧 ffmpeg 的 PTS 在同一条时间线上连续递增, 播放器不卡不快进。
        .arg("-copyts")
        .arg("-muxdelay").arg("0")
        .arg("-muxpreload").arg("0");

    match run.mode.as_str() {
        "copy" => {
            cmd.arg("-c:v").arg("copy").arg("-c:a").arg("copy");
        }
        "encode" => {
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
        .stderr(Stdio::piped())
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