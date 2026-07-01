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
#[allow(dead_code)]
pub fn build_cmd(run: &Run, source: &str) -> Command {
    build_cmd_inner(run, source, None)
}

pub fn build_cmd_with_video_pid(run: &Run, source: &str, video_pid: u16) -> Command {
    build_cmd_inner(run, source, Some(video_pid))
}

fn build_cmd_inner(run: &Run, source: &str, video_pid: Option<u16>) -> Command {
    let mut cmd = Command::new("ffmpeg");
    #[cfg(unix)]
    {
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
        .arg("+discardcorrupt+genpts");

    if matches!(run.mode.as_str(), "copy" | "encode") {
        cmd.arg("-probesize")
            .arg("32768")
            .arg("-analyzeduration")
            .arg("0");
    }

    cmd.arg("-i")
        .arg(source)
        // 时间戳连续: -copyts 让输出 PTS 锚定输入源时钟 (转码档也继承源时间戳),
        // 这样切档瞬间新旧 ffmpeg 的 PTS 在同一条时间线上连续递增, 播放器不卡不快进。
        .arg("-copyts")
        .arg("-muxdelay")
        .arg("0")
        .arg("-muxpreload")
        .arg("0");

    match run.mode.as_str() {
        "copy" => {
            cmd.arg("-c:v").arg("copy").arg("-c:a").arg("copy");
        }
        "encode" => {
            let vf = format!("yadif=0:-1:0,scale={}:{}", run.width, run.height);
            let maxrate = run.maxrate.max(run.bitrate);
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
                .arg(format!("{maxrate}k"))
                .arg("-bufsize")
                .arg(format!("{}k", run.bufsize))
                .arg("-g")
                .arg("25") // 1s IDR @25fps, 低延迟
                .arg("-keyint_min")
                .arg("25")
                .arg("-sc_threshold")
                .arg("0")
                .arg("-x264-params")
                .arg("repeat-headers=1")
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
            let maxrate = run.maxrate.max(run.bitrate);
            cmd.arg("-c:v")
                .arg("h264_v4l2m2m")
                .arg("-level")
                .arg("5")
                .arg("-b:v")
                .arg(format!("{}k", run.bitrate))
                .arg("-maxrate")
                .arg(format!("{maxrate}k"))
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

    let video_pid = video_pid.unwrap_or_else(|| video_pid_for_run(run));
    cmd.arg("-streamid").arg(format!("0:{video_pid}"));

    cmd.arg("-f")
        .arg("mpegts")
        .arg("-mpegts_copyts")
        .arg("1")
        .arg("-mpegts_flags")
        .arg("resend_headers+initial_discontinuity+pat_pmt_at_frames")
        .arg("-flush_packets")
        .arg("1")
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .stdin(Stdio::null())
        .kill_on_drop(true);

    cmd
}

fn video_pid_for_run(run: &Run) -> u16 {
    let mut hash = 0u16;
    for byte in run.name.bytes().chain(run.mode.bytes()) {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u16);
    }
    0x0200 + (hash % 0x0600) * 2
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
            if tokio::time::timeout(std::time::Duration::from_millis(1500), child.wait())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn run(mode: &str) -> Run {
        Run {
            name: format!("{mode}-test"),
            mode: mode.into(),
            width: 1920,
            height: 1080,
            bitrate: 5_000,
            maxrate: 5_800,
            bufsize: 11_000,
            preset: "ultrafast".into(),
            audio_bitrate: 128,
        }
    }

    fn args_for(mode: &str) -> Vec<String> {
        build_cmd(&run(mode), "udp://example")
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn arg_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        let pos = args.iter().position(|arg| arg == flag)?;
        args.get(pos + 1).map(String::as_str)
    }

    #[test]
    fn fast_probe_options_are_before_input_for_copy_and_encode() {
        for mode in ["copy", "encode"] {
            let args = args_for(mode);
            let input_pos = args.iter().position(|arg| arg == "-i").unwrap();
            let probesize_pos = args.iter().position(|arg| arg == "-probesize").unwrap();
            let analyze_pos = args
                .iter()
                .position(|arg| arg == "-analyzeduration")
                .unwrap();

            assert!(probesize_pos < input_pos);
            assert!(analyze_pos < input_pos);
        }
    }

    #[test]
    fn hwencode_keeps_default_input_analysis() {
        let args = args_for("hwencode");

        assert!(!args.iter().any(|arg| arg == "-probesize"));
        assert!(!args.iter().any(|arg| arg == "-analyzeduration"));
    }

    #[test]
    fn mpegts_output_marks_initial_discontinuity() {
        let args = args_for("encode");
        let flags_pos = args.iter().position(|arg| arg == "-mpegts_flags").unwrap();

        assert_eq!(
            args.get(flags_pos + 1).map(String::as_str),
            Some("resend_headers+initial_discontinuity+pat_pmt_at_frames")
        );
    }

    #[test]
    fn mpegts_muxer_preserves_input_timestamps() {
        let args = args_for("encode");
        let output_pos = args.iter().position(|arg| arg == "pipe:1").unwrap();
        let copyts_pos = args.iter().position(|arg| arg == "-mpegts_copyts").unwrap();

        assert!(copyts_pos < output_pos);
        assert_eq!(args.get(copyts_pos + 1).map(String::as_str), Some("1"));
    }

    #[test]
    fn libx264_encode_repeats_headers_on_fixed_gop() {
        let args = args_for("encode");
        let gop_pos = args.iter().position(|arg| arg == "-g").unwrap();
        let keyint_pos = args.iter().position(|arg| arg == "-keyint_min").unwrap();
        let scene_cut_pos = args.iter().position(|arg| arg == "-sc_threshold").unwrap();
        let x264_params_pos = args.iter().position(|arg| arg == "-x264-params").unwrap();

        assert_eq!(args.get(gop_pos + 1).map(String::as_str), Some("25"));
        assert_eq!(args.get(keyint_pos + 1).map(String::as_str), Some("25"));
        assert_eq!(args.get(scene_cut_pos + 1).map(String::as_str), Some("0"));
        assert_eq!(
            args.get(x264_params_pos + 1).map(String::as_str),
            Some("repeat-headers=1")
        );
    }

    #[test]
    fn encode_maxrate_falls_back_to_bitrate_when_missing() {
        let mut run = run("encode");
        run.maxrate = 0;
        let args: Vec<String> = build_cmd(&run, "udp://example")
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert_eq!(arg_after(&args, "-maxrate"), Some("5000k"));
    }

    #[test]
    fn mpegts_video_pid_is_stable_per_ladder_name() {
        let mut copy = run("copy");
        copy.name = "copy-main".into();
        let mut encode = run("encode");
        encode.name = "encode-low".into();

        let copy_pid = video_pid_for_run(&copy);
        let encode_pid = video_pid_for_run(&encode);

        assert_eq!(video_pid_for_run(&copy), copy_pid);
        assert_ne!(copy_pid, encode_pid);

        let args = build_cmd(&copy, "udp://example")
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let expected = format!("0:{copy_pid}");
        assert_eq!(arg_after(&args, "-streamid"), Some(expected.as_str()));
    }

    #[test]
    fn explicit_video_pid_overrides_ladder_hash() {
        let args = build_cmd_with_video_pid(&run("encode"), "udp://example", 777)
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(arg_after(&args, "-streamid"), Some("0:777"));
    }
}
