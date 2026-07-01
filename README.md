# itv-relay

一个 IPTV 中继服务。它读取 m3u 频道列表，对外提供新的播放列表和播放地址。

网络好时直接转发原始源；网络变差时自动切到低码率；网络恢复后再自动升回高码率。切换时会尽量保持同一个 HTTP 播放连接不断开，减少播放器卡顿。

## 功能

- 支持 m3u 频道列表。
- 输出 `/playlist.m3u` 和 `/play/tv-N`。
- 支持直通和多档转码。
- 根据发送情况自动降码率、升码率。
- 起播档可配置，起播失败会尝试其他档位。
- 上游断流时会在同一个播放连接里尝试恢复。

## 依赖

- Rust
- ffmpeg，需支持 `libx264`

## 配置

示例：

```toml
playlist_path = "/opt/itv-relay/playlist.m3u"
listen = "0.0.0.0:8088"
base_url = "http://<服务器IP>:8088"

startup_ladder = 1
startup_hold_s = 5.0
switch_timeout_s = 8.0
switch_preroll_bytes = 262144
sample_interval_s = 1.0

[[ladder]]
name = "copy-8m"
mode = "copy"
bitrate = 8000

[[ladder]]
name = "encode-3m"
mode = "encode"
width = 1920
height = 1080
bitrate = 3000
maxrate = 3500
bufsize = 6500
preset = "ultrafast"
audio_bitrate = 128

[[ladder]]
name = "encode-1m"
mode = "encode"
width = 1280
height = 720
bitrate = 1000
maxrate = 1200
bufsize = 2500
preset = "ultrafast"
audio_bitrate = 128

[congestion]
down_ratio = 0.90
down_hold_s = 2.0
up_ratio = 0.97
up_hold_s = 15.0
up_cooldown_s = 20.0
down_cooldown_s = 8.0
```

说明：

- `playlist_path` 是源 m3u 文件。
- `listen` 是监听地址。
- `base_url` 建议手动设置成播放器能访问的地址。
- `startup_ladder` 是起播档位，`0` 是直通，数字越大码率越低。
- `ladder` 必须按码率从高到低排列。

## 频道列表

普通 m3u 即可：

```m3u
#EXTM3U
#EXTINF:-1 tvg-name="CCTV1",CCTV1
http://example.com/live/cctv1.ts
```

## 运行

开发环境：

```bash
cargo run -- config.toml
```

生产环境：

```bash
cargo build --release
./target/release/itv-relay config.toml
```

打开：

```text
http://<服务器IP>:8088/playlist.m3u
http://<服务器IP>:8088/play/tv-1
```

## Docker

```bash
docker run -d \
  --name itv-relay \
  -p 8088:8088 \
  -v /path/to/relay:/data \
  -e RUST_LOG=itv_relay=info,state=info,ffmpeg=warn \
  ghcr.io/hoshinodesu/itv-relay:latest
```

容器内配置可写成：

```toml
playlist_path = "/data/playlist.m3u"
listen = "0.0.0.0:8088"
base_url = "http://<宿主机IP>:8088"
```

## 测试

运行基础检查：

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt -- --check
```

如果要模拟弱网，在中继机器上运行：

```bash
PLAYER_URL=http://<服务器IP>:8088/play/tv-1 \
VISUAL_OK=1 \
VISUAL_NOTE='无明显卡顿' \
bash tc_limit.sh expect-recover 2000 90 120
```

播放端请用另一台机器打开 `PLAYER_URL`，不要在中继机器本机用 `127.0.0.1` 测限速。

## 常用日志

```bash
RUST_LOG=itv_relay=info,state=info,session=info,ffmpeg=warn ./itv-relay config.toml
```

关注日志里的：

- `降档`
- `升档`
- `无缝切档完成`
- `播放中断恢复`

## 文件

- `src/session.rs`：播放会话、切档和恢复。
- `src/state.rs`：自动升降档判断。
- `src/ffmpeg.rs`：ffmpeg 命令。
- `src/server.rs`：HTTP 路由。
- `tc_limit.sh`：限速测试。

## 许可

GPL v3
