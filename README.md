# itv-relay

Rust 编写的 ITV 流中继服务，把局域网 ITV 源（RTSP-over-HTTP 代理）转成 pipe HTTP-TS 流对外服务。

默认直通源流零 CPU 占用；外网带宽不足卡顿时自动多档降码率；网络恢复后逐级试探升回。播放器是机顶盒或单路 URL，无 ABR、无客户端回传——拥塞判断完全由中继侧自测完成。

## 工作原理

```
GET /playlist.m3u  -> 频道播放列表 (绝对地址 + 台标 logo)
GET /play/tv-N     -> 一个 HTTP-TS 流会话:
  Session:
    1. spawn ffmpeg (当前档: 0=copy 直通 / 1..N=libx264 转码)
       ffmpeg -fflags +discardcorrupt+genpts -probesize 32768 -analyzeduration 0
              -i SOURCE -copyts -muxdelay 0 -muxpreload 0
              [ -vf yadif,scale -c:v libx264 -preset ultrafast -b:v Xk -g 25 | -c copy ]
              -c:a aac -b:a 128k -f mpegts -mpegts_flags resend_headers -flush_packets 1 pipe:1
    2. 读 ffmpeg stdout -> StreamBuf (可清空的有界缓冲池) -> HTTP 响应
    3. 拥塞探测: 缓冲池字节量的斜率 d(backlog_bytes)/dt 是诚实信号
       (pool 涨 = 产出 > 消费 = 真拥塞, 不被 TCP/客户端缓冲永久掩盖)
    4. 链路不足 -> 降档 (kill 旧 ffmpeg + spawn 新码率档, 热切换)
    5. 客户端断开 -> 会话终止 + kill ffmpeg (pipe 模式天然防挂死)
```

## 检测机制

采用 pool 字节斜率驱动的三态状态机，避免常见的左右互搏（升上去又被打回反复循环）：

- **Direct 态（直通）**：持续测量链路带宽 estimate = source_bps - 池斜率。拥塞时直接降落到合适码率档，不逐级爬梯。
- **Encode 态（编码档）**：降档只单步（不盲跳到最低档）；升档采用单步试探 + 观察期。
- **Probing 态（升档观察期）**：升档后进入观察期，若池子重新积压则回退到上一个稳定档（非最低档）并锁定该档指数退避。

防震荡：连续 3 次升档失败则暂停升档 30 分钟，避免无谓试探造成周期性卡顿。

为什么用池斜率而不是常见的 drain 速率或 backlog 水位：
- drain（reader 取走速率）会被内核 TCP 发送缓冲吸收突发而虚高，限速 2Mbps 实测却测出更高值。
- backlog 绝对水位会被客户端播放器缓冲吸收而长期保持低位（伪健康）。
- pool 字节斜率 = 产出 - 真实消费，是产大于消这一事实的第一手信号，稳态下诚实反映链路状态。

## 码率档位

纯码率档，不降分辨率（减少 CPU 负担）。默认 5 档：

| 档 | 模式 | 码率 | 说明 |
|---|---|---|---|
| 0 | copy 直通 | 源 ~8M | 网络好时零 CPU |
| 1 | encode | 5M | 1080p |
| 2 | encode | 3M | 1080p |
| 3 | encode | 2M | 1080p |
| 4 | encode | 1M | 深度拥塞兜底 |

切换时间戳通过 `-copyts` 锚定源时钟，跨档切换 PTS 连续，减少播放器跳变。

## 配置

```toml
playlist_path = "/root/relay/rtp2html.m3u"
listen = "0.0.0.0:8088"
sample_interval_s = 1.0
startup_ladder = 0        # 起播档 (0=直通)
startup_hold_s = 8.0

[[ladder]]
name = "0-passthrough-8m"
mode = "copy"

[[ladder]]
name = "1-1080p-5000k"
mode = "encode"
width = 1920
height = 1080
bitrate = 5000
maxrate = 5800
bufsize = 11000
preset = "ultrafast"
audio_bitrate = 128

# ...档 2/3/4 同理

[congestion]
down_ratio = 0.70
down_hold_s = 4.0
up_ratio = 0.95
up_hold_s = 15.0
down_cooldown_s = 8.0
```

`base_url` 可选，不填则自动用 `hostname -I` 探测本机 IP + listen 端口。`logo_base` 默认 jsdelivr 加速 fanmingming/live 台标源。

## 频道清单格式

m3u 文件，每条：
```
#EXTINF:-1 tvg-name="频道名" tvg-logo=".../频道.png",频道名
http://上游源URL
```
频道名去后缀（-高清/-HD/-4K 等）+ 去空格后映射为 logo 文件名。

## 编译

依赖 ffmpeg（软编 libx264）。可本机直接编译，也可交叉编译到 aarch64 等目标架构：

```bash
# 交叉编译到 aarch64 (示例)
sudo apt install gcc-aarch64-linux-gnu
rustup target add aarch64-unknown-linux-gnu

# .cargo/config.toml
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"

cargo build --target aarch64-unknown-linux-gnu --release
# 产物: target/aarch64-unknown-linux-gnu/release/itv-relay
```

## 部署

把二进制、config.toml、频道清单 m3u 放到目标机器同一目录，例如 `/root/relay/`：

```
/root/relay/
  itv-relay
  config.toml
  rtp2html.m3u
  run.sh
  tc_limit.sh
```

run.sh：
```bash
#!/bin/bash
cd /root/relay
export RUST_LOG=itv_relay=info,state=info,ffmpeg=warn
exec ./itv-relay config.toml
```

后台启动：
```bash
setsid -f /root/relay/run.sh </dev/null >/root/relay/relay.log 2>&1
```

停止：
```bash
pkill -9 -x itv-relay; pkill -9 ffmpeg
```

注意：`pkill -f itv-relay` 会匹配到含该字串的 SSH 命令行自身导致 exit 255，用 `pkill -x itv-relay` 精确匹配进程名。

## 限速测试

中继侧拥塞判断靠缓冲池斜率，不限速时客户端拉得快、池不涨不会触发降档。模拟限流需用 tc 限出站流量：

```bash
bash tc_limit.sh limit 2000    # 限 8088 出站到 2Mbps (触发降档)
bash tc_limit.sh limit 1000    # 1Mbps (更深降档)
bash tc_limit.sh status        # 看限速状态 + 降档日志
bash tc_limit.sh watch         # 实时看降档日志 (Ctrl+C 退)
bash tc_limit.sh clear         # 清限速
```

tc 配置要点：htb 限 8088 出站（`sport 8088`，不是 dport），SSH 等其他流量不受影响。本机拉 127.0.0.1 走 lo 限不到，测限速必须从另一台机器拉。

## 文件职责

| 文件 | 职责 |
|---|---|
| src/main.rs | 入口：加载配置 + 频道列表，起 axum 服务，自动探测本机 IP 作 base_url |
| src/config.rs | toml 解析：码率阶梯 + 拥塞参数 |
| src/playlist.rs | m3u 解析 + 母列表渲染（绝对地址 + 台标映射）|
| src/ffmpeg.rs | 构造 ffmpeg 命令（copy/libx264）+ 进程组 kill |
| src/streambuf.rs | 可清空的流式缓冲池，维护 backlog_bytes 计数 |
| src/session.rs | 核心：单播放会话，ffmpeg pipe 转发 + 拥塞采样 + 切档热切换 |
| src/state.rs | 三态状态机：Direct/Encode/Probing |
| src/congestion.rs | Sample 结构 |
| src/server.rs | axum 路由：/playlist.m3u + /play/:slug |
| tc_limit.sh | tc 限速测试工具 |

## 已知约束

- encoder 码率不能运行时热改（FFmpeg 官方确认），换码率必须换 ffmpeg 实例。
- 直通档起播需等源 GOP 关键帧，源端决定，约 2-5s 起播延迟，无法消除。
- 编码档使用 `ultrafast` preset，画质偏粗，更高质量 preset 会显著增加软编 CPU 开销。

## 许可

MIT
