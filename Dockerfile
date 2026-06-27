# 多阶段构建: builder 编译 itv-relay, runtime 只含二进制 + ffmpeg
# 多架构 (amd64/arm64) 由 buildx + QEMU 处理, 各架构原生编译

# ---- builder ----
FROM rust:bookworm AS builder

WORKDIR /build

# 先拷依赖清单利用层缓存
COPY Cargo.toml Cargo.lock ./
# 创建空 src 供 cargo 预编译依赖
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && cargo build --release || true

# 拷真实源码编译
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim

# 装 ffmpeg (含 libx264 软编) + ca-certificates (拉 logo 用 https) + tini (信号转发/init)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates tini && \
    rm -rf /var/lib/apt/lists/*

# 拷二进制
COPY --from=builder /build/target/release/itv-relay /usr/local/bin/itv-relay

# 工作目录: 用户挂载 config.toml + rtp2html.m3u 到这里
WORKDIR /data
VOLUME /data

EXPOSE 8088

# tini 作 init, 正确转发信号让 ffmpeg 子进程能被清理
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["itv-relay", "/data/config.toml"]
