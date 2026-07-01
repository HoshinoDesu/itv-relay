//! ITV 动态码率中继 (pipe HTTP-TS 版) 主入口。

mod config;
mod congestion;
mod ffmpeg;
mod playlist;
mod server;
mod session;
mod state;
mod streambuf;

use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "itv_relay=info,ffmpeg=warn,warn".into()),
        )
        .init();

    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let cfg = Config::load(&cfg_path)?;
    info!(
        "config loaded: {} 档位, ladder={:?}",
        cfg.ladder.len(),
        cfg.ladder
            .iter()
            .map(|r| r.name.clone())
            .collect::<Vec<_>>()
    );

    let channels = playlist::load(std::path::Path::new(&cfg.playlist_path))?;
    info!("频道数: {}", channels.len());

    let base_url = cfg.base_url.clone().unwrap_or_else(|| {
        let port = cfg.listen.rsplit(':').next().unwrap_or("8088");
        let ip = local_ipv4().unwrap_or_else(|| "127.0.0.1".into());
        format!("http://{ip}:{port}")
    });
    info!("母列表 base_url = {base_url}");

    let cfg = Arc::new(cfg);
    let app_state = server::AppState {
        channels: Arc::new(RwLock::new(channels)),
        cfg: cfg.clone(),
        base_url,
        logo_base: cfg.logo_base.clone(),
    };
    let app = server::build(app_state);
    let addr: SocketAddr = cfg.listen.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP listening on http://{addr}/playlist.m3u");
    if let Err(e) = axum::serve(listener, app).await {
        error!("http server: {e}");
    }
    Ok(())
}

/// 取本机第一个非 127.0.0.1 的 IPv4
fn local_ipv4() -> Option<String> {
    use std::process::Command;
    let out = Command::new("hostname").arg("-I").output().ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.split_whitespace()
        .find(|ip| !ip.starts_with("127.") && ip.contains('.'))
        .map(|s| s.to_string())
}
