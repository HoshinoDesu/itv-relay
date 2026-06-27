//! 对外 HTTP 服务:
//! - GET /playlist.m3u  → 频道播放列表 (绝对地址 + jsdelivr logo)
//! - GET /play/tv-N     → 启动转码会话, pipe HTTP-TS 流式响应

use crate::config::Config;
use crate::playlist::{render_master, Channel};
use crate::streambuf::{StreamBuf, StreamReader};
use axum::{
    body::Body,
    extract::{Path as AxumPath, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::stream::unfold;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub channels: Arc<RwLock<Vec<Channel>>>,
    pub cfg: Arc<Config>,
    pub base_url: String,
    pub logo_base: String,
}

pub fn build(state: AppState) -> Router {
    Router::new()
        .route("/playlist.m3u", get(playlist))
        .route("/play/:slug", get(play))
        .with_state(state)
}

async fn playlist(State(st): State<AppState>) -> Response {
    let chs = st.channels.read().await;
    let body = render_master(&chs, &st.base_url, &st.logo_base);
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "audio/x-mpegurl; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

async fn play(State(st): State<AppState>, AxumPath(slug): AxumPath<String>) -> Response {
    let idx: usize = match slug.strip_prefix("tv-").and_then(|s| s.parse().ok()) {
        Some(i) => i,
        None => return (StatusCode::NOT_FOUND, "bad channel").into_response(),
    };
    let (source, display_name) = {
        let chs = st.channels.read().await;
        match chs.iter().find(|c| c.index == idx) {
            Some(c) => (c.source_url.clone(), c.display_name.clone()),
            None => return (StatusCode::NOT_FOUND, "no such channel").into_response(),
        }
    };
    info!(target: "server", "/play/tv-{} ({}) 启动会话", idx, display_name);

    let (writer, reader) = StreamBuf::new();
    let session = crate::session::Session::new(idx, source, st.cfg.clone());
    tokio::spawn(async move {
        session.run(writer).await;
    });

    let stream = reader_to_stream(reader);
    let body = Body::from_stream(stream);
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "video/mp2t"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

fn reader_to_stream(
    reader: StreamReader,
) -> impl futures_util::Stream<Item = Result<axum::body::Bytes, std::io::Error>> {
    unfold(reader, |r| async move {
        let data = r.recv().await?;
        Some((Ok(axum::body::Bytes::from(data)), r))
    })
}
