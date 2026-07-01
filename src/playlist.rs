//! 频道列表解析: 从 m3u 文件读频道清单。

use anyhow::{Context, Result};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Channel {
    /// 序号 (从 1 开始), 用作 /play/tv-{index}
    pub index: usize,
    /// 显示给播放器的中文名
    pub display_name: String,
    /// 上游源 URL
    pub source_url: String,
    /// EPG 匹配 ID (来自 tvg-id 属性)
    pub tvg_id: Option<String>,
}

/// 解析 m3u: 每行 #EXTINF 含 tvg-name 或末尾逗号后频道名, 下一行是 URL
pub fn load(path: &Path) -> Result<Vec<Channel>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read playlist {path:?}"))?;
    Ok(parse(&raw))
}

pub fn parse(raw: &str) -> Vec<Channel> {
    let mut out = Vec::new();
    let mut pending_name: Option<String> = None;
    let mut pending_tvg_id: Option<String> = None;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("#EXTINF") {
            let name = extract_attr(line, "tvg-name")
                .or_else(|| line.rsplit_once(',').map(|(_, n)| n.trim().to_string()))
                .unwrap_or_else(|| "unknown".to_string());
            pending_tvg_id = extract_attr(line, "tvg-id");
            pending_name = Some(name);
        } else if line.starts_with('#') {
            continue;
        } else {
            if let Some(name) = pending_name.take() {
                out.push(Channel {
                    index: out.len() + 1,
                    display_name: name,
                    source_url: line.to_string(),
                    tvg_id: pending_tvg_id.take(),
                });
            }
        }
    }
    out
}

fn extract_attr(line: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let i = line.find(&needle)? + needle.len();
    let rest = &line[i..];
    let j = rest.find('"')?;
    Some(rest[..j].to_string())
}

/// 生成播放列表 m3u: 绝对地址 play/tv-{index} + jsdelivr 加速的 tvg-logo
pub fn render_master(
    channels: &[Channel],
    base_url: &str,
    logo_base: &str,
    epg_url: Option<&str>,
) -> String {
    let mut s = match epg_url {
        Some(url) => format!("#EXTM3U x-tvg-url=\"{}\"\n", escape_attr(url)),
        None => String::from("#EXTM3U\n"),
    };
    for ch in channels {
        let logo = logo_url(&ch.display_name, logo_base);
        let tvg_id_attr = match &ch.tvg_id {
            Some(id) => format!("tvg-id=\"{}\" ", escape_attr(id)),
            None => String::new(),
        };
        let display_name = sanitize_text(&ch.display_name);
        s.push_str(&format!(
            "#EXTINF:-1 {}tvg-name=\"{}\" tvg-logo=\"{}\",{}\n",
            tvg_id_attr,
            escape_attr(&display_name),
            escape_attr(&logo),
            display_name
        ));
        s.push_str(&format!(
            "{}/play/tv-{}\n",
            base_url.trim_end_matches('/'),
            ch.index
        ));
    }
    s
}

fn escape_attr(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in sanitize_text(value).chars() {
        match ch {
            '"' => out.push_str("&quot;"),
            _ => out.push(ch),
        }
    }
    out
}

fn sanitize_text(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_control() || *ch == '\t')
        .collect()
}

/// 频道名 -> logo 文件名: 去掉 -高清/-超清/-标清/-HD/-4K 等后缀, 去空格
fn logo_file(display: &str) -> String {
    let mut s = display.trim().to_string();
    // 去尾部画质后缀 (可能带 - 或 + 连接)
    for suffix in [
        "-高清", "-超清", "-标清", "-HD", "-4K", "-FHD", "-SD", "-666666", "+高清",
    ] {
        if s.ends_with(suffix) {
            s.truncate(s.len() - suffix.len());
            break;
        }
    }
    s.retain(|c| c != ' ');
    s
}

fn logo_url(display: &str, logo_base: &str) -> String {
    format!(
        "{}/{}.png",
        logo_base.trim_end_matches('/'),
        logo_file(display)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_master_escapes_extinf_attributes_and_sanitizes_display_text() {
        let channels = vec![Channel {
            index: 1,
            display_name: "A&B \"News\"\nHD".into(),
            source_url: "http://source".into(),
            tvg_id: Some("id\"&1".into()),
        }];

        let rendered = render_master(
            &channels,
            "http://relay/",
            "http://logo.example/a&b",
            Some("http://epg.example/epg.xml?a=1&b=2"),
        );

        assert!(rendered.contains("#EXTM3U x-tvg-url=\"http://epg.example/epg.xml?a=1&b=2\""));
        assert!(rendered.contains("tvg-id=\"id&quot;&1\""));
        assert!(rendered.contains("tvg-name=\"A&B &quot;News&quot;HD\""));
        assert!(rendered.contains("tvg-logo=\"http://logo.example/a&b/A&B&quot;News&quot;HD.png\""));
        assert!(rendered.contains(",A&B \"News\"HD\n"));
    }
}
