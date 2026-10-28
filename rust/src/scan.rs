//! JetStream 有界快照扫描（阶段二 MVP）。
//!
//! 语义（Dev Plan 3.3）：读取 [start_seq, end_seq] 区间内、扫描开始时已持久化的消息；
//! 不创建 consumer、不 ack。end_seq 缺省为扫描开始时流状态里的 last_seq。
//! MVP 实现把区间一次性物化进内存（有界快照）；批量/流式优化见计划第七章。

use std::ffi::CString;

use async_nats::jetstream::message::StreamMessage;
use futures_util::StreamExt;

use crate::auth::{connect_authenticated, AuthMethod};
use crate::subject_match::subject_matches;

pub(crate) struct ScanParams {
    pub(crate) stream: String,
    pub(crate) url: String,
    pub(crate) auth: AuthMethod,
    pub(crate) subject: Option<String>,
    pub(crate) start_seq: u64,
    /// 0 = 扫描开始时的 last_seq
    pub(crate) end_seq: u64,
    pub(crate) start_time_nanos: Option<i64>,
    pub(crate) end_time_nanos: Option<i64>,
    pub(crate) concurrency: usize,
    /// JSON 字段路径列表（如 ["device_id", "location.zone"]）——payload 为 JSON 时
    /// 提取指定字段作为额外输出列；非 JSON payload 该列输出 NULL。
    pub(crate) json_extract: Vec<String>,
}

pub(crate) struct ScanMsg {
    pub(crate) subject: CString,
    pub(crate) sequence: u64,
    /// 0 = 服务端未提供时间
    pub(crate) time_nanos: i64,
    pub(crate) headers: Vec<(CString, CString)>,
    pub(crate) payload: Vec<u8>,
    /// json_extract 的结果：每路径一个值（None = 非 JSON 或字段不存在）
    pub(crate) json_fields: Vec<Option<CString>>,
}

pub(crate) struct ScanData {
    pub(crate) stream: CString,
    pub(crate) msgs: Vec<ScanMsg>,
}

/// 隐藏 URL 中的 userinfo（user:pass@），防止错误消息泄露凭证（Dev Plan 六章）。
pub(crate) fn sanitize_url(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(s), Some(at)) if at > s + 3 => {
            let (scheme, _) = url.split_at(s + 3);
            let host = &url[at..];
            format!("{scheme}***{host}")
        }
        _ => url.to_string(),
    }
}

/// direct_get 对已删除/擦除的序列号返回错误，需跳过后继续。
/// "404" 仅在与 "message" 同时出现时认定，避免端口号等含 404 子串的错误被误判
/// （审查 F8，2026-10-27）。
fn is_no_message(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("no message") || e.contains("not found") || (e.contains("404") && e.contains("message"))
}

/// 流未开启 allow_direct 时 direct_get 返回 503 no responders，
/// 此时回退到 get_raw_message（走流 leader 的 JetStream API）。
fn is_no_responders(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("no responders") || e.contains("503")
}

pub(crate) async fn scan(p: ScanParams) -> Result<Box<ScanData>, String> {
    let client = connect_authenticated(&p.url, &p.auth).await?;
    let jetstream = async_nats::jetstream::new(client);
    let mut stream = jetstream
        .get_stream(p.stream.clone())
        .await
        .map_err(|e| format!("stream '{}' not available: {e}", p.stream))?;
    let info = stream
        .info()
        .await
        .map_err(|e| format!("stream info for '{}' failed: {e}", p.stream))?;
    let last_seq = info.state.last_sequence;

    let start = p.start_seq.max(1);
    let end = if p.end_seq == 0 { last_seq } else { p.end_seq.min(last_seq) };
    let stream_name = p.stream.clone();
    if start > end {
        return Ok(Box::new(ScanData { stream: to_cstring(&stream_name)?, msgs: Vec::new() }));
    }

    let mut fetched: Vec<StreamMessage> = Vec::new();
    // 读模式（同 nats_js 语义）：优先 direct_get（需 allow_direct），
    // 无响应者时逐条回退 get_raw_message（无需 allow_direct）。
    let stream_ref = &stream;
    let mut iter = futures_util::stream::iter(start..=end)
        .map(move |seq| async move {
            match stream_ref.direct_get(seq).await {
                Ok(m) => Ok(Some(m)),
                Err(e) => {
                    let text = e.to_string();
                    if is_no_message(&text) {
                        return Ok(None);
                    }
                    if is_no_responders(&text) {
                        return match stream_ref.get_raw_message(seq).await {
                            Ok(m) => Ok(Some(m)),
                            Err(e2) => {
                                let t2 = e2.to_string();
                                if is_no_message(&t2) {
                                    return Ok(None);
                                }
                                Err(format!("get_raw_message failed: {t2}"))
                            }
                        };
                    }
                    Err(format!("direct_get failed: {text}"))
                }
            }
        })
        .buffer_unordered(p.concurrency.max(1));
    while let Some(item) = iter.next().await {
        match item {
            Ok(Some(m)) => fetched.push(m),
            Ok(None) => continue,
            Err(msg) => return Err(format!("read on stream '{stream_name}' failed: {msg}")),
        }
    }
    fetched.sort_by_key(|m| m.sequence);

    let subject_filter = p.subject.as_deref().filter(|s| !s.is_empty());
    let mut msgs: Vec<ScanMsg> = Vec::with_capacity(fetched.len());
    for m in fetched {
        if let Some(pattern) = subject_filter {
            if !subject_matches(pattern, m.subject.as_ref()) {
                continue;
            }
        }
        let nanos = m.time.unix_timestamp_nanos();
        if let Some(lo) = p.start_time_nanos {
            if nanos < i128::from(lo) {
                continue;
            }
        }
        if let Some(hi) = p.end_time_nanos {
            if nanos > i128::from(hi) {
                continue;
            }
        }
        // header 取每键首个值；多值完整支持列入后续阶段（docs/notes-build.md）。
        // 过滤 NATS 内部传输头（Nats-Time-Stamp 等），不作为用户可见 headers 暴露。
        let mut headers = Vec::new();
        for (k, values) in m.headers.iter() {
            if k.to_string().to_lowercase().starts_with("nats-") {
                continue;
            }
            let Some(first) = values.first() else { continue };
            headers.push((to_cstring(&k.to_string())?, to_cstring(first.as_str())?));
        }
        msgs.push(ScanMsg {
            subject: to_cstring(m.subject.as_ref())?,
            sequence: m.sequence,
            time_nanos: nanos as i64,
            headers,
            payload: m.payload.to_vec(),
            json_fields: extract_json_fields(&m.payload, &p.json_extract),
        });
    }
    Ok(Box::new(ScanData { stream: to_cstring(&stream_name)?, msgs }))
}

fn to_cstring(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| format!("string contains NUL byte: {:?}", s))
}

/// 按点分路径从 JSON payload 提取字段值（"location.zone" → payload.location.zone）。
/// 非 JSON payload 或字段不存在 → None。
fn extract_json_fields(payload: &[u8], paths: &[String]) -> Vec<Option<CString>> {
    if paths.is_empty() {
        return Vec::new();
    }
    let parsed: Option<serde_json::Value> = serde_json::from_slice(payload).ok();
    paths
        .iter()
        .map(|path| {
            let root = parsed.as_ref()?;
            let mut current = root;
            for segment in path.split('.') {
                current = current.get(segment)?;
            }
            match current {
                serde_json::Value::String(s) => Some(s.clone()),
                other => Some(other.to_string()),
            }
            .and_then(|v| CString::new(v).ok())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_sanitizer_hides_credentials() {
        assert_eq!(sanitize_url("nats://user:secret@host:4222"), "nats://***@host:4222");
        assert_eq!(sanitize_url("nats://host:4222"), "nats://host:4222");
        assert_eq!(sanitize_url("tls://a@b"), "tls://***@b");
        // 无 scheme 的 @ 不误伤
        assert_eq!(sanitize_url("plain@thing"), "plain@thing");
    }

    #[test]
    fn no_message_detection() {
        assert!(is_no_message("No Message found"));
        assert!(is_no_message("stream not found"));
        assert!(is_no_message("404 no message for sequence"));
        assert!(!is_no_message("connection refused"));
        // 含 404 子串但与消息无关的错误不得误判（审查 F8）
        assert!(!is_no_message("connect to port 4040 refused"));
    }

    #[test]
    fn no_responders_detection() {
        assert!(is_no_responders("unable to get message: 503 no responders"));
        assert!(is_no_responders("503 Service Unavailable"));
        assert!(!is_no_responders("timeout"));
    }

    #[test]
    fn json_field_extraction() {
        let payload = br#"{"device_id":"sensor-1","location":{"zone":"north","floor":3},"kw":12.5,"active":true}"#;
        let paths: Vec<String> = vec!["device_id".into(), "location.zone".into(), "kw".into(), "active".into(), "missing".into()];

        let fields = extract_json_fields(payload, &paths);
        assert_eq!(fields[0].as_ref().map(|s| s.to_str().unwrap()), Some("sensor-1"));
        assert_eq!(fields[1].as_ref().map(|s| s.to_str().unwrap()), Some("north"));
        assert_eq!(fields[2].as_ref().map(|s| s.to_str().unwrap()), Some("12.5"));
        assert_eq!(fields[3].as_ref().map(|s| s.to_str().unwrap()), Some("true"));
        assert_eq!(fields[4], None); // 字段不存在
    }

    #[test]
    fn json_field_extraction_non_json_payload() {
        let payload = b"not json at all";
        let paths: Vec<String> = vec!["field".into()];
        let fields = extract_json_fields(payload, &paths);
        assert_eq!(fields[0], None); // 非 JSON → None，不报错
    }

    #[test]
    fn json_field_extraction_empty_paths() {
        let payload = br#"{"a":1}"#;
        let fields = extract_json_fields(payload, &[]);
        assert!(fields.is_empty());
    }
}
