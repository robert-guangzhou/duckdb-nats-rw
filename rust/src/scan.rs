//! JetStream 有界快照扫描（阶段二 MVP）。
//!
//! 语义（Dev Plan 3.3）：读取 [start_seq, end_seq] 区间内、扫描开始时已持久化的消息；
//! 不创建 consumer、不 ack。end_seq 缺省为扫描开始时流状态里的 last_seq。
//! MVP 实现把区间一次性物化进内存（有界快照）；批量/流式优化见计划第七章。

use std::ffi::CString;

use async_nats::jetstream::StreamMessage;
use futures_util::StreamExt;

use crate::subject_match::subject_matches;

pub(crate) struct ScanParams {
    pub(crate) stream: String,
    pub(crate) url: String,
    pub(crate) subject: Option<String>,
    pub(crate) start_seq: u64,
    /// 0 = 扫描开始时的 last_seq
    pub(crate) end_seq: u64,
    pub(crate) start_time_nanos: Option<i64>,
    pub(crate) end_time_nanos: Option<i64>,
    pub(crate) concurrency: usize,
}

pub(crate) struct ScanMsg {
    pub(crate) subject: CString,
    pub(crate) sequence: u64,
    /// 0 = 服务端未提供时间
    pub(crate) time_nanos: i64,
    pub(crate) headers: Vec<(CString, CString)>,
    pub(crate) payload: Vec<u8>,
}

pub(crate) struct ScanData {
    pub(crate) stream: CString,
    pub(crate) msgs: Vec<ScanMsg>,
}

/// 隐藏 URL 中的 userinfo（user:pass@），防止错误消息泄露凭证（Dev Plan 六章）。
fn sanitize_url(url: &str) -> String {
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
fn is_no_message(err: &str) -> bool {
    let e = err.to_lowercase();
    e.contains("no message") || e.contains("not found") || e.contains("404")
}

pub(crate) async fn scan(p: ScanParams) -> Result<Box<ScanData>, String> {
    let client = async_nats::connect(p.url.as_str())
        .await
        .map_err(|e| format!("connect to '{}' failed: {e}", sanitize_url(&p.url)))?;
    let jetstream = async_nats::jetstream::new(client);
    let mut stream = jetstream
        .get_stream(p.stream.clone())
        .await
        .map_err(|e| format!("stream '{}' not available: {e}", p.stream))?;
    let info = stream
        .info()
        .await
        .map_err(|e| format!("stream info for '{}' failed: {e}", p.stream))?;
    let last_seq = info.state.last_seq;

    let start = p.start_seq.max(1);
    let end = if p.end_seq == 0 { last_seq } else { p.end_seq.min(last_seq) };
    let stream_name = p.stream.clone();
    if start > end {
        return Ok(Box::new(ScanData { stream: to_cstring(stream_name)?, msgs: Vec::new() }));
    }

    let mut fetched: Vec<StreamMessage> = Vec::new();
    let mut iter = futures_util::stream::iter(start..=end)
        .map(|seq| stream.direct_get(seq))
        .buffer_unordered(p.concurrency.max(1));
    while let Some(item) = iter.next().await {
        match item {
            Ok(m) => fetched.push(m),
            Err(e) => {
                let text = e.to_string();
                if is_no_message(&text) {
                    continue;
                }
                return Err(format!("direct_get failed on stream '{stream_name}': {text}"));
            }
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
        // header 取每键首个值；多值完整支持列入后续阶段（docs/notes-build.md）
        let mut headers = Vec::new();
        for (k, v) in m.headers.iter() {
            headers.push((
                to_cstring(k.as_str())?,
                to_cstring(v.to_str().unwrap_or(""))?,
            ));
        }
        msgs.push(ScanMsg {
            subject: to_cstring(m.subject.as_ref())?,
            sequence: m.sequence,
            time_nanos: nanos as i64,
            headers,
            payload: m.payload.to_vec(),
        });
    }
    Ok(Box::new(ScanData { stream: to_cstring(stream_name)?, msgs }))
}

fn to_cstring(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| format!("string contains NUL byte: {:?}", s))
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
        assert!(!is_no_message("connection refused"));
    }
}
