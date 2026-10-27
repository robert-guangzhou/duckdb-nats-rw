//! JetStream 元数据（阶段四）：nats_rw_streams / nats_rw_stream_info 的 Rust 侧实现。

use std::ffi::CString;

use crate::auth::{connect_authenticated, AuthMethod};

pub(crate) struct StreamSummary {
    pub(crate) name: CString,
    pub(crate) messages: u64,
    pub(crate) bytes: u64,
    pub(crate) last_seq: u64,
}

pub(crate) struct StreamsData {
    pub(crate) streams: Vec<StreamSummary>,
}

pub(crate) struct StreamInfoData {
    pub(crate) name: CString,
    pub(crate) subjects: Vec<CString>,
    pub(crate) messages: u64,
    pub(crate) bytes: u64,
    pub(crate) first_seq: u64,
    pub(crate) last_seq: u64,
    pub(crate) consumer_count: u64,
}

fn to_cstring(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| format!("string contains NUL byte: {s:?}"))
}

/// 全部流摘要。stream_names 为惰性迭代器，逐名取 info（N 小时合理；大集群分页列阶段五）。
pub(crate) async fn list_streams(url: &str, auth: &AuthMethod) -> Result<Box<StreamsData>, String> {
    let client = connect_authenticated(url, auth).await?;
    let js = async_nats::jetstream::new(client);
    let mut summaries = Vec::new();
    let mut names = js.stream_names();
    while let Some(name) = names.next().await {
        let name = name.map_err(|e| format!("stream_names iteration failed: {e}"))?;
        let name_str: String = name.to_string();
        let mut stream = js
            .get_stream(name)
            .await
            .map_err(|e| format!("get_stream '{}' failed: {e}", name_str))?;
        let info = stream
            .info()
            .await
            .map_err(|e| format!("stream info for '{}' failed: {e}", name_str))?;
        summaries.push(StreamSummary {
            name: to_cstring(&name_str)?,
            messages: info.state.messages,
            bytes: info.state.bytes,
            last_seq: info.state.last_sequence,
        });
    }
    Ok(Box::new(StreamsData { streams: summaries }))
}

/// 单流详情（流不存在 → 含流名的可读错误）。
pub(crate) async fn stream_info(
    url: &str,
    auth: &AuthMethod,
    stream_name: &str,
) -> Result<Box<StreamInfoData>, String> {
    let client = connect_authenticated(url, auth).await?;
    let js = async_nats::jetstream::new(client);
    let mut stream = js
        .get_stream(stream_name.to_string())
        .await
        .map_err(|e| format!("stream '{}' not available: {e}", stream_name))?;
    let info = stream
        .info()
        .await
        .map_err(|e| format!("stream info for '{}' failed: {e}", stream_name))?;
    let mut subjects = Vec::new();
    for s in &info.config.subjects {
        subjects.push(to_cstring(s)?);
    }
    Ok(Box::new(StreamInfoData {
        name: to_cstring(stream_name)?,
        subjects,
        messages: info.state.messages,
        bytes: info.state.bytes,
        first_seq: info.state.first_sequence,
        last_seq: info.state.last_sequence,
        consumer_count: info.state.consumer_count as u64,
    }))
}

// StreamExt 供 stream_names 迭代
use futures_util::StreamExt;
