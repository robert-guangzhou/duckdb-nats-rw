//! core NATS 发布（非 JetStream，core-publish 计划 §2.2/2.3）。
//!
//! 语义边界：不建流、不查流、无 ack 持久化；`confirm` 仅是传输层确认——
//! 标量 = 单条后一次 flush；批量 = 批末一次 flush（NATS 协议保序，
//! PONG 返回即保证此前全部 PUB 已被服务器处理，与逐条 PING 等价，
//! 一批 256 条只花 1 次往返）。
//!
//! per-url 统计：published/errors 原子计数 + last_error/时间戳；
//! `reset` 用 `swap(0)` 读后原子清零——每次 fetch_add 恰好落入一个窗口，
//! 不丢不重（增量轮询的标准手法）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::auth::AuthMethod;
use crate::conn_cache::connect_cached;
use crate::tls::TlsConfig;

/// 单条/单批（不含 flush 等待）的操作级超时。
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
/// confirm（flush 往返）的独立超时。
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// 发布一行（subject + payload 字节）。
pub(crate) struct PublishRow {
    pub(crate) subject: String,
    pub(crate) payload: Vec<u8>,
}

/// subject 校验：非空、无空白/CR/LF（对齐 sql_audit 已验证的注入防御口径；
/// async-nats 侧另有校验，此处为 FFI 入口的第一道防线，可离线单测）。
pub(crate) fn validate_subject(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err("nats_rw_publish: subject must not be empty".into());
    }
    if s.chars().any(|c| c.is_whitespace() || c == '\r' || c == '\n') {
        return Err(format!(
            "nats_rw_publish: subject '{}' contains whitespace/CR/LF — not a valid NATS subject token",
            s
        ));
    }
    Ok(())
}

/// 单条发布；confirm = 发布后 flush（标量粒度）。
pub(crate) async fn publish_one(
    url: &str,
    auth: &AuthMethod,
    tls: &TlsConfig,
    row: PublishRow,
    confirm: bool,
) -> Result<(), String> {
    validate_subject(&row.subject).map_err(|e| { stats::record(url, false, Some(e.clone())); e })?;
    let client = connect_cached(url, auth, tls)
        .await
        .map_err(|e| { stats::record(url, false, Some(e.clone())); e })?;
    match tokio::time::timeout(PUBLISH_TIMEOUT, client.publish(row.subject.clone(), row.payload.into()))
        .await
    {
        Err(_) => {
            let e = format!("publish to '{}' timed out after {}s", row.subject, PUBLISH_TIMEOUT.as_secs());
            Err(fail(url)(e))
        }
        Ok(Err(e)) => {
            let e = format!("publish to '{}' failed: {e}", row.subject);
            Err(fail(url)(e))
        }
        Ok(Ok(())) => {
            if confirm {
                match tokio::time::timeout(FLUSH_TIMEOUT, client.flush()).await {
                    Err(_) => {
                        let e = format!(
                            "confirm (flush) after publishing to '{}' timed out after {}s",
                            row.subject,
                            FLUSH_TIMEOUT.as_secs()
                        );
                        Err(fail(url)(e))
                    }
                    Ok(Err(e)) => {
                        let e = format!("confirm (flush) after publishing to '{}' failed: {e}", row.subject);
                        Err(fail(url)(e))
                    }
                    Ok(Ok(())) => {
                        stats::record(url, true, None);
                        Ok(())
                    }
                }
            } else {
                stats::record(url, true, None);
                Ok(())
            }
        }
    }
}

/// 批量发布（一批一连接复用）；confirm = 批末一次 flush（批量粒度）。
/// 返回成功发布条数；任一条失败立即返回 Err（此前已发布的条数已计入统计）。
pub(crate) async fn publish_batch(
    url: &str,
    auth: &AuthMethod,
    tls: &TlsConfig,
    rows: Vec<PublishRow>,
    confirm: bool,
) -> Result<u64, String> {
    for r in &rows {
        validate_subject(&r.subject).map_err(|e| { stats::record(url, false, Some(e.clone())); e })?;
    }
    let client = connect_cached(url, auth, tls)
        .await
        .map_err(|e| { stats::record(url, false, Some(e.clone())); e })?;
    let mut published: u64 = 0;
    for row in rows {
        match tokio::time::timeout(PUBLISH_TIMEOUT, client.publish(row.subject.clone(), row.payload.into()))
            .await
        {
            Err(_) => {
                let e = format!("publish to '{}' timed out after {}s", row.subject, PUBLISH_TIMEOUT.as_secs());
                stats::record(url, false, Some(e.clone()));
                return Err(e);
            }
            Ok(Err(e)) => {
                let e = format!("publish to '{}' failed: {e}", row.subject);
                stats::record(url, false, Some(e.clone()));
                return Err(e);
            }
            Ok(Ok(())) => published += 1,
        }
    }
    if confirm {
        match tokio::time::timeout(FLUSH_TIMEOUT, client.flush()).await {
            Err(_) => {
                let e = format!(
                    "confirm (flush) after batch of {published} publishes timed out after {}s",
                    FLUSH_TIMEOUT.as_secs()
                );
                stats::record(url, false, Some(e.clone()));
                return Err(e);
            }
            Ok(Err(e)) => {
                let e = format!("confirm (flush) after batch of {published} publishes failed: {e}");
                stats::record(url, false, Some(e.clone()));
                return Err(e);
            }
            Ok(Ok(())) => {}
        }
    }
    stats::record_batch(url, published, 0);
    Ok(published)
}

/// 统计查询结果一行。
pub(crate) struct StatsRow {
    pub(crate) url: String,
    pub(crate) published: i64,
    pub(crate) errors: i64,
    pub(crate) last_error: Option<String>,
    /// epoch 毫秒；None = 无错误记录。
    pub(crate) last_error_at_ms: Option<i64>,
}

/// 查询统计：url_filter 为空 = 跨所有 url 聚合为单行（url 列为 "<all>"）；
/// 非空 = 仅该 url 的单行（无记录则零值行）。reset = 读后原子清零。
pub(crate) fn stats_query(url_filter: &str, reset: bool) -> StatsRow {
    stats::query(url_filter, reset)
}

/// 记录失败并原样返回错误串的闭包工厂（避免调用点重复写两遍错误串）。
fn fail(url: &str) -> impl Fn(String) -> String + '_ {
    move |e| {
        stats::record(url, false, Some(e.clone()));
        e
    }
}

// ==== per-url 统计（pub(crate) 供集成测试） ====
mod stats {
    use super::*;

    pub(super) struct Entry {
        pub(super) published: AtomicI64,
        pub(super) errors: AtomicI64,
        pub(super) last_error: Mutex<Option<(String, i64)>>,
    }

    impl Entry {
        fn new() -> Self {
            Self {
                published: AtomicI64::new(0),
                errors: AtomicI64::new(0),
                last_error: Mutex::new(None),
            }
        }
    }

    fn map() -> &'static Mutex<HashMap<String, Entry>> {
        static M: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
        M.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// 记录一次结果；失败时携带错误消息。
    pub(super) fn record(url: &str, ok: bool, err: Option<String>) {
        let mut m = map().lock().unwrap();
        let e = m.entry(url.to_string()).or_insert_with(Entry::new);
        if ok {
            e.published.fetch_add(1, Ordering::SeqCst);
        } else {
            e.errors.fetch_add(1, Ordering::SeqCst);
            if let Some(msg) = err {
                if let Ok(mut le) = e.last_error.lock() {
                    *le = Some((msg, now_ms()));
                }
            }
        }
    }

    /// 批量记录（成功 n 条 + 0 错误；errors>0 场景走 record 逐条路径）。
    pub(super) fn record_batch(url: &str, published: u64, _errors: u64) {
        let mut m = map().lock().unwrap();
        let e = m.entry(url.to_string()).or_insert_with(Entry::new);
        e.published.fetch_add(published as i64, Ordering::SeqCst);
    }

    pub(super) fn query(url_filter: &str, reset: bool) -> StatsRow {
        let mut m = map().lock().unwrap();
        let take = |e: &Entry, reset: bool| -> (i64, i64, Option<(String, i64)>) {
            let p = if reset { e.published.swap(0, Ordering::SeqCst) } else { e.published.load(Ordering::SeqCst) };
            let er = if reset { e.errors.swap(0, Ordering::SeqCst) } else { e.errors.load(Ordering::SeqCst) };
            let le = if reset {
                e.last_error.lock().ok().and_then(|mut g| g.take())
            } else {
                e.last_error.lock().ok().and_then(|g| g.clone())
            };
            (p, er, le)
        };
        if url_filter.is_empty() {
            let (mut p, mut er) = (0i64, 0i64);
            let mut last: Option<(String, i64)> = None;
            for e in m.values() {
                let (ep, eer, ele) = take(e, reset);
                p += ep;
                er += eer;
                if let Some((msg, at)) = ele {
                    if last.as_ref().map(|(_, t)| at >= *t).unwrap_or(true) {
                        last = Some((msg, at));
                    }
                }
            }
            let (last_error, last_error_at_ms) = match last {
                Some((m, t)) => (Some(m), Some(t)),
                None => (None, None),
            };
            StatsRow { url: "<all>".into(), published: p, errors: er, last_error, last_error_at_ms }
        } else {
            // 精确匹配（与连接键同串：C++ 传入 secret 解析出的连接 url 原文）
            match m.get(url_filter) {
                Some(e) => {
                    let (p, er, le) = take(e, reset);
                    StatsRow {
                        url: url_filter.to_string(),
                        published: p,
                        errors: er,
                        last_error: le.as_ref().map(|(m, _)| m.clone()),
                        last_error_at_ms: le.map(|(_, t)| t),
                    }
                }
                None => StatsRow {
                    url: url_filter.to_string(),
                    published: 0,
                    errors: 0,
                    last_error: None,
                    last_error_at_ms: None,
                },
            }
        }
    }

    /// 清空（仅测试）。
    #[cfg(test)]
    pub(super) fn clear_for_test() {
        map().lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(subject: &str, payload: &[u8]) -> PublishRow {
        PublishRow { subject: subject.into(), payload: payload.to_vec() }
    }

    // —— subject 校验（离线）——
    #[test]
    fn subject_empty_rejected() {
        assert!(validate_subject("").is_err());
    }

    #[test]
    fn subject_with_space_rejected() {
        let err = validate_subject("a b").unwrap_err();
        assert!(err.contains("whitespace"), "unexpected: {err}");
    }

    #[test]
    fn subject_with_crlf_rejected() {
        assert!(validate_subject("a\r\nb").is_err());
        assert!(validate_subject("a\n").is_err());
    }

    #[test]
    fn subject_plain_ok() {
        assert!(validate_subject("audit.a1").is_ok());
        assert!(validate_subject("seed.a.*.>").is_ok()); // 通配符合法
    }

    // —— 统计语义（离线；swap 窗口不丢不重）——
    #[test]
    fn stats_record_and_query() {
        stats::clear_for_test();
        stats::record("nats://u1", true, None);
        stats::record("nats://u1", true, None);
        stats::record("nats://u1", false, Some("boom".into()));
        let r = stats_query("nats://u1", false);
        assert_eq!(r.published, 2);
        assert_eq!(r.errors, 1);
        assert_eq!(r.last_error.as_deref(), Some("boom"));
        assert!(r.last_error_at_ms.is_some());
    }

    #[test]
    fn stats_reset_reads_then_zeroes() {
        stats::clear_for_test();
        stats::record_batch("nats://u2", 5, 0);
        let r = stats_query("nats://u2", true);
        assert_eq!(r.published, 5);
        let r2 = stats_query("nats://u2", false);
        assert_eq!(r2.published, 0, "reset 后应为零");
        // reset 窗口后的增量进入新窗口
        stats::record("nats://u2", true, None);
        assert_eq!(stats_query("nats://u2", false).published, 1);
    }

    #[test]
    fn stats_aggregate_and_filter() {
        stats::clear_for_test();
        stats::record("nats://a", true, None);
        stats::record("nats://b", false, Some("e-b".into()));
        let all = stats_query("", false);
        assert_eq!(all.url, "<all>");
        assert_eq!(all.published, 1);
        assert_eq!(all.errors, 1);
        let only_b = stats_query("nats://b", false);
        assert_eq!(only_b.url, "nats://b");
        assert_eq!(only_b.published, 0);
        assert_eq!(only_b.errors, 1);
        // 无匹配 url = 零值行（非错误）
        let none = stats_query("nats://zzz", false);
        assert_eq!(none.published, 0);
        assert_eq!(none.errors, 0);
    }
}
