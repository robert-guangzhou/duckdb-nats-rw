//! JetStream 写入（阶段三）：COPY TO … (FORMAT nats_rw) 的 Rust 侧实现。
//!
//! 语义（Dev Plan 3.3）：逐行 publish 并等待服务端 ack（JetStream at-least-once 持久化）；
//! 顺序保持与源行一致（串行 publish-ack）。`create_stream := true` 时按目标 subject 精确
//! 匹配建流；否则流必须已存在。
//! MVP：串行发布；流水线并行（在途批量 ack）列为性能优化项（Dev Plan 七）。
//! FFI 契约（nats_rw_copy_execute）：0 成功 / 1 错误或 panic 转译（与 scan 一致，无独立 panic 码）。

use std::time::Duration;

use async_nats::jetstream::stream::Config;

pub(crate) struct CopyRow {
    pub(crate) subject: String,
    pub(crate) payload: Vec<u8>,
}

pub(crate) struct CopyParams {
    pub(crate) url: String,
    pub(crate) stream: String,
    pub(crate) subject: String,
    pub(crate) create_stream: bool,
}

/// 单条 publish-ack 的超时；整批操作超时由调用方（FFI 层）包裹。
const PER_MESSAGE_TIMEOUT: Duration = Duration::from_secs(10);

fn sanitize_url(url: &str) -> String {
    crate::scan::sanitize_url(url)
}

pub(crate) async fn copy(p: CopyParams, rows: Vec<CopyRow>) -> Result<u64, String> {
    let client = async_nats::connect(p.url.as_str())
        .await
        .map_err(|e| format!("connect to '{}' failed: {e}", sanitize_url(&p.url)))?;
    let js = async_nats::jetstream::new(client);

    // 建流或校验流存在
    if p.create_stream {
        js.create_or_update_stream(Config {
            name: p.stream.clone(),
            subjects: vec![p.subject.clone()],
            ..Default::default()
        })
        .await
        .map_err(|e| format!("create_stream '{}' failed: {e}", p.stream))?;
    } else {
        js.get_stream(p.stream.clone())
            .await
            .map_err(|e| format!("stream '{}' not available: {e}", p.stream))?;
    }

    let mut published: u64 = 0;
    for row in rows {
        let ack = tokio::time::timeout(
            PER_MESSAGE_TIMEOUT,
            js.publish(row.subject.clone(), row.payload.clone().into()),
        )
        .await
        .map_err(|_| format!("publish to '{}' timed out", row.subject))?
        .map_err(|e| format!("publish failed: {e}"))?
        .await
        .map_err(|e| format!("publish ack failed (subject '{}'): {e}", row.subject))?;
        let _ = ack;
        published += 1;
    }
    Ok(published)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_are_owned() {
        // 编译期形态检查：CopyRow/CopyParams 为 owned 数据，FFI 侧无借用穿越
        let rows = vec![CopyRow { subject: "a".into(), payload: vec![1u8] }];
        let p = CopyParams { url: "u".into(), stream: "s".into(), subject: "a".into(), create_stream: false };
        assert_eq!(rows.len(), 1);
        assert!(!p.create_stream);
    }
}
