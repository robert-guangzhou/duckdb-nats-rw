//! 播种工具：重建 seed_stream 并发布确定性的 10 条消息（TP-02 fixtures）。
//! 在 builder 容器内运行：`cargo run --bin seed -- nats://nats:4222`
//! 取代 synadia/nats-box 镜像方案（daocloud 对该命名空间返回 403，见 docs/notes-build.md）。

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let url = std::env::args().nth(1).unwrap_or_else(|| "nats://nats:4222".into());
    match seed(&url).await {
        Ok(count) => {
            println!("seeded 'seed_stream' with {count} messages");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("seed failed: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn seed(url: &str) -> Result<usize, String> {
    let client = async_nats::connect(url).await.map_err(|e| e.to_string())?;
    let js = async_nats::jetstream::new(client);

    // 幂等：先删旧流（不存在则忽略），再按固定配置重建，保证 seq 从 1 开始
    let _ = js.delete_stream("seed_stream").await;
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "seed_stream".into(),
        subjects: vec!["seed.>".into()],
        allow_direct: true,
        ..Default::default()
    })
    .await
    .map_err(|e| format!("create_stream failed: {e}"))?;

    let mut count = 0usize;
    for (prefix, unit) in [("a", "alpha"), ("b", "beta")] {
        for i in 1..=5u32 {
            let mut headers = async_nats::header::HeaderMap::new();
            headers.insert("unit", unit);
            let subject = format!("seed.{prefix}.{i}");
            let payload = format!("payload-{prefix}-{i}");
            // JetStream publish：第一次 await 发出，第二次 await 等服务端确认
            js.publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| format!("publish failed: {e}"))?
                .await
                .map_err(|e| format!("publish ack failed: {e}"))?;
            count += 1;
        }
    }
    Ok(count)
}
