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

    // 幂等：先删旧流（不存在则忽略），再按固定配置重建，保证 seq 从 1 开始。
    // 同步清理 COPY 测试用的 rt_* 流（copy.test 依赖全新流状态）
    let _ = js.delete_stream("seed_stream").await;
    let _ = js.delete_stream("seed_gapped").await;
    let _ = js.delete_stream("seed_nodirect").await;
    let _ = js.delete_stream("rt_stream").await;
    let _ = js.delete_stream("rt_pc").await;
    let _ = js.delete_stream("rt_sub").await;
    let _ = js.delete_stream("rt_rt").await;
    let _ = js.delete_stream("rt_guard").await;
    let _ = js.delete_stream("rt_json").await;
    let _ = js.delete_stream("json_rt").await;
    let _ = js.delete_stream("rt_null").await;
    let _ = js.delete_stream("rt_chaos").await;
    let _ = js.delete_stream("perf_stream").await;
    // 认证服务器（阶段四）：独立连接清理
    if let Ok(client) = async_nats::ConnectOptions::new()
        .user_and_password("admin".into(), "s3cret-pass".into())
        .connect("nats://nats_userpass:4222".to_string())
        .await
    {
        let js_a = async_nats::jetstream::new(client);
        let _ = js_a.delete_stream("auth_rt").await;
    }
    if let Ok(client) = async_nats::ConnectOptions::new()
        .token("tok-12345".into())
        .connect("nats://nats_token:4222".to_string())
        .await
    {
        let js_t = async_nats::jetstream::new(client);
        let _ = js_t.delete_stream("tok_rt").await;
    }
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "seed_stream".into(),
        subjects: vec!["seed.>".into()],
        allow_direct: true,
        ..Default::default()
    })
    .await
    .map_err(|e| format!("create_stream failed: {e}"))?;

    // CG-HA-2：seed_gapped——发布 12 条后删除 seq=3、7，行使 scan 的跳过路径。
    // subjects 须与 seed_stream 的 seed.> 不相交（NATS 拒绝重叠过滤器，实测 400/10065）
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "seed_gapped".into(),
        subjects: vec!["gapseed.>".into()],
        allow_direct: true,
        ..Default::default()
    })
    .await
    .map_err(|e| format!("create_stream seed_gapped failed: {e}"))?;
    for i in 1..=12u32 {
        let subject = format!("gapseed.{i}");
        let payload = format!("gap-{i}");
        js.publish(subject, payload.into()).await
            .map_err(|e| format!("gap publish failed: {e}"))?
            .await
            .map_err(|e| format!("gap ack failed: {e}"))?;
    }
    let gapped = js.get_stream("seed_gapped").await.map_err(|e| e.to_string())?;
    for seq in [3u64, 7] {
        gapped.delete_message(seq).await.map_err(|e| format!("delete seq {seq} failed: {e}"))?;
    }

    // CG-HA-4：seed_nodirect——不开 allow_direct，行使 get_raw_message 回退路径
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "seed_nodirect".into(),
        subjects: vec!["ndseed.>".into()],
        ..Default::default()
    })
    .await
    .map_err(|e| format!("create_stream seed_nodirect failed: {e}"))?;
    for i in 1..=5u32 {
        let subject = format!("ndseed.{i}");
        js.publish(subject, format!("nd-{i}").into()).await
            .map_err(|e| format!("nd publish failed: {e}"))?
            .await
            .map_err(|e| format!("nd ack failed: {e}"))?;
    }

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
