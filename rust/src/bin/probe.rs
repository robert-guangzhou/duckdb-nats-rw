//! 诊断探针：复刻 scan 的调用链并打印每步真实结果（临时工具）。
//! `cargo run --bin probe -- nats://nats:4222 seed_stream`

use futures_util::StreamExt;

#[tokio::main]
async fn main() {
    let url = std::env::args().nth(1).unwrap_or_else(|| "nats://nats:4222".into());
    let stream_name = std::env::args().nth(2).unwrap_or_else(|| "seed_stream".into());

    let client = match async_nats::connect(&url).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect FAILED: {e}");
            return;
        }
    };
    println!("connect OK");
    let js = async_nats::jetstream::new(client);
    let mut stream = match js.get_stream(stream_name.clone()).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("get_stream FAILED: {e}");
            return;
        }
    };
    println!("get_stream OK");
    match stream.info().await {
        Ok(info) => println!(
            "info: messages={} first_seq={} last_seq={}",
            info.state.messages, info.state.first_sequence, info.state.last_sequence
        ),
        Err(e) => eprintln!("info FAILED: {e}"),
    }
    match stream.direct_get(1).await {
        Ok(m) => println!(
            "direct_get(1) OK: subject={} payload={}",
            m.subject.as_ref(),
            String::from_utf8_lossy(&m.payload)
        ),
        Err(e) => println!("direct_get(1) FAILED: {e}"),
    }
    // 与 scan 相同的并发遍历：1..=10
    let stream_ref = &stream;
    let mut count_ok = 0usize;
    let mut count_skip = 0usize;
    let mut count_err = 0usize;
    let mut it = futures_util::stream::iter(1u64..=10)
        .map(move |seq| async move { stream_ref.direct_get(seq).await })
        .buffer_unordered(8);
    while let Some(item) = it.next().await {
        match item {
            Ok(_) => count_ok += 1,
            Err(e) => {
                let t = e.to_string();
                if t.to_lowercase().contains("no message") || t.to_lowercase().contains("not found") {
                    count_skip += 1;
                } else {
                    count_err += 1;
                    println!("  seq error: {t}");
                }
            }
        }
    }
    println!("buffered 1..=10: ok={count_ok} skipped={count_skip} other_err={count_err}");
}
