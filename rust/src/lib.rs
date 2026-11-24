//! nats_rw Rust 核心：DuckDB 扩展的 NATS JetStream 读/写实现。
//!
//! C ABI 面仅限本 crate 中 `#[no_mangle] pub extern "C"` 导出的函数；
//! 头文件由 cbindgen 生成（`rust/regen-header.sh`），提交入库以保证构建确定性。
//! 所有进入 Rust 的 FFI 调用都经 `ffi::catch_panic` 包裹，panic 不允许跨 FFI 边界。

mod auth;
mod conn_cache;
mod copyto;
mod ffi;
mod meta;
mod publish;
mod scan;
mod subject_match;
mod tls;

use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use scan::{ScanData, ScanParams};

/// Rust 核心版本（编译期取自 Cargo.toml），含结尾 NUL。
const VERSION_C: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// COPY 整批写入的操作级超时上限（单条 publish-ack 超时在 copyto 模块）。
const COPY_BATCH_TIMEOUT: Duration = Duration::from_secs(300);

/// FFI 约定的 TLS 四字段（null/空串 = 未设置；tls_required 以 i32 传入）。
unsafe fn tls_from_ffi(
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
) -> Result<tls::TlsConfig, String> {
    Ok(tls::TlsConfig {
        root_cert: arg_opt(tls_root_cert)?.unwrap_or_default(),
        required: tls_required != 0,
        client_cert: arg_opt(tls_client_cert)?.unwrap_or_default(),
        client_key: arg_opt(tls_client_key)?.unwrap_or_default(),
    })
}

/// 可空 C 字符串 → Option<String>（null 或空串 → None）。
unsafe fn arg_opt(p: *const c_char) -> Result<Option<String>, String> {
    if p.is_null() {
        return Ok(None);
    }
    let s = arg_str(p, "tls option")?;
    Ok(if s.is_empty() { None } else { Some(s.to_string()) })
}

/// 进程内共享的 tokio 运行时：DuckDB 线程同步 block_on 使用，
/// 不与 DuckDB 线程模型混用。
/// 运行时被有意泄漏（Box::leak）：静态析构阶段 drop Runtime 会在 worker 线程
/// join 时偶发段错误（2026-10-26 实测：进程退出期 5 次 2 次崩溃）；
/// 进程级泄漏 2 个线程的资源是可接受的固定代价。
/// 退出期由 C++ 侧 std::atexit 调 `nats_rw_runtime_shutdown` 优雅停机（见下）。
static RT: OnceLock<&'static tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    *RT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to initialize tokio runtime");
        Box::leak(Box::new(rt))
    })
}

/// atexit 停机是否已执行（幂等保护）。
static SHUTDOWN_DONE: AtomicBool = AtomicBool::new(false);

/// 优雅停机 tokio runtime：由 C++ 扩展入口经 `std::atexit` 注册。
/// atexit 为 LIFO——本回调注册晚于 ASan 启动期注册的退出扫描，因而在
/// LeakSanitizer stop-the-world 之前执行；worker 线程全部 join 干净退出，
/// 消除 debug+ASan 退出期 DEADLYSIGNAL 递归风暴的触发条件
/// （plans/nats-rw-exit-storm-disk-guard-fix-plan.md F2，2026-10-26）。
/// 幂等，重复调用立即返回。
#[no_mangle]
pub extern "C" fn nats_rw_runtime_shutdown() {
    if SHUTDOWN_DONE.load(Ordering::Acquire) {
        return;
    }
    if let Some(&rt) = RT.get() {
        // shutdown_timeout(self) 按值消费 Runtime，需取回 Box::leak 的所有权。
        // 安全性：由 atexit 调用（main 已返回，无并发 FFI/查询），此后 RT 指针
        // 不再被解引用；SHUTDOWN_DONE 置位保证本块只执行一次。
        let owned = unsafe { Box::from_raw(rt as *const tokio::runtime::Runtime as *mut tokio::runtime::Runtime) };
        // 停机（join worker，最多等 2s）后 drop——危险区（静态析构期 join）已规避
        owned.shutdown_timeout(Duration::from_secs(2));
        SHUTDOWN_DONE.store(true, Ordering::Release);
    }
    // RT 未初始化（本进程从未执行扫描）：不置位——atexit 之后不会再创建 runtime。
}

/// 返回 Rust 核心版本字符串（NUL 结尾）。
#[no_mangle]
pub extern "C" fn nats_rw_core_version() -> *const c_char {
    VERSION_C.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// FFI: 有界快照扫描
// 约定：成功返回 0（*out 指向 ScanData，调用方用 nats_rw_scan_free 释放）；
// 错误与 Rust panic 一律返回 1（panic 经 ffi::catch_panic 转译为错误串，
// 不跨 FFI unwind，故无独立 panic 返回码）；*err_out 需用
// nats_rw_free_cstring 释放。
// ---------------------------------------------------------------------------

unsafe fn arg_str<'a>(p: *const c_char, name: &str) -> Result<&'a str, String> {
    if p.is_null() {
        return Err(format!("argument '{name}' must not be null"));
    }
    CStr::from_ptr(p).to_str().map_err(|_| format!("argument '{name}' is not valid UTF-8"))
}

/// 有界快照扫描。返回码：0 成功（*out 需 nats_rw_scan_free 释放）；
/// 1 错误或 Rust panic 转译（panic 不跨 FFI unwind，无独立返回码；*err_out 需
/// nats_rw_free_cstring 释放）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_execute(
    stream: *const c_char,
    url: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
    subject: *const c_char,
    start_seq: u64,
    end_seq: u64,
    start_time_nanos: i64,
    end_time_nanos: i64,
    concurrency: u32,
    json_extract_paths: *const *const c_char,
    json_extract_count: u32,
    out: *mut *mut std::ffi::c_void,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<Box<ScanData>, String> {
        // 停机后 RT 指针已悬垂（Box 被 shutdown 取回释放），必须拒绝再入而非解引用
        if SHUTDOWN_DONE.load(Ordering::Acquire) {
            return Err("nats_rw runtime already shut down (process exiting)".to_string());
        }
        let stream = arg_str(stream, "stream")?.to_string();
        let url = arg_str(url, "url")?.to_string();
        let auth = auth_from_ffi(user, password, token, nkey, creds)?;
        let tls_cfg = tls_from_ffi(tls_root_cert, tls_required, tls_client_cert, tls_client_key)?;
        let subject = if subject.is_null() { None } else { Some(arg_str(subject, "subject")?.to_string()) };
        let mut json_extract = Vec::new();
        if !json_extract_paths.is_null() && json_extract_count > 0 {
            for i in 0..json_extract_count as usize {
                let p = *json_extract_paths.add(i);
                if !p.is_null() {
                    json_extract.push(arg_str(p, "json_extract path")?.to_string());
                }
            }
        }
        let params = ScanParams {
            stream,
            url,
            auth,
            subject,
            start_seq,
            end_seq,
            start_time_nanos: if start_time_nanos == i64::MIN { None } else { Some(start_time_nanos) },
            end_time_nanos: if end_time_nanos == i64::MIN { None } else { Some(end_time_nanos) },
            concurrency: concurrency.clamp(1, 64) as usize,
            json_extract,
            tls: tls_cfg,
        };
        // 阶段二 MVP：有界快照一次性物化（计划三章 3.3 语义）
        runtime().block_on(scan::scan(params))
    };
    match ffi::catch_panic(run) {
        Ok(data) => {
            *out = Box::into_raw(data) as *mut std::ffi::c_void;
            0
        }
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_len(r: *const std::ffi::c_void) -> usize {
    (r as *const ScanData).as_ref().map(|d| d.msgs.len()).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_stream_name(r: *const std::ffi::c_void) -> *const c_char {
    (r as *const ScanData).as_ref().map(|d| d.stream.as_ptr()).unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_subject(r: *const std::ffi::c_void, i: usize) -> *const c_char {
    (r as *const ScanData)
        .as_ref()
        .and_then(|d| d.msgs.get(i))
        .map(|m| m.subject.as_ptr())
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_seq(r: *const std::ffi::c_void, i: usize) -> u64 {
    (r as *const ScanData).as_ref().and_then(|d| d.msgs.get(i)).map(|m| m.sequence).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_time_nanos(r: *const std::ffi::c_void, i: usize) -> i64 {
    (r as *const ScanData).as_ref().and_then(|d| d.msgs.get(i)).map(|m| m.time_nanos).unwrap_or(0)
}

/// header 取每个键的首个值（多值 header 的完整支持列入后续阶段）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_header_count(r: *const std::ffi::c_void, i: usize) -> u32 {
    (r as *const ScanData).as_ref().and_then(|d| d.msgs.get(i)).map(|m| m.headers.len() as u32).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_header_key(
    r: *const std::ffi::c_void,
    i: usize,
    h: u32,
) -> *const c_char {
    (r as *const ScanData)
        .as_ref()
        .and_then(|d| d.msgs.get(i))
        .and_then(|m| m.headers.get(h as usize))
        .map(|(k, _)| k.as_ptr())
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_header_value(
    r: *const std::ffi::c_void,
    i: usize,
    h: u32,
) -> *const c_char {
    (r as *const ScanData)
        .as_ref()
        .and_then(|d| d.msgs.get(i))
        .and_then(|m| m.headers.get(h as usize))
        .map(|(_, v)| v.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// json_extract 字段数（每条消息相同 = json_extract 路径数）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_json_field_count(r: *const std::ffi::c_void, i: usize) -> u32 {
    (r as *const ScanData)
        .as_ref()
        .and_then(|d| d.msgs.get(i))
        .map(|m| m.json_fields.len() as u32)
        .unwrap_or(0)
}

/// json_extract 指定路径的值（NULL = 非 JSON / 字段不存在）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_json_field(
    r: *const std::ffi::c_void,
    i: usize,
    j: u32,
) -> *const c_char {
    (r as *const ScanData)
        .as_ref()
        .and_then(|d| d.msgs.get(i))
        .and_then(|m| m.json_fields.get(j as usize))
        .and_then(|opt| opt.as_ref())
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
// where json_fields is Vec<Option<CString>> — s is &CString, as_ptr() is correct
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_payload(
    r: *const std::ffi::c_void,
    i: usize,
    len_out: *mut u64,
) -> *const u8 {
    match (r as *const ScanData).as_ref().and_then(|d| d.msgs.get(i)) {
        Some(m) => {
            if !len_out.is_null() {
                *len_out = m.payload.len() as u64;
            }
            m.payload.as_ptr()
        }
        None => {
            if !len_out.is_null() {
                *len_out = 0;
            }
            std::ptr::null()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_scan_free(r: *mut std::ffi::c_void) {
    if !r.is_null() {
        drop(Box::from_raw(r as *mut ScanData));
    }
}

/// COPY 写入：把 count 行 (subject, payload) 逐条发布到 JetStream 流并等待 ack。
/// 返回码：0 成功（*published_out 为成功发布条数）；1 错误或 Rust panic 转译
/// （*err_out 需 nats_rw_free_cstring 释放）。整批 300s 超时；单条 publish-ack
/// 超时见 copyto 模块。进程停机后再入返回错误（同 scan 契约，悬垂防护）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_copy_execute(
    stream: *const c_char,
    url: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
    subject: *const c_char,
    create_stream: i32,
    subjects: *const *const c_char,
    payloads: *const *const u8,
    payload_lens: *const u64,
    count: u64,
    published_out: *mut u64,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<u64, String> {
        if SHUTDOWN_DONE.load(Ordering::Acquire) {
            return Err("nats_rw runtime already shut down (process exiting)".to_string());
        }
        let stream = arg_str(stream, "stream")?.to_string();
        let url = arg_str(url, "url")?.to_string();
        let auth = auth_from_ffi(user, password, token, nkey, creds)?;
        let tls_cfg = tls_from_ffi(tls_root_cert, tls_required, tls_client_cert, tls_client_key)?;
        let subject = arg_str(subject, "subject")?.to_string();
        if count == 0 {
            return Ok(0);
        }
        let n = count as usize;
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            let s = *subjects.add(i);
            let p = *payloads.add(i);
            let len = *payload_lens.add(i) as usize;
            let subject_i = arg_str(s, "row subject")?.to_string();
            let payload_i = if p.is_null() || len == 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(p, len).to_vec()
            };
            rows.push(copyto::CopyRow { subject: subject_i, payload: payload_i });
        }
        let params = copyto::CopyParams {
            url, stream, auth, subject, create_stream: create_stream != 0, tls: tls_cfg,
        };
        runtime().block_on(async move {
            tokio::time::timeout(COPY_BATCH_TIMEOUT, copyto::copy(params, rows))
                .await
                .map_err(|_| format!("copy timed out after {}s", COPY_BATCH_TIMEOUT.as_secs()))?
        })
    };
    match ffi::catch_panic(run) {
        Ok(published) => {
            if !published_out.is_null() {
                *published_out = published;
            }
            0
        }
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

/// core NATS 单条发布（非 JetStream，core-publish 计划 §2.2）。secret-driven：
/// url/认证/TLS 均由 C++ 侧从命中的 secret 解析后传入。confirm=1 时发布后
/// flush（标量粒度）。返回码：0 成功；1 错误或 panic 转译（*err_out 需
/// nats_rw_free_cstring 释放）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_publish(
    url: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
    subject: *const c_char,
    payload: *const u8,
    payload_len: u64,
    confirm: i32,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<(), String> {
        if SHUTDOWN_DONE.load(Ordering::Acquire) {
            return Err("nats_rw runtime already shut down (process exiting)".to_string());
        }
        let url = arg_str(url, "url")?.to_string();
        let auth = auth_from_ffi(user, password, token, nkey, creds)?;
        let tls_cfg = tls_from_ffi(tls_root_cert, tls_required, tls_client_cert, tls_client_key)?;
        let subject = arg_str(subject, "subject")?.to_string();
        let payload = if payload.is_null() || payload_len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(payload, payload_len as usize).to_vec()
        };
        let row = publish::PublishRow { subject, payload };
        runtime().block_on(publish::publish_one(&url, &auth, &tls_cfg, row, confirm != 0))
    };
    match ffi::catch_panic(run) {
        Ok(()) => 0,
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

/// core NATS 批量发布（一批一连接；confirm=1 时批末一次 flush——批量粒度，
/// PONG 覆盖此前全部 PUB）。返回码：0 成功（*published_out 为成功条数）；
/// 1 错误或 panic 转译（此前已发布的条数已计入统计）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_publish_batch(
    url: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
    subjects: *const *const c_char,
    payloads: *const *const u8,
    payload_lens: *const u64,
    count: u64,
    confirm: i32,
    published_out: *mut u64,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<u64, String> {
        if SHUTDOWN_DONE.load(Ordering::Acquire) {
            return Err("nats_rw runtime already shut down (process exiting)".to_string());
        }
        let url = arg_str(url, "url")?.to_string();
        let auth = auth_from_ffi(user, password, token, nkey, creds)?;
        let tls_cfg = tls_from_ffi(tls_root_cert, tls_required, tls_client_cert, tls_client_key)?;
        if count == 0 {
            return Ok(0);
        }
        let n = count as usize;
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            let s = *subjects.add(i);
            let p = *payloads.add(i);
            let len = *payload_lens.add(i) as usize;
            let subject_i = arg_str(s, "row subject")?.to_string();
            let payload_i = if p.is_null() || len == 0 { Vec::new() } else { std::slice::from_raw_parts(p, len).to_vec() };
            rows.push(publish::PublishRow { subject: subject_i, payload: payload_i });
        }
        runtime().block_on(publish::publish_batch(&url, &auth, &tls_cfg, rows, confirm != 0))
    };
    match ffi::catch_panic(run) {
        Ok(published) => {
            if !published_out.is_null() {
                *published_out = published;
            }
            0
        }
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

/// 发布统计查询（单行返回）。url_filter 为空 = 跨 url 聚合（url 列 "<all>"）；
/// 非空 = 该 url 的计数（无记录则零值行）。reset=1 读后原子清零。
/// last_error 为空串表示无错误；last_error_at_ms 为 -1 表示无记录。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_publish_stats(
    url_filter: *const c_char,
    reset: i32,
    out_url: *mut *mut c_char,
    out_published: *mut i64,
    out_errors: *mut i64,
    out_last_error: *mut *mut c_char,
    out_last_error_at_ms: *mut i64,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<(), String> {
        let filter = if url_filter.is_null() { "" } else { CStr::from_ptr(url_filter).to_str().map_err(|_| "invalid utf-8 in url filter".to_string())? };
        let row = publish::stats_query(filter, reset != 0);
        if !out_url.is_null() {
            *out_url = CString::new(row.url).map_err(|_| "url contains NUL".to_string())?.into_raw();
        }
        if !out_published.is_null() {
            *out_published = row.published;
        }
        if !out_errors.is_null() {
            *out_errors = row.errors;
        }
        if !out_last_error.is_null() {
            *out_last_error = CString::new(row.last_error.unwrap_or_default())
                .map_err(|_| "last_error contains NUL".to_string())?
                .into_raw();
        }
        if !out_last_error_at_ms.is_null() {
            *out_last_error_at_ms = row.last_error_at_ms.unwrap_or(-1);
        }
        Ok(())
    };
    match ffi::catch_panic(run) {
        Ok(()) => 0,
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

/// 5 个可空认证字段 → AuthMethod（FFI 共享约定；null 或空串 = 未设置）。
unsafe fn auth_from_ffi(
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
) -> Result<auth::AuthMethod, String> {
    fn opt<'a>(p: *const c_char, name: &str) -> Result<Option<&'a str>, String> {
        if p.is_null() {
            return Ok(None);
        }
        // safety: 调用方（同为 unsafe fn）保证指针有效
        unsafe { Ok(Some(arg_str(p, name)?)) }
    }
    auth::AuthMethod::from_fields(
        opt(user, "user")?,
        opt(password, "password")?,
        opt(token, "token")?,
        opt(nkey, "nkey")?,
        opt(creds, "creds")?,
    )
}

// ---------------------------------------------------------------------------
// FFI: 元数据（阶段四）
// ---------------------------------------------------------------------------

/// 列出全部流摘要。返回码与内存约定同 scan（0 成功，*out 需 nats_rw_streams_free）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_execute(
    url: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
    out: *mut *mut std::ffi::c_void,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<Box<meta::StreamsData>, String> {
        if SHUTDOWN_DONE.load(Ordering::Acquire) {
            return Err("nats_rw runtime already shut down (process exiting)".to_string());
        }
        let url = arg_str(url, "url")?.to_string();
        let auth = auth_from_ffi(user, password, token, nkey, creds)?;
        let tls_cfg = tls_from_ffi(tls_root_cert, tls_required, tls_client_cert, tls_client_key)?;
        runtime().block_on(meta::list_streams(&url, &auth, &tls_cfg))
    };
    match ffi::catch_panic(run) {
        Ok(data) => {
            *out = Box::into_raw(data) as *mut std::ffi::c_void;
            0
        }
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_len(r: *const std::ffi::c_void) -> usize {
    (r as *const meta::StreamsData).as_ref().map(|d| d.streams.len()).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_name(r: *const std::ffi::c_void, i: usize) -> *const c_char {
    (r as *const meta::StreamsData)
        .as_ref()
        .and_then(|d| d.streams.get(i))
        .map(|s| s.name.as_ptr())
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_messages(r: *const std::ffi::c_void, i: usize) -> u64 {
    (r as *const meta::StreamsData)
        .as_ref()
        .and_then(|d| d.streams.get(i))
        .map(|s| s.messages)
        .unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_bytes(r: *const std::ffi::c_void, i: usize) -> u64 {
    (r as *const meta::StreamsData)
        .as_ref()
        .and_then(|d| d.streams.get(i))
        .map(|s| s.bytes)
        .unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_last_seq(r: *const std::ffi::c_void, i: usize) -> u64 {
    (r as *const meta::StreamsData)
        .as_ref()
        .and_then(|d| d.streams.get(i))
        .map(|s| s.last_seq)
        .unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_streams_free(r: *mut std::ffi::c_void) {
    if !r.is_null() {
        drop(Box::from_raw(r as *mut meta::StreamsData));
    }
}

/// 单流详情。返回码与内存约定同上（*out 需 nats_rw_stream_info_free）。
#[no_mangle]
pub unsafe extern "C" fn nats_rw_stream_info_execute(
    url: *const c_char,
    stream: *const c_char,
    user: *const c_char,
    password: *const c_char,
    token: *const c_char,
    nkey: *const c_char,
    creds: *const c_char,
    tls_root_cert: *const c_char,
    tls_required: i32,
    tls_client_cert: *const c_char,
    tls_client_key: *const c_char,
    out: *mut *mut std::ffi::c_void,
    err_out: *mut *mut c_char,
) -> i32 {
    let run = || -> Result<Box<meta::StreamInfoData>, String> {
        if SHUTDOWN_DONE.load(Ordering::Acquire) {
            return Err("nats_rw runtime already shut down (process exiting)".to_string());
        }
        let url = arg_str(url, "url")?.to_string();
        let stream = arg_str(stream, "stream")?.to_string();
        let auth = auth_from_ffi(user, password, token, nkey, creds)?;
        let tls_cfg = tls_from_ffi(tls_root_cert, tls_required, tls_client_cert, tls_client_key)?;
        runtime().block_on(meta::stream_info(&url, &auth, &tls_cfg, &stream))
    };
    match ffi::catch_panic(run) {
        Ok(data) => {
            *out = Box::into_raw(data) as *mut std::ffi::c_void;
            0
        }
        Err(msg) => {
            if let Ok(c) = CString::new(msg) {
                *err_out = c.into_raw();
            }
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_name(r: *const std::ffi::c_void) -> *const c_char {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.name.as_ptr()).unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_subject_count(r: *const std::ffi::c_void) -> u32 {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.subjects.len() as u32).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_subject(r: *const std::ffi::c_void, i: u32) -> *const c_char {
    (r as *const meta::StreamInfoData)
        .as_ref()
        .and_then(|d| d.subjects.get(i as usize))
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_messages(r: *const std::ffi::c_void) -> u64 {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.messages).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_bytes(r: *const std::ffi::c_void) -> u64 {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.bytes).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_first_seq(r: *const std::ffi::c_void) -> u64 {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.first_seq).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_last_seq(r: *const std::ffi::c_void) -> u64 {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.last_seq).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_info_consumer_count(r: *const std::ffi::c_void) -> u64 {
    (r as *const meta::StreamInfoData).as_ref().map(|d| d.consumer_count).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_stream_info_free(r: *mut std::ffi::c_void) {
    if !r.is_null() {
        drop(Box::from_raw(r as *mut meta::StreamInfoData));
    }
}

#[no_mangle]
pub unsafe extern "C" fn nats_rw_free_cstring(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_nul_terminated_and_matches_cargo_version() {
        assert!(VERSION_C.ends_with('\0'));
        assert_eq!(VERSION_C.trim_end_matches('\0'), env!("CARGO_PKG_VERSION"));
    }

    /// fix-plan §六：shutdown 幂等性——连续调用两次不 panic。
    /// 本用例会真实停机 runtime；其余用例均不触 runtime，顺序无关。
    #[test]
    fn runtime_shutdown_is_idempotent() {
        runtime();
        nats_rw_runtime_shutdown();
        nats_rw_runtime_shutdown();
    }
}
