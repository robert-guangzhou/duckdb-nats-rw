//! nats_rw Rust 核心：DuckDB 扩展的 NATS JetStream 读/写实现。
//!
//! C ABI 面仅限本 crate 中 `#[no_mangle] pub extern "C"` 导出的函数；
//! 头文件由 cbindgen 生成（`rust/regen-header.sh`），提交入库以保证构建确定性。
//! 所有进入 Rust 的 FFI 调用都经 `ffi::catch_panic` 包裹，panic 不允许跨 FFI 边界。

mod copyto;
mod ffi;
mod scan;
mod subject_match;

use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use scan::{ScanData, ScanParams};

/// Rust 核心版本（编译期取自 Cargo.toml），含结尾 NUL。
const VERSION_C: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// COPY 整批写入的操作级超时上限（单条 publish-ack 超时在 copyto 模块）。
const COPY_BATCH_TIMEOUT: Duration = Duration::from_secs(300);

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
    subject: *const c_char,
    start_seq: u64,
    end_seq: u64,
    start_time_nanos: i64,
    end_time_nanos: i64,
    concurrency: u32,
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
        let subject = if subject.is_null() { None } else { Some(arg_str(subject, "subject")?.to_string()) };
        let params = ScanParams {
            stream,
            url,
            subject,
            start_seq,
            end_seq,
            start_time_nanos: if start_time_nanos == i64::MIN { None } else { Some(start_time_nanos) },
            end_time_nanos: if end_time_nanos == i64::MIN { None } else { Some(end_time_nanos) },
            concurrency: concurrency.clamp(1, 64) as usize,
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
        let params = copyto::CopyParams { url, stream, subject, create_stream: create_stream != 0 };
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
