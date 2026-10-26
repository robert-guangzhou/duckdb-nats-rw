//! nats_rw Rust 核心：DuckDB 扩展的 NATS JetStream 读/写实现。
//!
//! C ABI 面仅限本 crate 中 `#[no_mangle] pub extern "C"` 导出的函数；
//! 头文件由 cbindgen 生成（`rust/regen-header.sh`），提交入库以保证构建确定性。
//! 所有进入 Rust 的 FFI 调用都经 `ffi::catch_panic` 包裹，panic 不允许跨 FFI 边界。

mod ffi;
mod scan;
mod subject_match;

use std::ffi::{c_char, CStr, CString};
use std::sync::OnceLock;

use scan::{ScanData, ScanParams};

/// Rust 核心版本（编译期取自 Cargo.toml），含结尾 NUL。
const VERSION_C: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// 进程内共享的 tokio 运行时：DuckDB 线程同步 block_on 使用，
/// 不与 DuckDB 线程模型混用。
/// 运行时被有意泄漏（Box::leak）：静态析构阶段 drop Runtime 会在 worker 线程
/// join 时偶发段错误（2026-10-26 实测：进程退出期 5 次 2 次崩溃）；
/// 进程级泄漏 2 个线程的资源是可接受的固定代价。
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<&'static tokio::runtime::Runtime> = OnceLock::new();
    *RT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to initialize tokio runtime");
        Box::leak(Box::new(rt))
    })
}

/// 返回 Rust 核心版本字符串（NUL 结尾）。
#[no_mangle]
pub extern "C" fn nats_rw_core_version() -> *const c_char {
    VERSION_C.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// FFI: 有界快照扫描
// 约定：成功返回 0；错误返回 1 且 *err_out 指向调用方需用
// nats_rw_free_cstring 释放的错误串；panic 返回 2。
// ---------------------------------------------------------------------------

unsafe fn arg_str<'a>(p: *const c_char, name: &str) -> Result<&'a str, String> {
    if p.is_null() {
        return Err(format!("argument '{name}' must not be null"));
    }
    CStr::from_ptr(p).to_str().map_err(|_| format!("argument '{name}' is not valid UTF-8"))
}

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
}
