//! nats_rw Rust 核心：DuckDB 扩展的 NATS JetStream 读/写实现。
//!
//! C ABI 面仅限本 crate 中 `#[no_mangle] pub extern "C"` 导出的函数；
//! 头文件由 cbindgen 生成（`rust/regen-header.sh`），提交入库以保证构建确定性。

use std::ffi::c_char;

/// Rust 核心版本（编译期取自 Cargo.toml），含结尾 NUL。
const VERSION_C: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// 返回 Rust 核心版本字符串（NUL 结尾）。
///
/// # FFI 约定
/// 返回指向进程生命周期有效的静态字符串的指针；永不失败。
#[no_mangle]
pub extern "C" fn nats_rw_core_version() -> *const c_char {
    VERSION_C.as_ptr() as *const c_char
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
