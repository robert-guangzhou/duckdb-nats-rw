//! FFI panic 边界：所有从 C 进入 Rust 的调用都必须经由 `catch_panic` 包裹，
//! panic 一律转译为 Err(String)，禁止跨 FFI unwind（UB 风险）。

use std::any::Any;

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// 执行 `f`，把 panic 捕获为 Err；错误信息带 `rust panic:` 前缀以便上层归因。
pub(crate) fn catch_panic<T, F>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String> + std::panic::UnwindSafe,
{
    std::panic::catch_unwind(f).unwrap_or_else(|p| Err(format!("rust panic: {}", panic_message(p))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catches_panic_into_err() {
        let r: Result<(), String> = catch_panic(|| panic!("boom"));
        let msg = r.unwrap_err();
        assert!(msg.starts_with("rust panic:"), "unexpected message: {msg}");
        assert!(msg.contains("boom"));
    }

    #[test]
    fn passes_through_ok_and_err() {
        assert_eq!(catch_panic(|| Ok::<_, String>(7)).unwrap(), 7);
        assert_eq!(catch_panic(|| Err::<(), _>("clean error".into())).unwrap_err(), "clean error");
    }
}
