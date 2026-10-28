//! 连接缓存（阶段一性能优化）：进程级 `(url, auth)` 键控 NATS 连接复用。
//!
//! 设计：全局 `RwLock<HashMap<ConnKey, Arc<Client>>>`，读锁查缓存（快路径），
//! 未命中时写锁二次检查后新建。连接失败不缓存（下次重试）。
//! 缓存条目永不过期——async-nats Client 内置心跳重连，无需主动失效。
//! atexit 停机时缓存随进程退出自然释放（runtime shutdown 已处理）。

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::auth::{connect_authenticated, AuthMethod, CONNECT_TIMEOUT};

/// 缓存键：URL + 认证方法的组合唯一确定一条连接。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnKey {
    url: String,
    auth: AuthMethod,
}

static CACHE: OnceLock<RwLock<HashMap<ConnKey, Arc<async_nats::Client>>>> = OnceLock::new();

fn cache() -> &'static RwLock<HashMap<ConnKey, Arc<async_nats::Client>>> {
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 带缓存的认证连接：同 `(url, auth)` 复用已有连接，否则新建并缓存。
/// 连接失败不缓存（下次调用重试）。
pub(crate) async fn connect_cached(
    url: &str,
    auth: &AuthMethod,
) -> Result<Arc<async_nats::Client>, String> {
    let key = ConnKey {
        url: url.to_string(),
        auth: auth.clone(),
    };

    // 快路径：读锁查缓存
    if let Ok(map) = cache().read() {
        if let Some(client) = map.get(&key) {
            return Ok(Arc::clone(client));
        }
    }

    // 慢路径：新建连接（不持锁，避免长时间阻塞其他读者）
    let client = connect_authenticated(url, auth).await?;

    // 写锁存入（二次检查：并发场景下另一线程可能已建好）
    let client = Arc::new(client);
    if let Ok(mut map) = cache().write() {
        match map.get(&key) {
            Some(existing) => {
                // 另一线程先到——用它的，丢弃我们刚建的
                return Ok(Arc::clone(existing));
            }
            None => {
                map.insert(key, Arc::clone(&client));
            }
        }
    }

    Ok(client)
}

/// 清空缓存（仅测试用；生产环境由进程退出自然释放）。
#[cfg(test)]
pub(crate) fn clear_cache_for_test() {
    if let Some(c) = CACHE.get() {
        if let Ok(mut map) = c.write() {
            map.clear();
        }
    }
}

/// 缓存条目数（仅测试用）。
#[cfg(test)]
pub(crate) fn cache_size_for_test() -> usize {
    CACHE.get().and_then(|c| c.read().ok()).map(|m| m.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // 注意：CR-01..03 需要真实 NATS 服务器（docker compose 网络），
    // 在容器内 cargo test 时可用。以下为纯逻辑测试：

    #[test]
    fn conn_key_hash_equality() {
        let k1 = ConnKey {
            url: "nats://a:4222".into(),
            auth: AuthMethod::None,
        };
        let k2 = ConnKey {
            url: "nats://a:4222".into(),
            auth: AuthMethod::None,
        };
        let k3 = ConnKey {
            url: "nats://b:4222".into(),
            auth: AuthMethod::None,
        };
        let k4 = ConnKey {
            url: "nats://a:4222".into(),
            auth: AuthMethod::Token("t".into()),
        };
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k1, k4);
        // Hash 一致性（HashMap 可用）
        let mut map = HashMap::new();
        map.insert(k1, 1);
        assert_eq!(map.get(&k2), Some(&1));
        assert_eq!(map.get(&k3), None);
    }

    #[test]
    fn cache_size_starts_empty() {
        clear_cache_for_test();
        assert_eq!(cache_size_for_test(), 0);
    }

    #[test]
    fn connect_timeout_is_reasonable() {
        // 5s 超时是计划规定的值——防止有人误改成 60s 导致 CI 超时
        assert!(CONNECT_TIMEOUT.as_secs() <= 10, "CONNECT_TIMEOUT should be ≤10s");
    }
}
