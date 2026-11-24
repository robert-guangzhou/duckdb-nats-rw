//! TLS 连接配置（core-publish 计划 §2.6）：secret 四字段的结构化形态。
//!
//! 全字段缺省 = 不配置任何 TLS 选项（与历史行为完全一致）；`tls://` scheme
//! 由 async-nats 自动启用 TLS（证书链用系统根证书）。`TLS_REQUIRED=true` 时
//! 明文连接直接报错（不降级）。mTLS 需 client 证书+密钥成对出现。

use std::path::PathBuf;

/// TLS 配置（secret 四字段的 Rust 形态；进入连接缓存键，故需 Eq+Hash）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) struct TlsConfig {
    /// 自签 CA bundle 路径（TLS_ROOT_CERT）；空 = 用系统根证书。
    pub(crate) root_cert: String,
    /// 强制 TLS（TLS_REQUIRED，默认 false）；true = 明文连接直接报错。
    pub(crate) required: bool,
    /// mTLS 客户端证书路径（TLS_CLIENT_CERT）；空 = 不提供。
    pub(crate) client_cert: String,
    /// mTLS 客户端私钥路径（TLS_CLIENT_KEY）；必须与 client_cert 成对。
    pub(crate) client_key: String,
}

impl TlsConfig {
    /// 是否完全未配置（四字段全缺省）。
    pub(crate) fn is_unset(&self) -> bool {
        self.root_cert.is_empty() && !self.required && self.client_cert.is_empty() && self.client_key.is_empty()
    }

    /// 校验组合合法性：client 证书与密钥必须成对出现，缺一报错。
    pub(crate) fn validate(&self) -> Result<(), String> {
        match (self.client_cert.is_empty(), self.client_key.is_empty()) {
            (false, false) => {}
            (true, true) => {}
            (false, true) => {
                return Err(
                    "nats_rw secret: TLS_CLIENT_CERT is set but TLS_CLIENT_KEY is missing — they must be provided as a pair".into(),
                )
            }
            (true, false) => {
                return Err(
                    "nats_rw secret: TLS_CLIENT_KEY is set but TLS_CLIENT_CERT is missing — they must be provided as a pair".into(),
                )
            }
        }
        Ok(())
    }

    /// 应用到连接选项（在认证选项之后叠加）。
    pub(crate) fn apply(
        &self,
        opts: async_nats::ConnectOptions,
    ) -> Result<async_nats::ConnectOptions, String> {
        let opts = if self.root_cert.is_empty() {
            opts
        } else {
            opts.add_root_certificates(PathBuf::from(&self.root_cert))
        };
        let opts = if self.client_cert.is_empty() {
            opts
        } else {
            opts.add_client_certificate(
                PathBuf::from(&self.client_cert),
                PathBuf::from(&self.client_key),
            )
        };
        Ok(if self.required { opts.require_tls(true) } else { opts })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_unset_and_valid() {
        let t = TlsConfig::default();
        assert!(t.is_unset());
        assert!(t.validate().is_ok());
    }

    #[test]
    fn cert_without_key_rejected() {
        let t = TlsConfig { client_cert: "c.pem".into(), ..Default::default() };
        let err = t.validate().unwrap_err();
        assert!(err.contains("TLS_CLIENT_KEY"), "unexpected: {err}");
    }

    #[test]
    fn key_without_cert_rejected() {
        let t = TlsConfig { client_key: "k.pem".into(), ..Default::default() };
        let err = t.validate().unwrap_err();
        assert!(err.contains("TLS_CLIENT_CERT"), "unexpected: {err}");
    }

    #[test]
    fn paired_cert_key_accepted() {
        let t = TlsConfig {
            client_cert: "c.pem".into(),
            client_key: "k.pem".into(),
            ..Default::default()
        };
        assert!(t.validate().is_ok());
        assert!(!t.is_unset());
    }

    #[test]
    fn required_alone_is_set_but_valid() {
        let t = TlsConfig { required: true, ..Default::default() };
        assert!(!t.is_unset());
        assert!(t.validate().is_ok());
    }

    #[test]
    fn hash_equality_for_cache_key() {
        let a = TlsConfig { root_cert: "ca.pem".into(), required: true, ..Default::default() };
        let b = a.clone();
        let c = TlsConfig { root_cert: "other.pem".into(), ..Default::default() };
        assert_eq!(a, b);
        assert_ne!(a, c);
        use std::collections::HashMap;
        let mut m = HashMap::new();
        m.insert(a, 1);
        assert_eq!(m.get(&b), Some(&1));
    }
}
