//! 共享认证连接（阶段四）：把 FFI 传入的可空认证字段转换为 async-nats 连接选项。
//! 认证方法互斥（至多一种）——由 C++ secret 层校验，此处防御性再验。

use std::time::Duration;

/// 连接建立的超时上限（阶段二引入，阶段四移至共享模块）。
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub(crate) enum AuthMethod {
    #[default]
    None,
    UserPass {
        user: String,
        password: String,
    },
    Token(String),
    Nkey(String),
    CredsFile(String),
}

impl AuthMethod {
    /// 从 5 个可空字段构建（FFI 约定：null = 未设置）。多于一种时返回错误。
    pub(crate) fn from_fields(
        user: Option<&str>,
        password: Option<&str>,
        token: Option<&str>,
        nkey: Option<&str>,
        creds: Option<&str>,
    ) -> Result<Self, String> {
        let user_pass = match (user, password) {
            (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => Some((u, p)),
            (None, None) => None,
            (u, p) => {
                let u = u.is_some_and(|s| !s.is_empty());
                let p = p.is_some_and(|s| !s.is_empty());
                if u || p {
                    return Err("USER and PASSWORD must be set together in the nats_rw secret".into());
                }
                None
            }
        };
        let count = user_pass.is_some() as u8
            + token.is_some_and(|s| !s.is_empty()) as u8
            + nkey.is_some_and(|s| !s.is_empty()) as u8
            + creds.is_some_and(|s| !s.is_empty()) as u8;
        if count > 1 {
            return Err("nats_rw secret takes at most one of USER+PASSWORD, TOKEN, NKEY, CREDS".into());
        }
        if let Some((u, p)) = user_pass {
            return Ok(AuthMethod::UserPass { user: u.into(), password: p.into() });
        }
        if let Some(t) = token.filter(|s| !s.is_empty()) {
            return Ok(AuthMethod::Token(t.into()));
        }
        if let Some(k) = nkey.filter(|s| !s.is_empty()) {
            return Ok(AuthMethod::Nkey(k.into()));
        }
        if let Some(c) = creds.filter(|s| !s.is_empty()) {
            return Ok(AuthMethod::CredsFile(c.into()));
        }
        Ok(AuthMethod::None)
    }
}

fn sanitize_url(url: &str) -> String {
    crate::scan::sanitize_url(url)
}

/// 建立带认证的连接（统一 5s 超时与错误脱敏；scan/copy/meta 共用）。
pub(crate) async fn connect_authenticated(
    url: &str,
    auth: &AuthMethod,
) -> Result<async_nats::Client, String> {
    let connect_error = |detail: String| format!("connect to '{}' failed: {detail}", sanitize_url(url));
    let opts = async_nats::ConnectOptions::new();
    let opts = match auth {
        AuthMethod::None => opts,
        AuthMethod::UserPass { user, password } => opts.user_and_password(user.clone(), password.clone()),
        AuthMethod::Token(t) => opts.token(t.clone()),
        AuthMethod::Nkey(seed) => opts.nkey(seed.clone()),
        AuthMethod::CredsFile(path) => {
            let o = opts
                .credentials_file(path.clone())
                .await
                .map_err(|e| connect_error(format!("reading credentials file: {e}")))?;
            o
        }
    };
    tokio::time::timeout(CONNECT_TIMEOUT, opts.connect(url.to_string()))
        .await
        .map_err(|_| connect_error(format!("timed out after {}s", CONNECT_TIMEOUT.as_secs())))?
        .map_err(|e| connect_error(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_when_all_absent() {
        assert_eq!(AuthMethod::from_fields(None, None, None, None, None), Ok(AuthMethod::None));
        assert_eq!(
            AuthMethod::from_fields(Some(""), Some(""), Some(""), Some(""), Some("")),
            Ok(AuthMethod::None)
        );
    }

    #[test]
    fn single_methods() {
        assert_eq!(
            AuthMethod::from_fields(Some("u"), Some("p"), None, None, None),
            Ok(AuthMethod::UserPass { user: "u".into(), password: "p".into() })
        );
        assert_eq!(
            AuthMethod::from_fields(None, None, Some("t"), None, None),
            Ok(AuthMethod::Token("t".into()))
        );
        assert_eq!(
            AuthMethod::from_fields(None, None, None, Some("k"), None),
            Ok(AuthMethod::Nkey("k".into()))
        );
        assert_eq!(
            AuthMethod::from_fields(None, None, None, None, Some("c")),
            Ok(AuthMethod::CredsFile("c".into()))
        );
    }

    #[test]
    fn rejects_split_user_pass_and_multiple_methods() {
        assert!(AuthMethod::from_fields(Some("u"), None, None, None, None).is_err());
        assert!(AuthMethod::from_fields(None, Some("p"), None, None, None).is_err());
        assert!(AuthMethod::from_fields(Some("u"), Some("p"), Some("t"), None, None).is_err());
        assert!(AuthMethod::from_fields(None, None, Some("t"), Some("k"), None).is_err());
    }
}
