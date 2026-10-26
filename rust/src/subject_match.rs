//! NATS subject 通配符匹配：`*` 恰好匹配一个 token，`>` 匹配任意剩余 tokens（按惯例为最后一个 token）。

/// 判断 `subject` 是否匹配模式 `pattern`（NATS 通配符语义）。
pub(crate) fn subject_matches(pattern: &str, subject: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let s: Vec<&str> = subject.split('.').collect();
    let mut i = 0;
    while i < p.len() {
        if p[i] == ">" {
            // `>` 匹配一个及以上剩余 tokens（"a.>" 不匹配 "a" 本身）；
            // 忽略 `>` 不在末尾的非法写法，按尾通配处理
            return i < s.len();
        }
        if i >= s.len() {
            return false;
        }
        if p[i] == "*" {
            i += 1;
            continue;
        }
        if p[i] != s[i] {
            return false;
        }
        i += 1;
    }
    i == s.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(subject_matches("a.b.c", "a.b.c"));
        assert!(!subject_matches("a.b.c", "a.b.d"));
    }

    #[test]
    fn star_matches_exactly_one_token() {
        assert!(subject_matches("a.*.c", "a.b.c"));
        assert!(!subject_matches("a.*.c", "a.b.c.d"));
        assert!(!subject_matches("a.*.c", "a.c"));
    }

    #[test]
    fn gt_matches_tail() {
        assert!(subject_matches("a.>", "a.b.c.d"));
        assert!(subject_matches("a.>", "a.b"));
        assert!(!subject_matches("a.>", "b.a"));
        assert!(!subject_matches("a.>", "a"));
    }

    #[test]
    fn seed_patterns() {
        assert!(subject_matches("seed.a.>", "seed.a.1"));
        assert!(!subject_matches("seed.a.>", "seed.b.1"));
        assert!(subject_matches("seed.*.*", "seed.a.1"));
        assert!(!subject_matches("seed.*.*", "seed.a.1.2"));
    }
}
