#!/usr/bin/env bash
# 重新生成 src/include/nats_rw_rust.h（需 cbindgen CLI，dev.Dockerfile 已安装）。
# Rust 侧 C ABI 变更后运行本脚本并提交生成的头文件。
set -euo pipefail
cd "$(dirname "$0")"
cbindgen --config cbindgen.toml --output ../src/include/nats_rw_rust.h
# 容器内 cbindgen 不输出 extern "C" 守卫（开守卫由 cbindgen.toml 的
# after_includes 注入）；此处补闭守卫，插在 include guard #endif 之前——
# cbindgen 的 trailer 选项会落在 guard 之外，头文件被重复包含时不平衡（2026-10-26 实测）。
python3 - <<'PY'
from pathlib import Path

p = Path("../src/include/nats_rw_rust.h")
text = p.read_text()
guard_close = '#endif  /* NATS_RW_RUST_H */'
closing = '#ifdef __cplusplus\n} /* extern "C" */\n#endif\n'
assert text.count(guard_close) == 1, "include guard close not found exactly once"
assert 'extern "C"' in text, "after_includes guard missing"
assert text.count('} /* extern "C" */') == 0, "closing guard already present"
p.write_text(text.replace(guard_close, closing + guard_close))
print("extern \"C\" closing guard inserted before include guard #endif")
PY
