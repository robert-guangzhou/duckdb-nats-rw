#!/usr/bin/env bash
# 重新生成 src/include/nats_rw_rust.h（需 cbindgen CLI，dev.Dockerfile 已安装）。
# Rust 侧 C ABI 变更后运行本脚本并提交生成的头文件。
set -euo pipefail
cd "$(dirname "$0")"
cbindgen --config cbindgen.toml --output ../src/include/nats_rw_rust.h
