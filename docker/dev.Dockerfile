# nats_rw 构建容器（minis Docker 模式，计划五章）
# 基线 rust:1.90-bookworm：glibc 2.36，兼容 minis（Ubuntu 24.04, glibc 2.39）
FROM rust:1.90-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# cbindgen CLI：供 rust/regen-header.sh 重新生成 C 头文件
RUN cargo install --locked cbindgen

WORKDIR /workspace
