# FFI 头文件滞后检测（2026-10-27 阶段四事故根治）
# rust/src/lib.rs 比 src/include/nats_rw_rust.h 新 → 编译前 FATAL_ERROR
# 防止：改了 Rust FFI 签名但忘跑 regen-header.sh，到 C++ 编译才报
#       "too many arguments" 的晚发现（Dev Plan 六章 + lessons L1 类）
#
# ⚠️ CI 跳过（2026-10-28 PR #3442 事故）：git checkout 逐文件写入，
#    不同目录的文件时间戳几乎必然有微秒级差异，导致误报。
#    本检测仅适用于本地开发环境（开发者手动修改文件后时间戳语义有意义）。

if(DEFINED ENV{CI} AND "$ENV{CI}" STREQUAL "true")
    return()  # GitHub Actions / CI 环境：跳过时间戳比较
endif()

set(RUST_LIB "${BASE_DIR}/rust/src/lib.rs")
set(CBINDGEN_CFG "${BASE_DIR}/rust/cbindgen.toml")
set(FFI_HEADER "${BASE_DIR}/src/include/nats_rw_rust.h")

if(NOT EXISTS "${RUST_LIB}")
    return()  # Rust 源不可达（如 CI 只下载了头文件的场景），跳过
endif()

if(NOT EXISTS "${FFI_HEADER}")
    message(FATAL_ERROR
        "nats_rw: FFI header missing: ${FFI_HEADER}\n"
        "Generate it: run 'rust/regen-header.sh' in the builder container,\n"
        "then commit the result (build determinism by design, see docs/notes-build.md).")
endif()

file(TIMESTAMP "${RUST_LIB}" RUST_TS "%s")
file(TIMESTAMP "${FFI_HEADER}" HEADER_TS "%s")

if(RUST_TS GREATER HEADER_TS)
    message(FATAL_ERROR
        "nats_rw: rust/src/lib.rs (${RUST_TS}) is NEWER than "
        "src/include/nats_rw_rust.h (${HEADER_TS}).\n"
        "FFI signatures may have changed without regenerating the C header.\n"
        "Fix: run 'rust/regen-header.sh' in the builder container,\n"
        "then commit the regenerated header. Do NOT hand-edit nats_rw_rust.h.")
endif()

# cbindgen 配置变更同样影响头文件产出格式
if(EXISTS "${CBINDGEN_CFG}")
    file(TIMESTAMP "${CBINDGEN_CFG}" CFG_TS "%s")
    if(CFG_TS GREATER HEADER_TS)
        message(FATAL_ERROR
            "nats_rw: rust/cbindgen.toml (${CFG_TS}) is NEWER than "
            "src/include/nats_rw_rust.h (${HEADER_TS}).\n"
            "The header generation config changed; regenerate with "
            "'rust/regen-header.sh' in the builder container.")
    endif()
endif()
