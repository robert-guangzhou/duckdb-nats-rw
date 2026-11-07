cmake_minimum_required(VERSION 3.5)

# ---- Rust 核心接线（蓝本：ywelsch/duckdb-prql，其平台选择逻辑源自
#      rustyconover/duckdb-lindel-extension）----
set(CORROSION_VERBOSE_OUTPUT ON)
set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 显式检测构建主机架构（优先于模板的 OS_NAME/OS_ARCH 检测）
# 修复：模板检测在 OS_NAME 未设置时默认 x86_64，ARM64 环境下产生错误 target
if(UNIX AND NOT APPLE AND NOT EMSCRIPTEN AND NOT Rust_CARGO_TARGET)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64")
        set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
        set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
    endif()
    if(Rust_CARGO_TARGET)
        message(STATUS "nats_rw: explicit Rust target (host arch): ${Rust_CARGO_TARGET}")
    endif()
endif()

execute_process(
    COMMAND rustup target list --installed
    OUTPUT_VARIABLE RUST_TARGETS
)

# Propagate arch to Rust build for CI
set(Rust_CARGO_TARGET "")
if("${OS_NAME}" STREQUAL "linux")
    if ("${OS_ARCH}" STREQUAL "arm64")
        set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
    elseif("${CMAKE_CXX_COMPILER}" MATCHES "aarch64")
        set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
    else()
        string(FIND "${RUST_TARGETS}" "musl" MUSL_TARGET_FOUND)
        if(NOT MUSL_TARGET_FOUND EQUAL -1)
            set(Rust_CARGO_TARGET "x86_64-unknown-linux-musl")
        else()
            set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
        endif()
    endif()
elseif("${OS_NAME}" STREQUAL "osx")
    if ("${OSX_BUILD_ARCH}" STREQUAL "arm64")
        set(Rust_CARGO_TARGET "aarch64-apple-darwin")
    elseif("${OSX_BUILD_ARCH}" STREQUAL "x86_64")
        set(Rust_CARGO_TARGET "x86_64-apple-darwin")
    elseif("${OS_ARCH}" STREQUAL "arm64")
        set(Rust_CARGO_TARGET "aarch64-apple-darwin")
    endif()
elseif(WIN32)
   if (MINGW AND "${OS_ARCH}" STREQUAL "arm64")
       set(Rust_CARGO_TARGET "aarch64-pc-windows-gnu")
   elseif (MINGW AND "${OS_ARCH}" STREQUAL "amd64")
       set(Rust_CARGO_TARGET "x86_64-pc-windows-gnu")
   elseif (MSVC AND "${OS_ARCH}" STREQUAL "arm64")
       set(Rust_CARGO_TARGET "aarch64-pc-windows-msvc")
   elseif (MSVC AND "${OS_ARCH}" STREQUAL "amd64")
       set(Rust_CARGO_TARGET "x86_64-pc-windows-msvc")
   endif()
endif()

# We currently only support the predefined targets.
if("${Rust_CARGO_TARGET}" STREQUAL "")
  message(FATAL_ERROR "Failed to detect the correct platform")
endif()
message(STATUS "Rust_CARGO_TARGET: ${Rust_CARGO_TARGET}")

set(TARGET_NAME nats_rw)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

if(EMSCRIPTEN)
    set(Rust_CARGO_TARGET "wasm32-unknown-emscripten")
endif()

# CI 环境下 vcpkg toolchain 可能干扰 corrosion 的自动检测——显式钉死 Linux target
# （PR #3442 排查：CI 的 CMAKE_TOOLCHAIN_FILE 设置 CMAKE_SYSTEM_NAME 后
#  corrosion 的平台推断可能失败，导致 cargo 用错误目标编译）
if(NOT Rust_CARGO_TARGET AND UNIX AND NOT APPLE AND NOT EMSCRIPTEN)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64")
        set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
    else()
        set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
    endif()
    message(STATUS "nats_rw: explicit Rust target (CI safety): ${Rust_CARGO_TARGET}")
endif()

add_subdirectory(corrosion)

corrosion_import_crate(
  MANIFEST_PATH
  rust/Cargo.toml
  CRATES
  nats_rw
  CRATE_TYPES
  staticlib
  FLAGS
  --crate-type=staticlib)

set(EXTENSION_SOURCES src/nats_rw_extension.cpp src/nats_rw_scan.cpp src/nats_rw_copyto.cpp src/nats_rw_secret.cpp src/nats_rw_meta.cpp)

# FFI 头文件滞后检测（2026-10-27 阶段四事故根治——"头文件没再生"）
# lib.rs 或 cbindgen.toml 比 nats_rw_rust.h 新 → 编译前 FATAL_ERROR
# 防止改了 Rust FFI 签名但忘跑 regen-header.sh，到 C++ 编译才报"too many arguments"
add_custom_target(nats_rw_header_check
    COMMAND ${CMAKE_COMMAND}
        -DBASE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_header_fresh.cmake
    COMMENT "nats_rw: checking FFI header freshness (lib.rs vs nats_rw_rust.h)"
    VERBATIM)

# Windows 上 static 和 loadable 都生成 .lib，会冲突——只构建 loadable
if(NOT WIN32)
    build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
endif()
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# 头文件检测先于扩展编译（两种目标都依赖）
add_dependencies(${EXTENSION_NAME} nats_rw_header_check)
add_dependencies(${LOADABLE_EXTENSION_NAME} nats_rw_header_check)

# staticlib 自含 Rust std 与全部依赖：链接进静态与可加载两种目标，产物单文件自足
target_link_libraries(${EXTENSION_NAME} nats_rw-static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} nats_rw-static)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
