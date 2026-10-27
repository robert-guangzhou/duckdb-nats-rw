cmake_minimum_required(VERSION 3.5)

# ---- Rust 核心接线（蓝本：ywelsch/duckdb-prql，其平台选择逻辑源自
#      rustyconover/duckdb-lindel-extension）----
set(CORROSION_VERBOSE_OUTPUT ON)
set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

set(EXTENSION_SOURCES src/nats_rw_extension.cpp src/nats_rw_scan.cpp src/nats_rw_copyto.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# staticlib 自含 Rust std 与全部依赖：链接进静态与可加载两种目标，产物单文件自足
target_link_libraries(${EXTENSION_NAME} nats_rw-static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} nats_rw-static)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
