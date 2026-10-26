# 构建布局笔记（阶段一产出）

> 对应 Dev Plan 三/四章。记录脚手架采用的技术路线、蓝本来源与预研中推翻的假设。

## 1. 技术路线：Corrosion + Rust staticlib + C++ 胶水

- **蓝本**：`ywelsch/duckdb-prql`（社区注册表已收录，`requires_toolchains: "rust"`；作者为社区扩展机制维护者之一）。其 CMake 平台选择逻辑注明源自 `rustyconover/duckdb-lindel-extension`。
- 接线方式（本仓库 CMakeLists.txt 同构）：
  - `corrosion` 以 **vendor 普通目录**形式内嵌（v0.6.1，2026-10-26 经 api.github.com tarball 获取并剥离 .github；上游 https://github.com/corrosion-rs/corrosion），`add_subdirectory(corrosion)`。不采用子模块：本地与 minis 的 github.com:443 均间歇不可达，vendor 使仓库自包含、CI 少一个子模块；
  - `corrosion_import_crate(MANIFEST_PATH rust/Cargo.toml CRATES nats_rw CRATE_TYPES staticlib FLAGS --crate-type=staticlib)`；
  - C++ 胶水 `src/nats_rw_extension.cpp` 同时链接到 `build_static_extension` 与 `build_loadable_extension` 两个目标（链接 `nats_rw-static`）。
- **staticlib（而非 cdylib）链接到可加载扩展**：与 prql 不同（prql 把 cdylib 链给 loadable 目标）。选择 staticlib 使 `.duckdb.extension` 单文件自足，免去随包分发 `libnats_rw.so` 的麻烦；代价是二进制更大。若社区 CI 打包对体积敏感再评估改回 prql 方案。

## 2. C 头文件策略：cbindgen 显式生成 + 提交入库

- `rust/regen-header.sh` 用 cbindgen CLI 生成 `src/include/nats_rw_rust.h`；文件**提交入库**。
- 原因：避免「cargo build.rs 生成头文件 vs C++ 编译依赖头文件」的构建顺序竞态；CI 侧无需安装 cbindgen。Rust C ABI 变更后运行脚本重新生成并提交。
- dev.Dockerfile 预装 cbindgen CLI 供容器内重生成。

## 3. 预研中推翻/修正的假设（重要）

| 原假设 | 实际情况（已核实） | 影响 |
|--------|------------------|------|
| nats_js 是「Rust + C API」注册表先例（其 README 自称 Written in Rust） | **其 src 为纯 C++**（仅 .cpp/.hpp，无 Cargo.toml/rust 目录） | Rust 接线蓝本改用 prql（见上）；对 nats_js 的文档宣称保持警惕 |
| crypto 是 Rust 扩展（官方 FAQ 举例） | 其注册表描述符确有 `requires_toolchains: rust`，但 **main 分支现为纯 C++**（Rust 版可能仅在钉死的旧 ref 上） | 同上，蓝本仍用 prql |
| tributary 开源版含 COPY 写回（其 README 有示例） | **main 分支仅注册 `tributary_metadata` / `tributary_version`，无任何 producer/COPY**；与官方产品页「当前版本无写回」一致 | 差异化判断成立：NATS 读写组合仍无竞品 |
| 计划依赖需要 cargo-make | 本路线（extension-ci-tools Makefile + Corrosion）用 GNU make，无需 cargo-make | 依赖清单修正 |

## 4. 注册表发布要点（阶段五用）

- description.yml 需加 `requires_toolchains: rust`（crypto/prql/evalexpr_rhai 同款字段）。
- 描述符模板见 `extensions/nats_js/description.yml`、`extensions/tributary/description.yml`（本工作区 `reference/` 有克隆）。

## 5. 构建入口（minis Docker 模式）

- 容器：`docker/dev.Dockerfile`（rust:1.90-bookworm + cmake/ninja/build-essential/git + cbindgen）。
- `docker/docker-compose.yml`：`nats`（nats:2.11 JetStream）+ `builder`（源码挂载 /workspace，cargo 缓存命名卷）+ `seeder`（synadia/nats-box，profile=seed）。
- 容器内构建：`make debug`（首跑含 DuckDB 主仓全量编译）；测试 `make test`。

## 6. 参考克隆位置（本工作区）

- `reference/prql`（API 读取的蓝本文件见 `reference/blueprints/prql-CMakeLists.txt`）
- `reference/nats_js`、`reference/tributary`、`reference/crypto`（对照用）
- 注：prql/crypto 未能整仓克隆（github.com:443 间歇失联），关键文件经 api.github.com 获取。
