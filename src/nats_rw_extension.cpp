#define DUCKDB_EXTENSION_MAIN

#include "nats_rw_extension.hpp"
#include "nats_rw_rust.h" // cbindgen 生成；变更后运行 rust/regen-header.sh
#include "nats_rw_scan.hpp"
#include "nats_rw_copyto.hpp"
#include "nats_rw_secret.hpp"
#include "nats_rw_meta.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <cstdlib> // std::atexit（退出期 tokio runtime 优雅停机注册）

namespace duckdb {

static string NatsRwCoreVersion() {
	const char *version = nats_rw_core_version();
	if (version == nullptr) {
		return "unknown";
	}
	return string(version);
}

static void NatsRwVersionFun(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value(NatsRwCoreVersion()));
}

static void LoadInternal(ExtensionLoader &loader) {
	// 版本函数（TP-01）
	auto version_function = ScalarFunction("nats_rw_version", {}, LogicalType::VARCHAR, NatsRwVersionFun);
	loader.RegisterFunction(version_function);

	// 读取（TP-02 组）：有界快照扫描
	NatsRwScanFunction::Register(loader);

	// 写入（TP-03 组）：COPY … TO '<stream>' (FORMAT nats_rw, …)
	NatsRwCopyFunction::Register(loader);

	// 认证与元数据（TP-04 组，阶段四）
	NatsRwSecret::Register(loader);
	NatsRwMetaFunctions::Register(loader);
}

void NatsRwExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string NatsRwExtension::Name() {
	return "nats_rw";
}

std::string NatsRwExtension::Version() const {
#ifdef EXT_VERSION_NATS_RW
	return EXT_VERSION_NATS_RW;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(nats_rw, loader) {
	duckdb::LoadInternal(loader);
	// fix-plan 2026-10-26 F2：退出期先于 ASan/LSan 扫描优雅停机 Rust 侧 tokio
	// runtime。atexit 为 LIFO——本注册晚于 ASan 启动期注册，因而先执行；
	// 消除 debug+ASan 构建退出期 DEADLYSIGNAL 递归风暴的触发条件。
	std::atexit(nats_rw_runtime_shutdown);
}

} // extern "C"
