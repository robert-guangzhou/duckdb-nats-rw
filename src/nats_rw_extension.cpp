#define DUCKDB_EXTENSION_MAIN

#include "nats_rw_extension.hpp"
#include "nats_rw_rust.h" // cbindgen 生成；变更后运行 rust/regen-header.sh

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

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
	// 阶段二起在此注册 nats_rw_scan、COPY 替身、secrets 与元数据函数
	auto version_function = ScalarFunction("nats_rw_version", {}, LogicalType::VARCHAR, NatsRwVersionFun);
	loader.RegisterFunction(version_function);
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
}

} // extern "C"
