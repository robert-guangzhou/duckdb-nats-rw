#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class NatsRwScanFunction {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
