#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class NatsRwMetaFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
