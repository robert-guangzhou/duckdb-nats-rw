#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class NatsRwCopyFunction {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
