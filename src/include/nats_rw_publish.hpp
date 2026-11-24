#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// core NATS 发布三函数（nats_rw_publish / _batch / _stats）的注册入口
// （core-publish 计划 §2.2/2.4）。
void RegisterNatsRwPublish(ExtensionLoader &loader);

} // namespace duckdb
