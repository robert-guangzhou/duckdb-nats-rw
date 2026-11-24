#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>

namespace duckdb {

class NatsRwSecret {
public:
	// 解析后的连接设置（secret 名 + 认证字段 + 可选 URL 覆盖）
	struct Settings {
		std::string secret_name;
		std::string url; // secret 携带的 URL（可为空）
		std::string user;
		std::string password;
		std::string token;
		std::string nkey;
		std::string creds;
		// TLS 四字段（core-publish §2.6；全空 = 不配置 TLS 选项）
		std::string tls_root_cert;
		bool tls_required = false;
		std::string tls_client_cert;
		std::string tls_client_key;
	};

	static void Register(ExtensionLoader &loader);
	// name 非空：按名精确查找（找不到报 Binder 错误）；name 为空：按 URL scope 最佳匹配；
	// 两者皆无命中：返回空 Settings（无 secret，匿名连接）。
	static Settings Lookup(ClientContext &context, const std::string &name, const std::string &url);
	// publish 的默认 secret 解析（core-publish §2.4-2）：取无 SCOPE 的 nats_rw secret
	// （scope-less = 默认，DuckDB 通行模式）；无任何 nats_rw secret 返回空 Settings。
	static Settings LookupDefault(ClientContext &context);
};

} // namespace duckdb
