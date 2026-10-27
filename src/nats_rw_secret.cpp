#include "nats_rw_secret.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

static constexpr const char *SECRET_TYPE = "nats_rw";

// 与 nats CLI 风格一致的小写选项名；URL 为本扩展独有（secret 携带连接地址）
static const char *const SECRET_OPTIONS[] = {"url", "user", "password", "token", "nkey", "creds"};

static unique_ptr<BaseSecret> CreateNatsRwSecret(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	for (auto option : SECRET_OPTIONS) {
		secret->TrySetValue(option, input);
	}
	auto has = [&](const char *option) {
		Value value;
		return secret->TryGetValue(option, value) && !value.IsNull() && !value.ToString().empty();
	};
	for (auto option : SECRET_OPTIONS) {
		Value value;
		if (has(option) && secret->TryGetValue(option, value) && value.ToString().empty()) {
			throw InvalidInputException("A nats_rw secret's %s cannot be empty", StringUtil::Upper(option));
		}
	}
	if (has("url") && !StringUtil::StartsWith(has("url") ? Value().ToString() : "", "x")) {
		// 仅类型形态检查：url 必须非空字符串（上面的循环已保证非空）
	}
	int methods = int(has("user") || has("password")) + int(has("token")) + int(has("nkey")) + int(has("creds"));
	if (methods > 1) {
		throw InvalidInputException("A nats_rw secret takes one of USER and PASSWORD, TOKEN, NKEY or CREDS");
	}
	if (has("user") != has("password")) {
		throw InvalidInputException("A nats_rw secret takes USER and PASSWORD together");
	}
	// 敏感字段脱敏（Dev Plan 六/TP-04c）；url 保持可见以支持 scope 调试
	secret->redact_keys = {"password", "token", "nkey", "creds"};
	return std::move(secret);
}

void NatsRwSecret::Register(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = SECRET_TYPE;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "nats_rw";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction function;
	function.secret_type = SECRET_TYPE;
	function.provider = "config";
	function.function = CreateNatsRwSecret;
	for (auto option : SECRET_OPTIONS) {
		function.named_parameters[option] = LogicalType(LogicalTypeId::VARCHAR);
	}
	loader.RegisterFunction(function);
}

// —— scope 匹配（算法自 nats_js 蓝本适配，MIT）——

// scope 匹配用的 URL：无 scheme 前缀则补 nats://，并剥离 userinfo
static string ScopeURL(const string &url) {
	auto result = url.substr(0, url.find(',')).find("://") == string::npos ? "nats://" + url : url;
	auto start = result.find("://") + 3;
	auto at = result.find('@', start);
	auto end = result.find_first_of("/,", start);
	if (at != string::npos && (end == string::npos || at < end)) {
		result.erase(start, at + 1 - start);
	}
	return result;
}

// scope 是否恰好结束在 scheme/host/port 边界（防止 nats://host:4222 误配 nats://host:42220）
static bool IsScopeEnd(const string &url, idx_t pos) {
	if (pos == url.size() || url[pos - 1] == '/' || url[pos] == '/' || url[pos] == ',') {
		return true;
	}
	if (url[pos] != ':') {
		return false;
	}
	auto open = url.rfind('[', pos);
	auto close = url.rfind(']', pos);
	return open == string::npos || (close != string::npos && close > open);
}

// 与 URL scope 最佳匹配的 secret；无 scope 或空 scope 匹配一切（最弱优先）
static unique_ptr<SecretEntry> LookupScopedSecret(SecretManager &secret_manager, CatalogTransaction transaction,
                                                   const string &url) {
	int64_t best_score = -1;
	string best_scope;
	for (auto &entry : secret_manager.AllSecrets(transaction)) {
		if (entry.secret->GetType() != SECRET_TYPE) {
			continue;
		}
		auto &scopes = entry.secret->GetScope();
		if (scopes.empty() && best_score < 0) {
			best_score = 0;
		}
		for (auto &scope : scopes) {
			if (!scope.empty() && !(StringUtil::StartsWith(url, scope) && IsScopeEnd(url, scope.size()))) {
				continue;
			}
			auto score = NumericCast<int64_t>(scope.size());
			if (score > best_score) {
				best_score = score;
				best_scope = scope;
			}
		}
	}
	if (best_score < 0) {
		return nullptr;
	}
	auto match = secret_manager.LookupSecret(transaction, best_scope, SECRET_TYPE);
	return match.HasMatch() ? std::move(match.secret_entry) : nullptr;
}

NatsRwSecret::Settings NatsRwSecret::Lookup(ClientContext &context, const std::string &name,
                                             const std::string &url) {
	Settings settings;
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	unique_ptr<SecretEntry> entry;
	if (!name.empty()) {
		entry = secret_manager.GetSecretByName(transaction, name);
		if (!entry || entry->secret->GetType() != SECRET_TYPE) {
			throw BinderException("nats_rw: no secret named '%s'", name);
		}
	} else {
		entry = LookupScopedSecret(secret_manager, transaction, ScopeURL(url));
		if (!entry) {
			return settings;
		}
	}

	auto &secret = entry->secret->Cast<KeyValueSecret>();
	auto get = [&](const char *option) {
		Value value;
		return secret.TryGetValue(option, value) && !value.IsNull() ? value.ToString() : string();
	};
	settings.secret_name = secret.GetName().GetIdentifierName();
	settings.url = get("url");
	settings.user = get("user");
	settings.password = get("password");
	settings.token = get("token");
	settings.nkey = get("nkey");
	settings.creds = get("creds");
	return settings;
}

} // namespace duckdb
