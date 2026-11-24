// nats_rw_publish / nats_rw_publish_batch / nats_rw_publish_stats
// core NATS 发布（非 JetStream，core-publish 计划 §2.2）。
//
// secret-driven（§2.4，用户指定：不传 url）：
//   - secret_name 非空 → 具名精确查找（不存在 → Binder 错误）
//   - secret_name 为空 → 默认 secret（无 SCOPE 的 nats_rw secret；§2.4-2）
//   - 命中 secret 的 settings.url 是唯一连接目标；为空 → Binder 错误指路
//   - 无匿名路径（与 scan/cop 的"匿名连默认"刻意不同——审计桥接场景连接信息集中管理）
//
// confirm 粒度（§2.2）：标量 = 单条后 flush；批量 = 批末一次 flush
// （NATS 保序，PONG 覆盖此前全部 PUB，与逐条 PING 等价，一批一次往返）。
//
// payload 为 BLOB：字符串字面量可隐式转换；二进制（\x00、≥0x80）必须走 BLOB
// 通道（TP-08f 数据四类）。

#include "nats_rw_rust.h"
#include "nats_rw_secret.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {

namespace {

struct NatsRwPublishConn {
	std::string url;
	std::string user, password, token, nkey, creds;
	std::string tls_root_cert;
	bool tls_required = false;
	std::string tls_client_cert, tls_client_key;
};

// §2.4 secret-driven 解析（publish 专用；与 scan/cop 的三级回退不同，无匿名路径）
NatsRwPublishConn ResolvePublishConnection(ClientContext &context, const std::string &secret_name) {
	NatsRwSecret::Settings settings = secret_name.empty()
	                                      ? NatsRwSecret::LookupDefault(context)
	                                      : NatsRwSecret::Lookup(context, secret_name, "");
	if (settings.secret_name.empty()) {
		throw BinderException(
		    "nats_rw_publish: no nats_rw secret available — CREATE SECRET (TYPE nats_rw, URL '...', ...) first, "
		    "or pass secret_name of an existing one");
	}
	if (settings.url.empty()) {
		throw BinderException("nats_rw_publish: secret '%s' must carry URL (CREATE SECRET (TYPE nats_rw, URL '...'))",
		                      settings.secret_name);
	}
	NatsRwPublishConn conn;
	conn.url = settings.url;
	conn.user = settings.user;
	conn.password = settings.password;
	conn.token = settings.token;
	conn.nkey = settings.nkey;
	conn.creds = settings.creds;
	conn.tls_root_cert = settings.tls_root_cert;
	conn.tls_required = settings.tls_required;
	conn.tls_client_cert = settings.tls_client_cert;
	conn.tls_client_key = settings.tls_client_key;
	return conn;
}

const char *OrNull(const std::string &s) {
	return s.empty() ? nullptr : s.c_str();
}

void ThrowRustError(const char *err) {
	std::string msg = err == nullptr ? "unknown nats_rw error" : err;
	nats_rw_free_cstring(const_cast<char *>(err));
	throw InvalidInputException("nats_rw: %s", msg);
}

struct NatsRwPublishBindData : public TableFunctionData {
	NatsRwPublishConn conn;
	bool confirm = false;
	std::vector<std::string> subjects;
	std::vector<std::string> payloads;
};

// ---------------- 标量：nats_rw_publish(subject, payload, secret_name, confirm) ----------------

static void NatsRwPublishScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	// 同一 secret_name 只解析一次（catalog 查询较重；行间复用）
	std::string last_secret;
	bool have_conn = false;
	NatsRwPublishConn conn;
	bool confirm = false;
	for (idx_t r = 0; r < count; r++) {
		auto secret_name = args.GetValue(2, r).ToString();
		if (!have_conn || secret_name != last_secret) {
			conn = ResolvePublishConnection(context, secret_name);
			last_secret = secret_name;
			have_conn = true;
		}
		confirm = args.GetValue(3, r).GetValue<bool>();
		auto subject = args.GetValue(0, r).ToString();
		auto payload_value = args.GetValue(1, r);
		std::string payload;
		if (!payload_value.IsNull()) {
			auto blob = payload_value.GetValueUnsafe<string_t>();
			payload.assign(blob.GetData(), blob.GetSize());
		}
		char *err = nullptr;
		int32_t rc = nats_rw_publish(conn.url.c_str(), OrNull(conn.user), OrNull(conn.password), OrNull(conn.token),
		                             OrNull(conn.nkey), OrNull(conn.creds), OrNull(conn.tls_root_cert),
		                             conn.tls_required ? 1 : 0, OrNull(conn.tls_client_cert),
		                             OrNull(conn.tls_client_key), subject.c_str(),
		                             reinterpret_cast<const uint8_t *>(payload.data()), payload.size(),
		                             confirm ? 1 : 0, &err);
		if (rc != 0) {
			if (rc == 1 && err != nullptr) {
				ThrowRustError(err);
			}
			throw InvalidInputException("nats_rw_publish failed with unexpected FFI error (rc=%d)", rc);
		}
		result.SetValue(r, Value::BOOLEAN(true));
	}
}

// ---------------- 表函数：nats_rw_publish_batch(list, secret_name, confirm) ----------------

static unique_ptr<FunctionData> NatsRwPublishBatchBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs.empty()) {
		throw BinderException("nats_rw_publish_batch requires a LIST(STRUCT(subject, payload)) argument");
	}
	auto list_type = input.inputs[0].type();
	if (list_type.id() != LogicalTypeId::LIST) {
		throw BinderException("nats_rw_publish_batch first argument must be a LIST, got %s",
		                      list_type.ToString().c_str());
	}
	auto &child = ListType::GetChildType(list_type);
	if (child.id() != LogicalTypeId::STRUCT) {
		throw BinderException("nats_rw_publish_batch list element must be STRUCT(subject VARCHAR, payload BLOB), got %s",
		                      child.ToString().c_str());
	}
	auto &children = StructType::GetChildTypes(child);
	bool has_subject = false, has_payload = false;
	for (auto &kv : children) {
		if (kv.first == "subject" && kv.second.id() == LogicalTypeId::VARCHAR) {
			has_subject = true;
		}
		if (kv.first == "payload" && kv.second.id() == LogicalTypeId::BLOB) {
			has_payload = true;
		}
	}
	if (!has_subject || !has_payload) {
		throw BinderException(
		    "nats_rw_publish_batch element struct must contain subject VARCHAR and payload BLOB fields");
	}

	auto bind_data = make_uniq<NatsRwPublishBindData>();
	std::string secret_name;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "secret_name") {
			if (kv.second.IsNull()) {
				throw BinderException("nats_rw_publish_batch: secret_name must not be NULL");
			}
			secret_name = kv.second.GetValue<std::string>();
		} else if (kv.first == "confirm") {
			bind_data->confirm = kv.second.GetValue<bool>();
		}
	}
	bind_data->conn = ResolvePublishConnection(context, secret_name);

	// 物化 list → (subject, payload) 对（bind 期一次性展开，执行期单遍发布）
	auto list_value = input.inputs[0];
	if (!list_value.IsNull()) {
		auto &list_entries = ListValue::GetChildren(list_value);
		bind_data->subjects.reserve(list_entries.size());
		bind_data->payloads.reserve(list_entries.size());
		for (auto &entry : list_entries) {
			if (entry.IsNull()) {
				throw BinderException("nats_rw_publish_batch: list elements must not be NULL");
			}
			auto &struct_children = StructValue::GetChildren(entry);
			// 字段按 struct 定义顺序取（bind 已校验名称与类型）
			std::string subject, payload;
			for (idx_t i = 0; i < children.size(); i++) {
				if (children[i].first == "subject") {
					subject = StringValue::Get(struct_children[i]);
				} else if (children[i].first == "payload") {
					if (!struct_children[i].IsNull()) {
						auto blob = struct_children[i].GetValueUnsafe<string_t>();
						payload.assign(blob.GetData(), blob.GetSize());
					}
				}
			}
			bind_data->subjects.push_back(std::move(subject));
			bind_data->payloads.push_back(std::move(payload));
		}
	}

	names.emplace_back("published");
	return_types.emplace_back(LogicalType::BIGINT);
	return std::move(bind_data);
}

struct NatsRwBatchGlobalState : public GlobalTableFunctionState {
	int64_t count = 0;
	bool consumed = false;
};

static unique_ptr<GlobalTableFunctionState>
NatsRwPublishBatchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<NatsRwPublishBindData>();
	auto state = make_uniq<NatsRwBatchGlobalState>();

	std::vector<const char *> subject_ptrs;
	std::vector<const uint8_t *> payload_ptrs;
	std::vector<uint64_t> payload_lens;
	subject_ptrs.reserve(bind_data.subjects.size());
	payload_ptrs.reserve(bind_data.payloads.size());
	payload_lens.reserve(bind_data.payloads.size());
	for (size_t i = 0; i < bind_data.subjects.size(); i++) {
		subject_ptrs.push_back(bind_data.subjects[i].c_str());
		payload_ptrs.push_back(reinterpret_cast<const uint8_t *>(bind_data.payloads[i].data()));
		payload_lens.push_back(bind_data.payloads[i].size());
	}

	uint64_t published = 0;
	char *err = nullptr;
	int32_t rc = nats_rw_publish_batch(
	    bind_data.conn.url.c_str(), OrNull(bind_data.conn.user), OrNull(bind_data.conn.password),
	    OrNull(bind_data.conn.token), OrNull(bind_data.conn.nkey), OrNull(bind_data.conn.creds),
	    OrNull(bind_data.conn.tls_root_cert), bind_data.conn.tls_required ? 1 : 0,
	    OrNull(bind_data.conn.tls_client_cert), OrNull(bind_data.conn.tls_client_key),
	    subject_ptrs.empty() ? nullptr : subject_ptrs.data(), payload_ptrs.empty() ? nullptr : payload_ptrs.data(),
	    payload_lens.empty() ? nullptr : payload_lens.data(), static_cast<uint64_t>(bind_data.subjects.size()),
	    bind_data.confirm ? 1 : 0, &published, &err);
	if (rc != 0) {
		if (rc == 1 && err != nullptr) {
			ThrowRustError(err);
		}
		throw InvalidInputException("nats_rw_publish_batch failed with unexpected FFI error (rc=%d)", rc);
	}
	state->count = published;
	return std::move(state);
}

static void NatsRwPublishBatchExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<NatsRwBatchGlobalState>();
	if (state.consumed) {
		output.SetCardinality(0);
		return;
	}
	// 单行单列：published 计数（InitGlobal 中已完成发布）
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(state.count));
	state.consumed = true;
}

// ---------------- 表函数：nats_rw_publish_stats(url, reset) ----------------

struct NatsRwStatsBindData : public TableFunctionData {
	std::string url_filter;
	bool reset = false;
};

static unique_ptr<FunctionData> NatsRwStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<NatsRwStatsBindData>();
	for (auto &kv : input.named_parameters) {
		if (kv.first == "url") {
			if (!kv.second.IsNull()) {
				bind_data->url_filter = kv.second.GetValue<std::string>();
			}
		} else if (kv.first == "reset") {
			bind_data->reset = kv.second.GetValue<bool>();
		}
	}
	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("published");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("errors");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("last_error");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("last_error_at");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	return std::move(bind_data);
}

struct NatsRwStatsGlobalState : public GlobalTableFunctionState {
	bool consumed = false;
};

static unique_ptr<GlobalTableFunctionState> NatsRwStatsInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<NatsRwStatsGlobalState>();
}

static void NatsRwStatsExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<NatsRwStatsGlobalState>();
	if (gstate.consumed) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<NatsRwStatsBindData>();
	char *out_url = nullptr;
	int64_t published = 0, errors = 0;
	char *out_last_error = nullptr;
	int64_t last_error_at_ms = -1;
	char *err = nullptr;
	int32_t rc = nats_rw_publish_stats(bind_data.url_filter.c_str(), bind_data.reset ? 1 : 0, &out_url, &published,
	                                   &errors, &out_last_error, &last_error_at_ms, &err);
	if (rc != 0) {
		if (rc == 1 && err != nullptr) {
			ThrowRustError(err);
		}
		throw InvalidInputException("nats_rw_publish_stats failed with unexpected FFI error (rc=%d)", rc);
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(out_url ? out_url : ""));
	output.SetValue(1, 0, Value::BIGINT(published));
	output.SetValue(2, 0, Value::BIGINT(errors));
	output.SetValue(3, 0, Value(out_last_error && *out_last_error ? out_last_error : Value()));
	if (out_last_error && last_error_at_ms >= 0) {
		// epoch ms → TIMESTAMP（DuckDB TIMESTAMP 为 µs 精度、UTC 语义由调用方解读）
		timestamp_t ts = Timestamp::FromEpochMs(last_error_at_ms);
		output.SetValue(4, 0, Value::TIMESTAMP(ts));
	} else {
		output.SetValue(4, 0, Value());
	}
	if (out_url) {
		nats_rw_free_cstring(out_url);
	}
	if (out_last_error) {
		nats_rw_free_cstring(out_last_error);
	}
	gstate.consumed = true;
}

} // namespace

void RegisterNatsRwPublish(ExtensionLoader &loader) {
	// 标量：nats_rw_publish(subject VARCHAR, payload BLOB, secret_name VARCHAR := '', confirm BOOLEAN := false)
	ScalarFunction publish_fun(
	    "nats_rw_publish",
	    {LogicalType::VARCHAR, LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	    LogicalType::BOOLEAN, NatsRwPublishScalar);
	// 连接/发布可抛运行时错误（DNS 失败/不可达/认证拒绝）——DuckDB v2 要求显式标记
	publish_fun.SetFallible();
	loader.RegisterFunction(publish_fun);

	// 表函数：nats_rw_publish_batch(messages LIST(STRUCT(subject VARCHAR, payload BLOB)),
	//                              secret_name VARCHAR := '', confirm BOOLEAN := false)
	TableFunction batch_fun(
	    Identifier("nats_rw_publish_batch"),
	    FunctionSignature().AddPositionalOnly(
	        "messages", LogicalType::LIST(LogicalType::STRUCT(
	                                           {{"subject", LogicalType(LogicalTypeId::VARCHAR)},
	                                            {"payload", LogicalType(LogicalTypeId::BLOB)}}))),
	    NatsRwPublishBatchExecute, NatsRwPublishBatchBind, NatsRwPublishBatchInitGlobal);
	batch_fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("secret_name", LogicalType(LogicalTypeId::VARCHAR));
		options.Add("confirm", LogicalType(LogicalTypeId::BOOLEAN));
	});
	loader.RegisterFunction(batch_fun);

	// 表函数：nats_rw_publish_stats(url VARCHAR := '', reset BOOLEAN := false)
	TableFunction stats_fun(Identifier("nats_rw_publish_stats"), FunctionSignature(),
	                        NatsRwStatsExecute, NatsRwStatsBind, NatsRwStatsInitGlobal);
	stats_fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("url", LogicalType(LogicalTypeId::VARCHAR));
		options.Add("reset", LogicalType(LogicalTypeId::BOOLEAN));
	});
	loader.RegisterFunction(stats_fun);
}

} // namespace duckdb
