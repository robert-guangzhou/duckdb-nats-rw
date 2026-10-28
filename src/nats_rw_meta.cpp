#include "nats_rw_meta.hpp"
#include "nats_rw_rust.h"
#include "nats_rw_secret.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <string>
#include <vector>

namespace duckdb {

// 解析 url/secret 选项为生效 URL + 认证字段（scan/copy/元数据共用）。
// 优先级（Dev Plan 3.3）：显式 url 参数 > secret.url > 默认 nats://127.0.0.1:4222；
// secret := 'name' 显式指定，缺省按生效 URL 做 scope 最佳匹配。
struct NatsRwResolved {
	std::string url;
	std::string user, password, token, nkey, creds;
};

static NatsRwResolved ResolveConnection(ClientContext &context, const std::string &url_param,
                                        const std::string &secret_param) {
	NatsRwResolved resolved;
	resolved.url = url_param.empty() ? "nats://127.0.0.1:4222" : url_param;
	// 先用参数 url 或默认值做 scope 匹配；若命中 secret 且其带 url 且未显式传 url，则用 secret.url
	auto settings = NatsRwSecret::Lookup(context, secret_param, resolved.url);
	if (!settings.secret_name.empty()) {
		if (url_param.empty() && !settings.url.empty()) {
			resolved.url = settings.url;
		}
		resolved.user = settings.user;
		resolved.password = settings.password;
		resolved.token = settings.token;
		resolved.nkey = settings.nkey;
		resolved.creds = settings.creds;
	}
	return resolved;
}

static void ThrowRustErrorMeta(const char *err) {
	std::string msg = err == nullptr ? "unknown nats_rw error" : err;
	nats_rw_free_cstring(const_cast<char *>(err));
	throw InvalidInputException("nats_rw: %s", msg);
}

static const char *OrNull(const std::string &s) {
	return s.empty() ? nullptr : s.c_str();
}

// ---------------- nats_rw_streams() ----------------

struct NatsRwStreamsBindData : public TableFunctionData {
	std::string url;
	std::string user, password, token, nkey, creds;

	unique_ptr<FunctionData> Copy() const override {
		auto ret = make_uniq<NatsRwStreamsBindData>();
		ret->url = url;
		ret->user = user;
		ret->password = password;
		ret->token = token;
		ret->nkey = nkey;
		ret->creds = creds;
		return std::move(ret);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NatsRwStreamsBindData>();
		return url == other.url && user == other.user && password == other.password && token == other.token &&
		       nkey == other.nkey && creds == other.creds;
	}
};

struct NatsRwStreamsGlobal : public GlobalTableFunctionState {
	std::vector<std::array<std::string, 1>> names;
	std::vector<uint64_t> messages, bytes, last_seqs;
	idx_t offset = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> NatsRwStreamsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<NatsRwStreamsBindData>();
	std::string url_param, secret_param;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "url") {
			url_param = kv.second.GetValue<std::string>();
		} else if (kv.first == "secret") {
			secret_param = kv.second.GetValue<std::string>();
		}
	}
	auto resolved = ResolveConnection(context, url_param, secret_param);
	bind_data->url = resolved.url;
	bind_data->user = resolved.user;
	bind_data->password = resolved.password;
	bind_data->token = resolved.token;
	bind_data->nkey = resolved.nkey;
	bind_data->creds = resolved.creds;

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("messages");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("last_seq");
	return_types.emplace_back(LogicalType::UBIGINT);
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> NatsRwStreamsInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<NatsRwStreamsBindData>();
	auto state = make_uniq<NatsRwStreamsGlobal>();
	void *raw = nullptr;
	char *err = nullptr;
	int32_t rc =
	    nats_rw_streams_execute(bind_data.url.c_str(), OrNull(bind_data.user), OrNull(bind_data.password),
		                        OrNull(bind_data.token), OrNull(bind_data.nkey), OrNull(bind_data.creds), &raw, &err);
	if (rc != 0) {
		if (rc == 1 && err != nullptr) {
			ThrowRustErrorMeta(err);
		}
		throw InvalidInputException("nats_rw: streams listing failed with unexpected FFI error (rc=%d)", rc);
	}
	size_t len = nats_rw_streams_len(raw);
	state->messages.reserve(len);
	state->bytes.reserve(len);
	state->last_seqs.reserve(len);
	for (size_t i = 0; i < len; i++) {
		const char *n = nats_rw_streams_name(raw, i);
		state->names.push_back({std::string(n != nullptr ? n : "")});
		state->messages.push_back(nats_rw_streams_messages(raw, i));
		state->bytes.push_back(nats_rw_streams_bytes(raw, i));
		state->last_seqs.push_back(nats_rw_streams_last_seq(raw, i));
	}
	nats_rw_streams_free(raw);
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> NatsRwStreamsInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	return nullptr;
}

static void NatsRwStreamsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<NatsRwStreamsGlobal>();
	idx_t out = 0;
	while (state.offset < state.names.size() && out < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, out, Value(state.names[state.offset][0]));
		output.SetValue(1, out, Value::UBIGINT(state.messages[state.offset]));
		output.SetValue(2, out, Value::UBIGINT(state.bytes[state.offset]));
		output.SetValue(3, out, Value::UBIGINT(state.last_seqs[state.offset]));
		state.offset++;
		out++;
	}
	output.SetCardinality(out);
}

// ---------------- nats_rw_stream_info(stream) ----------------

struct NatsRwInfoBindData : public TableFunctionData {
	std::string stream;
	std::string url;
	std::string user, password, token, nkey, creds;

	unique_ptr<FunctionData> Copy() const override {
		auto ret = make_uniq<NatsRwInfoBindData>();
		ret->stream = stream;
		ret->url = url;
		ret->user = user;
		ret->password = password;
		ret->token = token;
		ret->nkey = nkey;
		ret->creds = creds;
		return std::move(ret);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NatsRwInfoBindData>();
		return stream == other.stream && url == other.url && token == other.token && user == other.user &&
		       password == other.password && nkey == other.nkey && creds == other.creds;
	}
};

struct NatsRwInfoGlobal : public GlobalTableFunctionState {
	bool emitted = false;
	std::string name;
	std::vector<std::string> subjects;
	uint64_t messages = 0, bytes = 0, first_seq = 0, last_seq = 0, consumers = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> NatsRwInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<NatsRwInfoBindData>();
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("nats_rw_stream_info: stream name is required");
	}
	bind_data->stream = input.inputs[0].GetValue<std::string>();
	std::string url_param, secret_param;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "url") {
			url_param = kv.second.GetValue<std::string>();
		} else if (kv.first == "secret") {
			secret_param = kv.second.GetValue<std::string>();
		}
	}
	auto resolved = ResolveConnection(context, url_param, secret_param);
	bind_data->url = resolved.url;
	bind_data->user = resolved.user;
	bind_data->password = resolved.password;
	bind_data->token = resolved.token;
	bind_data->nkey = resolved.nkey;
	bind_data->creds = resolved.creds;

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("subjects");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	names.emplace_back("messages");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("first_seq");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("last_seq");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("consumer_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> NatsRwInfoInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<NatsRwInfoBindData>();
	auto state = make_uniq<NatsRwInfoGlobal>();
	void *raw = nullptr;
	char *err = nullptr;
	int32_t rc = nats_rw_stream_info_execute(bind_data.url.c_str(), bind_data.stream.c_str(), OrNull(bind_data.user),
	                                         OrNull(bind_data.password), OrNull(bind_data.token),
	                                         OrNull(bind_data.nkey), OrNull(bind_data.creds), &raw, &err);
	if (rc != 0) {
		if (rc == 1 && err != nullptr) {
			ThrowRustErrorMeta(err);
		}
		throw InvalidInputException("nats_rw: stream info failed with unexpected FFI error (rc=%d)", rc);
	}
	const char *n = nats_rw_info_name(raw);
	state->name = n != nullptr ? n : bind_data.stream;
	uint32_t sc = nats_rw_info_subject_count(raw);
	for (uint32_t i = 0; i < sc; i++) {
		const char *s = nats_rw_info_subject(raw, i);
		state->subjects.push_back(s != nullptr ? s : "");
	}
	state->messages = nats_rw_info_messages(raw);
	state->bytes = nats_rw_info_bytes(raw);
	state->first_seq = nats_rw_info_first_seq(raw);
	state->last_seq = nats_rw_info_last_seq(raw);
	state->consumers = nats_rw_info_consumer_count(raw);
	nats_rw_stream_info_free(raw);
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> NatsRwInfoInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return nullptr;
}

static void NatsRwInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<NatsRwInfoGlobal>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	std::vector<Value> subject_values;
	for (auto &s : state.subjects) {
		subject_values.emplace_back(Value(s));
	}
	output.SetValue(0, 0, Value(state.name));
	output.SetValue(1, 0, Value::LIST(LogicalType::VARCHAR, std::move(subject_values)));
	output.SetValue(2, 0, Value::UBIGINT(state.messages));
	output.SetValue(3, 0, Value::UBIGINT(state.bytes));
	output.SetValue(4, 0, Value::UBIGINT(state.first_seq));
	output.SetValue(5, 0, Value::UBIGINT(state.last_seq));
	output.SetValue(6, 0, Value::UBIGINT(state.consumers));
	state.emitted = true;
	output.SetCardinality(1);
}

void NatsRwMetaFunctions::Register(ExtensionLoader &loader) {
	TableFunction streams_fn(Identifier("nats_rw_streams"),
	                         FunctionSignature().AddPositionalOnly("__unused", LogicalType(LogicalTypeId::VARCHAR)),
	                         NatsRwStreamsExecute, NatsRwStreamsBind, NatsRwStreamsInitGlobal, NatsRwStreamsInitLocal);
	streams_fn.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("url", LogicalType(LogicalTypeId::VARCHAR));
		options.Add("secret", LogicalType(LogicalTypeId::VARCHAR));
	});
	loader.RegisterFunction(streams_fn);

	TableFunction info_fn(Identifier("nats_rw_stream_info"),
	                      FunctionSignature().AddPositionalOnly("stream", LogicalType(LogicalTypeId::VARCHAR)),
	                      NatsRwInfoExecute, NatsRwInfoBind, NatsRwInfoInitGlobal, NatsRwInfoInitLocal);
	info_fn.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("url", LogicalType(LogicalTypeId::VARCHAR));
		options.Add("secret", LogicalType(LogicalTypeId::VARCHAR));
	});
	loader.RegisterFunction(info_fn);
}

} // namespace duckdb
