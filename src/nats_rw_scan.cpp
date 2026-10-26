#include "nats_rw_scan.hpp"
#include "nats_rw_rust.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

static constexpr idx_t COL_STREAM = 0;
static constexpr idx_t COL_SUBJECT = 1;
static constexpr idx_t COL_SEQ = 2;
static constexpr idx_t COL_TS = 3;
static constexpr idx_t COL_HEADERS = 4;
static constexpr idx_t COL_PAYLOAD = 5;

struct NatsRwScanBindData : public TableFunctionData {
	std::string stream;
	std::string url = "nats://127.0.0.1:4222";
	bool has_subject = false;
	std::string subject;
	uint64_t start_seq = 1;
	/// 0 = 扫描开始时的 last_seq
	uint64_t end_seq = 0;
	bool has_start_time = false;
	int64_t start_time_nanos = 0;
	bool has_end_time = false;
	int64_t end_time_nanos = 0;
	uint32_t concurrency = 32;
};

struct NatsRwScanRow {
	std::string subject;
	uint64_t seq;
	int64_t time_nanos;
	std::vector<std::pair<std::string, std::string>> headers;
	std::string payload; // 原始字节
};

struct NatsRwScanGlobalState : public GlobalTableFunctionState {
	std::vector<NatsRwScanRow> rows;
	idx_t offset = 0;

	idx_t MaxThreads() const override {
		// MVP 已一次性物化，无需并行扫描
		return 1;
	}
};

static void ThrowRustError(const char *err) {
	std::string msg = err == nullptr ? "unknown nats_rw error" : err;
	nats_rw_free_cstring(const_cast<char *>(err));
	throw InvalidInputException("nats_rw: %s", msg);
}

static unique_ptr<FunctionData> NatsRwScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<NatsRwScanBindData>();

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("nats_rw_scan: stream name is required");
	}
	bind_data->stream = input.inputs[0].GetValue<std::string>();

	for (auto &kv : input.named_parameters) {
		const auto &key = kv.first;
		const auto &value = kv.second;
		if (key == "url") {
			bind_data->url = value.GetValue<std::string>();
		} else if (key == "subject") {
			bind_data->has_subject = !value.IsNull();
			bind_data->subject = bind_data->has_subject ? value.GetValue<std::string>() : "";
		} else if (key == "start_seq") {
			bind_data->start_seq = value.GetValue<uint64_t>();
			if (bind_data->start_seq == 0) {
				throw BinderException("nats_rw_scan: start_seq must be >= 1");
			}
		} else if (key == "end_seq") {
			bind_data->end_seq = value.GetValue<uint64_t>();
		} else if (key == "start_time") {
			bind_data->has_start_time = !value.IsNull();
			if (bind_data->has_start_time) {
				bind_data->start_time_nanos = value.GetValue<timestamp_t>().value * 1000;
			}
		} else if (key == "end_time") {
			bind_data->has_end_time = !value.IsNull();
			if (bind_data->has_end_time) {
				bind_data->end_time_nanos = value.GetValue<timestamp_t>().value * 1000;
			}
		} else if (key == "batch_size") {
			auto batch = value.GetValue<uint64_t>();
			if (batch < 1 || batch > 256) {
				throw BinderException("nats_rw_scan: batch_size must be within [1, 256]");
			}
			bind_data->concurrency = static_cast<uint32_t>(batch);
		}
	}

	names.emplace_back("stream");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("subject");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("seq");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("ts_nats");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);
	names.emplace_back("headers");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)));
	names.emplace_back("payload");
	return_types.emplace_back(LogicalType::BLOB);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> NatsRwScanInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<NatsRwScanBindData>();
	auto state = make_uniq<NatsRwScanGlobalState>();

	void *raw = nullptr;
	char *err = nullptr;
	int32_t rc = nats_rw_scan_execute(
	    bind_data.stream.c_str(), bind_data.url.c_str(),
	    bind_data.has_subject ? bind_data.subject.c_str() : nullptr,
	    bind_data.start_seq, bind_data.end_seq,
	    bind_data.has_start_time ? bind_data.start_time_nanos : std::numeric_limits<int64_t>::min(),
	    bind_data.has_end_time ? bind_data.end_time_nanos : std::numeric_limits<int64_t>::min(),
	    bind_data.concurrency, &raw, &err);
	if (rc != 0) {
		if (rc == 1 && err != nullptr) {
			ThrowRustError(err);
		}
		throw InvalidInputException("nats_rw: scan failed with unexpected FFI error (rc=%d)", rc);
	}

	size_t len = nats_rw_scan_len(raw);
	state->rows.reserve(len);
	const std::string stream_name = nats_rw_scan_stream_name(raw) != nullptr ? nats_rw_scan_stream_name(raw) : bind_data.stream;
	for (size_t i = 0; i < len; i++) {
		NatsRwScanRow row;
		row.subject = nats_rw_scan_subject(raw, i) != nullptr ? nats_rw_scan_subject(raw, i) : "";
		row.seq = nats_rw_scan_seq(raw, i);
		row.time_nanos = nats_rw_scan_time_nanos(raw, i);
		uint32_t hcount = nats_rw_scan_header_count(raw, i);
		row.headers.reserve(hcount);
		for (uint32_t h = 0; h < hcount; h++) {
			const char *k = nats_rw_scan_header_key(raw, i, h);
			const char *v = nats_rw_scan_header_value(raw, i, h);
			row.headers.emplace_back(k != nullptr ? k : "", v != nullptr ? v : "");
		}
		uint64_t plen = 0;
		const uint8_t *pdata = nats_rw_scan_payload(raw, i, &plen);
		row.payload.assign(reinterpret_cast<const char *>(pdata != nullptr ? pdata : ""), plen);
		(void)stream_name;
		state->rows.push_back(std::move(row));
	}
	nats_rw_scan_free(raw);
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> NatsRwScanInitLocal(ExecutionContext &context,
                                                              TableFunctionInitInput &input) {
	return nullptr;
}

static Value NatsRwHeadersValue(const std::vector<std::pair<std::string, std::string>> &headers) {
	std::vector<Value> map_keys;
	std::vector<Value> map_values;
	for (const auto &h : headers) {
		map_keys.emplace_back(Value(h.first));
		// 每键首个值；多值 header 完整支持列入后续阶段
		map_values.push_back(Value::LIST(LogicalType::LIST(LogicalType::VARCHAR), {Value(h.second)}));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR), std::move(map_keys),
	                  std::move(map_values));
}

static void NatsRwScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<NatsRwScanBindData>();
	auto &state = data_p.global_state->Cast<NatsRwScanGlobalState>();

	while (state.offset < state.rows.size() && output.size() < STANDARD_VECTOR_SIZE) {
		const auto &row = state.rows[state.offset];
		idx_t out = output.size();

		output.SetValue(COL_STREAM, out, Value(bind_data.stream));
		output.SetValue(COL_SUBJECT, out, Value(row.subject));
		output.SetValue(COL_SEQ, out, Value::UBIGINT(row.seq));
		if (row.time_nanos != 0) {
			output.SetValue(COL_TS, out, Value::TIMESTAMPTZ(timestamp_t(row.time_nanos / 1000)));
		} else {
			output.SetValue(COL_TS, out, Value());
		}
		output.SetValue(COL_HEADERS, out, NatsRwHeadersValue(row.headers));
		output.SetValue(COL_PAYLOAD, out, Value::BLOB(row.payload));

		state.offset++;
	}
	output.SetCardinality(output.size());
}

void NatsRwScanFunction::Register(ExtensionLoader &loader) {
	TableFunction nats_rw_scan(Identifier("nats_rw_scan"),
	                           FunctionSignature().AddPositionalOnly("stream", LogicalType(LogicalTypeId::VARCHAR)),
	                           NatsRwScanExecute, NatsRwScanBind, NatsRwScanInitGlobal, NatsRwScanInitLocal);

	nats_rw_scan.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("url", LogicalType(LogicalTypeId::VARCHAR));
		options.Add("subject", LogicalType(LogicalTypeId::VARCHAR));
		options.Add("start_seq", LogicalType(LogicalTypeId::UBIGINT));
		options.Add("end_seq", LogicalType(LogicalTypeId::UBIGINT));
		options.Add("start_time", LogicalType(LogicalTypeId::TIMESTAMP_TZ));
		options.Add("end_time", LogicalType(LogicalTypeId::TIMESTAMP_TZ));
		options.Add("batch_size", LogicalType(LogicalTypeId::UBIGINT));
	});

	loader.RegisterFunction(nats_rw_scan);
}

} // namespace duckdb
