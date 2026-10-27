#include "nats_rw_copyto.hpp"
#include "nats_rw_rust.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include <cstdio>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

struct NatsRwCopyBindData : public FunctionData {
	std::string stream;
	std::string url = "nats://127.0.0.1:4222";
	std::string subject;      // 缺省 = 流名
	bool create_stream = false;
	bool has_payload_column = false;
	std::string payload_column;
	// 整行 JSON 序列化所需（payload_column 模式不用）
	std::vector<std::string> names;

	unique_ptr<FunctionData> Copy() const override {
		auto ret = make_uniq<NatsRwCopyBindData>();
		ret->stream = stream;
		ret->url = url;
		ret->subject = subject;
		ret->create_stream = create_stream;
		ret->has_payload_column = has_payload_column;
		ret->payload_column = payload_column;
		ret->names = names;
		return std::move(ret);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NatsRwCopyBindData>();
		return stream == other.stream && url == other.url && subject == other.subject &&
		       create_stream == other.create_stream && has_payload_column == other.has_payload_column &&
		       payload_column == other.payload_column && names == other.names;
	}
};

struct NatsRwCopyGlobalState : public GlobalFunctionData {
	std::atomic<uint64_t> published {0};
};

// 从 COPY 选项取常量值（函数自定义选项由绑定器存放于 info.options：
// map<Identifier, vector<Value>>——2026-10-26 实测，parsed_options 为空）。
static std::optional<std::string> OptionString(const CopyInfo &info, const std::string &key) {
	auto it = info.options.find(Identifier(key));
	if (it == info.options.end() || it->second.empty()) {
		return std::nullopt;
	}
	const Value &v = it->second[0];
	if (v.IsNull()) {
		return std::nullopt;
	}
	if (v.type().id() == LogicalTypeId::VARCHAR) {
		return v.GetValue<std::string>();
	}
	if (v.type().id() == LogicalTypeId::BOOLEAN) {
		return v.GetValue<bool>() ? "true" : "false";
	}
	// 数值等其他声明类型以字符串承载
	return v.ToString();
}

static unique_ptr<FunctionData> NatsRwCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                               const vector<Identifier> &names,
                                               const vector<LogicalType> &sql_types) {
	if (input.info.file_path.empty()) {
		throw BinderException("nats_rw COPY: target stream name is required (COPY ... TO '<stream>')");
	}
	auto bind_data = make_uniq<NatsRwCopyBindData>();
	bind_data->stream = input.info.file_path;
	bind_data->subject = bind_data->stream;

	if (auto url = OptionString(input.info, "url")) {
		bind_data->url = *url;
	}
	if (auto subject = OptionString(input.info, "subject_prefix")) {
		if (subject->empty()) {
			throw BinderException("nats_rw COPY: subject_prefix must not be empty");
		}
		bind_data->subject = *subject;
	}
	if (auto pc = OptionString(input.info, "payload_column")) {
		bind_data->has_payload_column = true;
		bind_data->payload_column = *pc;
		bool found = false;
		for (auto &n : names) {
			if (StringUtil::CIEquals(static_cast<const std::string &>(n), bind_data->payload_column)) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw BinderException("nats_rw COPY: payload_column '%s' not found in source columns", bind_data->payload_column);
		}
	}
	if (auto cs = OptionString(input.info, "create_stream")) {
		std::string v = StringUtil::Lower(*cs);
		if (v == "true" || v == "1") {
			bind_data->create_stream = true;
		} else if (v == "false" || v == "0") {
			bind_data->create_stream = false;
		} else {
			throw BinderException("nats_rw COPY: create_stream must be true/false");
		}
	}

	for (auto &n : names) {
		bind_data->names.push_back(static_cast<const std::string &>(n));
	}
	return std::move(bind_data);
}

static unique_ptr<GlobalFunctionData> NatsRwCopyInitGlobal(ClientContext &context, FunctionData &bind_data,
                                                           const std::string &file_path) {
	return make_uniq<NatsRwCopyGlobalState>();
}

static unique_ptr<LocalFunctionData> NatsRwCopyInitLocal(ExecutionContext &context, FunctionData &bind_data) {
	return make_uniq<LocalFunctionData>();
}

// 顺序保持要求串行 sink——显式 REGULAR 模式（单线程 sink），combine 为空实现
static void NatsRwCopyCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                              LocalFunctionData &lstate) {
}

// 最小 JSON 转义：控制字符、引号、反斜杠；其余 UTF-8 原样透传。
static std::string JsonEscape(const std::string &s) {
	std::string out;
	out.reserve(s.size() + 8);
	for (char c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", c);
				out += buf;
			} else {
				out += c;
			}
		}
	}
	return out;
}

// 标量值的干净文本（剥掉 Value::ToString() 对时间等类型添加的 SQL 字面量单引号，
// 避免 JSON 值出现双重引号——审查 F3，2026-10-27）
static std::string CleanScalarText(const Value &v) {
	std::string s = v.ToString();
	if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
		return s.substr(1, s.size() - 2);
	}
	return s;
}

static std::string ValueToJsonScalar(const Value &v) {
	if (v.IsNull()) {
		return "null";
	}
	switch (v.type().id()) {
	case LogicalTypeId::VARCHAR:
		return "\"" + JsonEscape(v.GetValue<std::string>()) + "\"";
	case LogicalTypeId::BOOLEAN:
		return v.GetValue<bool>() ? "true" : "false";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return v.ToString();
	default:
		// 时间戳/日期/DECIMAL 等以干净字符串承载（无 SQL 字面量引号）
		return "\"" + JsonEscape(CleanScalarText(v)) + "\"";
	}
}

// sink 的 chunk 为平面向量：VARCHAR/BLOB 的 string_t 即原始字节（BLOB round-trip 精确）
static void BuildRows(FunctionData &bind_data_p, DataChunk &input, std::vector<std::string> &subjects,
                      std::vector<std::string> &payloads) {
	auto &bind_data = bind_data_p.Cast<NatsRwCopyBindData>();
	idx_t payload_idx = 0;
	if (bind_data.has_payload_column) {
		for (idx_t c = 0; c < input.ColumnCount(); c++) {
			if (StringUtil::CIEquals(bind_data.names[c], bind_data.payload_column)) {
				payload_idx = c;
				break;
			}
		}
	}
	for (idx_t r = 0; r < input.size(); r++) {
		std::string payload;
		if (bind_data.has_payload_column) {
			auto &vec = input.data[payload_idx];
			auto val = vec.GetValue(r);
			if (val.IsNull()) {
				payload.clear();
			} else if (val.type().id() == LogicalTypeId::VARCHAR) {
				// 裸字节取值（对字典/常量等非平面向量同样正确；Value::ToString()
				// 会产生带引号的 SQL 字面量——审查 F1，2026-10-27）
				payload = val.GetValue<std::string>();
			} else if (val.type().id() == LogicalTypeId::BLOB) {
				// BLOB Value 取裸字节须经字面量往返（GetValue<string> 不返回原始字节——实测）
				payload = Blob::ToBlob(val.ToString());
			} else {
				payload = val.ToString();
			}
		} else {
			// 整行 JSON（紧凑格式，键序 = 源列序）
			payload = "{";
			for (idx_t c = 0; c < input.ColumnCount(); c++) {
				if (c > 0) {
					payload += ",";
				}
				payload += "\"" + JsonEscape(bind_data.names[c]) + "\":";
				payload += ValueToJsonScalar(input.data[c].GetValue(r));
			}
			payload += "}";
		}
		subjects.push_back(bind_data.subject);
		payloads.push_back(std::move(payload));
	}
}

static void NatsRwCopySink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                           LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<NatsRwCopyBindData>();
	auto &gstate = gstate_p.Cast<NatsRwCopyGlobalState>();
	if (input.size() == 0) {
		return;
	}
	std::vector<std::string> subjects;
	std::vector<std::string> payloads;
	BuildRows(bind_data_p, input, subjects, payloads);

	// FFI 数组视图（生命周期仅限本次调用）
	std::vector<const char *> subject_ptrs;
	std::vector<const uint8_t *> payload_ptrs;
	std::vector<uint64_t> payload_lens;
	subject_ptrs.reserve(subjects.size());
	payload_ptrs.reserve(payloads.size());
	payload_lens.reserve(payloads.size());
	for (idx_t i = 0; i < subjects.size(); i++) {
		subject_ptrs.push_back(subjects[i].c_str());
		payload_ptrs.push_back(reinterpret_cast<const uint8_t *>(payloads[i].data()));
		payload_lens.push_back(payloads[i].size());
	}

	uint64_t published = 0;
	char *err = nullptr;
	int32_t rc = nats_rw_copy_execute(bind_data.stream.c_str(), bind_data.url.c_str(),
	                                  bind_data.subject.c_str(), bind_data.create_stream ? 1 : 0,
	                                  subject_ptrs.data(), payload_ptrs.data(), payload_lens.data(),
	                                  static_cast<uint64_t>(subjects.size()), &published, &err);
	if (rc != 0 && err != nullptr) {
		std::string msg = err;
		nats_rw_free_cstring(err);
		throw InvalidInputException("nats_rw: %s", msg);
	}
	if (rc != 0) {
		throw InvalidInputException("nats_rw: copy failed with unexpected FFI error (rc=%d)", rc);
	}
	gstate.published.fetch_add(published);
}

static void NatsRwCopyFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	// publish 已在 sink 内完成并等待 ack；此处无持久化句柄需要收尾
}

void NatsRwCopyFunction::Register(ExtensionLoader &loader) {
	CopyFunction nats_rw_copy("nats_rw");
	// 选项声明（必须）：未声明的 COPY 选项会被静默丢弃——2026-10-26 实测教训
	nats_rw_copy.copy_options = [](ClientContext &context, CopyOptionsInput &input) {
		auto &opts = input.options;
		opts["url"] = CopyOption(LogicalType::VARCHAR);
		opts["subject_prefix"] = CopyOption(LogicalType::VARCHAR);
		opts["payload_column"] = CopyOption(LogicalType::VARCHAR);
		opts["create_stream"] = CopyOption(LogicalType::BOOLEAN);
	};
	nats_rw_copy.copy_to_bind = NatsRwCopyBind;
	nats_rw_copy.copy_to_initialize_global = NatsRwCopyInitGlobal;
	nats_rw_copy.copy_to_initialize_local = NatsRwCopyInitLocal;
	nats_rw_copy.copy_to_sink = NatsRwCopySink;
	nats_rw_copy.copy_to_combine = NatsRwCopyCombine;
	nats_rw_copy.copy_to_finalize = NatsRwCopyFinalize;
	nats_rw_copy.execution_mode = [](bool preserve_insertion_order, bool supports_batch_index) {
		return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
	};
	loader.RegisterFunction(nats_rw_copy);
}

} // namespace duckdb
