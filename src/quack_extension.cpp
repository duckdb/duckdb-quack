#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "quack_extension.hpp"
#include "rpc_log_type.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"
#include "rpc_uri.hpp"

#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "catalog.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

namespace duckdb {

static constexpr const char *QUACK_SECRET_TYPE = "quack";

static unique_ptr<BaseSecret> CreateQuackSecretFromConfig(ClientContext &, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.emplace_back("quack:");
	}
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "token") {
			secret->secret_map["token"] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown named parameter for quack secret: %s", lower_name);
		}
	}
	secret->redact_keys = {"token"};
	return std::move(secret);
}

static void RegisterQuackSecretType(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = QUACK_SECRET_TYPE;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "quack";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction config_fun = {QUACK_SECRET_TYPE, "config", CreateQuackSecretFromConfig};
	config_fun.named_parameters["token"] = LogicalType::VARCHAR;
	loader.RegisterFunction(config_fun);
}

static unique_ptr<Catalog> RpcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &attach_options) {
	auto diable_ssl = attach_options.options.find("disable_ssl") != attach_options.options.end() &&
	                  attach_options.options["disable_ssl"].GetValue<bool>();
	// info.path may or may not already carry the "quack:" prefix.
	auto uri = StringUtil::StartsWith(info.path, "quack:") ? info.path : "quack:" + info.path;
	return make_uniq<RpcCatalog>(db, RpcUri(uri, !diable_ssl), context);
}

static unique_ptr<TransactionManager> RpcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	return make_uniq<RpcTransactionManager>(db, rpc_catalog);
}

class RpcStorageExtension : public StorageExtension {
public:
	RpcStorageExtension() {
		attach = RpcAttach;
		create_transaction_manager = RpcCreateTransactionManager;
	}
};

// pass session id
static void RpcAuthToken(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto auth_str = args.GetValue(1, 0).GetValue<string>();
	Value default_token_val;
	auto &config = DBConfig::GetConfig(state.GetContext());
	auto lookup_result = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
	D_ASSERT(lookup_result);
	D_ASSERT(!default_token_val.IsNull());
	D_ASSERT(default_token_val.type().id() == LogicalTypeId::VARCHAR);
	auto default_token = default_token_val.GetValue<string>();
	result.SetValue(0, Value(auth_str == default_token));
}

static void RpcDummyAuthorization(const DataChunk &args, ExpressionState &, Vector &result) {
	result.SetValue(0, Value(true)); // choose life
}

static void RpcUriParser(const DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::BOOLEAN);
	D_ASSERT(result.GetType().id() == LogicalTypeId::STRUCT);

	RpcUri parsed(args.GetValue(0, 0).GetValue<string>(), args.GetValue(1, 0).GetValue<bool>());

	result.SetValue(0, Value::STRUCT({{"host", Value(parsed.Host())},
	                                  {"port", Value::USMALLINT(parsed.Port())},
	                                  {"ipv6", Value::BOOLEAN(parsed.IPv6())},
	                                  {"ssl", Value::BOOLEAN(parsed.Ssl())},
	                                  {"url", Value(parsed.Http())}}));
}

static void QuackIdentifyFun(ClientContext &, TableFunctionInput &, DataChunk &) {
	// No-op: side effects are in bind.
}

static unique_ptr<FunctionData> QuackIdentifyBind(ClientContext &ctx, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto &db_config = DBConfig::GetConfig(ctx);
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			continue;
		}
		db_config.SetOptionByName("whoami_" + kv.first, kv.second);
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("ok");
	return nullptr;
}

static TableFunction GetQuackIdentifyFunction() {
	TableFunction fun("quack_identify", {}, QuackIdentifyFun, QuackIdentifyBind);
	fun.named_parameters["name"] = LogicalType::VARCHAR;
	fun.named_parameters["provider"] = LogicalType::VARCHAR;
	fun.named_parameters["hostname"] = LogicalType::VARCHAR;
	fun.named_parameters["region"] = LogicalType::VARCHAR;
	fun.named_parameters["meta"] = LogicalType::VARCHAR; // JSON as string
	return fun;
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckDB Remote Procedure Calls (RPC)");

	loader.RegisterFunction(RpcScanFunction::GetFunction());
	loader.RegisterFunction(RpcScanByNameFunction::GetFunction());
	loader.RegisterFunction(GetQuackIdentifyFunction());

	loader.RegisterFunction(RpcStartFunction::GetFunction());
	loader.RegisterFunction(RpcStopFunction::GetFunction());

	// the default authentication function
	ScalarFunction rpc_auth_token("rpc_auth_token",
	                              {/* session id */ LogicalType::VARCHAR, /* auth string */ LogicalType::VARCHAR},
	                              LogicalType::BOOLEAN, RpcAuthToken);
	rpc_auth_token.SetVolatile();
	loader.RegisterFunction(rpc_auth_token);

	ScalarFunction rpc_authorization("rpc_dummy_authorization",
	                                 {/* session id */ LogicalType::VARCHAR, /* query string */ LogicalType::VARCHAR},
	                                 LogicalType::BOOLEAN, RpcDummyAuthorization);
	rpc_authorization.SetVolatile();
	loader.RegisterFunction(rpc_authorization);

	ScalarFunction rpc_uri_parser("rpc_uri_parser", {/* uri */ LogicalType::VARCHAR, /* ssl */ LogicalType::BOOLEAN},
	                              LogicalType::STRUCT({{"host", LogicalType::VARCHAR},
	                                                   {"port", LogicalType::USMALLINT},
	                                                   {"ipv6", LogicalType::BOOLEAN},
	                                                   {"ssl", LogicalType::BOOLEAN},
	                                                   {"url", LogicalType::VARCHAR}}),
	                              RpcUriParser);
	loader.RegisterFunction(rpc_uri_parser);

	RegisterQuackSecretType(loader);

	loader.GetDatabaseInstance().GetLogManager().RegisterLogType(make_uniq<RPCLogType>());

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<RpcStorageExtension>();
	ext->storage_info = duckdb::make_uniq<RpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, RpcStorageExtensionInfo::STORAGE_EXTENSION_KEY,
	                           ext);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("rpc_authentication_function", "Name of a callback function for authentication",
	                          LogicalType::VARCHAR, Value("rpc_auth_token"));
	config.AddExtensionOption("rpc_authorization_function", "Name of a callback function for authorization",
	                          LogicalType::VARCHAR, Value("rpc_dummy_authorization"));

	// TODO make this readonly from SQL?
	config.AddExtensionOption("rpc_default_token", "Authorization token used by default", LogicalType::VARCHAR, Value(),
	                          nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("quack_fetch_batch_chunks", "Maximum number of DataChunks returned per FETCH response",
	                          LogicalType::UBIGINT, Value::UBIGINT(12));

	// Process-wide fallback anchor for whoami().uptime when whoami_started_at isn't set.
	// Stored as BIGINT epoch-microseconds to stay TZ-invariant regardless of ICU state.
	config.AddExtensionOption("quack_loaded_at_us", "Epoch microseconds at extension load", LogicalType::BIGINT,
	                          Value::BIGINT(Timestamp::GetCurrentTimestamp().value));

	// whoami() identity fields — global settings so they propagate across all sessions
	// (rpc_call creates fresh server-side sessions that wouldn't see per-connection state).
	config.AddExtensionOption("whoami_name", "Human-readable name for this node", LogicalType::VARCHAR, Value());
	config.AddExtensionOption("whoami_provider", "Deployment provider (ec2, docker, local, ...)", LogicalType::VARCHAR,
	                          Value());
	config.AddExtensionOption("whoami_hostname", "Network hostname / public address", LogicalType::VARCHAR, Value());
	config.AddExtensionOption("whoami_region", "Deployment region", LogicalType::VARCHAR, Value());
	config.AddExtensionOption("whoami_started_at", "Node start time (ISO-8601 TIMESTAMP)", LogicalType::VARCHAR,
	                          Value());
	config.AddExtensionOption("whoami_meta", "Provider-specific metadata as JSON", LogicalType::VARCHAR, Value("{}"));

	// whoami() contract — register the table macro directly via the default-table-macro
	// machinery so function resolution in the body is deferred to invocation time
	// (avoids the get_current_timestamp / core_functions chicken-and-egg).
	static const DefaultTableMacro whoami_macro = {
	    DEFAULT_SCHEMA,       "whoami", {nullptr}, // no positional parameters
	    {{nullptr, nullptr}},                      // no named parameters
	    R"SQL(SELECT
		    current_setting('whoami_name')::VARCHAR     AS name,
		    current_setting('whoami_provider')::VARCHAR AS provider,
		    current_setting('whoami_hostname')::VARCHAR AS hostname,
		    current_setting('whoami_region')::VARCHAR   AS region,
		    to_microseconds(epoch_us(current_timestamp) - COALESCE(
		      epoch_us(current_setting('whoami_started_at')::TIMESTAMPTZ),
		      current_setting('quack_loaded_at_us')::BIGINT
		    ))                                          AS uptime,
		    current_timestamp                           AS ts_now,
		    json_merge_patch(
		      json_object(
		        'duckdb_version', version(),
		        'platform',       (SELECT platform FROM pragma_platform())
		      ),
		      COALESCE(TRY_CAST(current_setting('whoami_meta') AS JSON), '{}'::JSON)
		    )                                           AS meta
	    )SQL",
	};
	auto whoami_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(whoami_macro);
	loader.RegisterFunction(*whoami_info);
}

void QuackExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_RPC
	return EXT_VERSION_RPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quack, loader) {
	LoadInternal(loader);
}
}
