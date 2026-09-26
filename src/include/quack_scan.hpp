#pragma once

#include "duckdb/parser/qualified_name.hpp"
#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

struct QuackScanBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanBindData>();
		return other.client_connection->ConnectionId() == client_connection->ConnectionId() &&
		       other.client_connection->ServerURI() == client_connection->ServerURI() &&
		       other.qualified_table_name == qualified_table_name && other.column_names == column_names &&
		       other.column_types == column_types;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanBindData>();
		result->client_connection = client_connection;
		result->qualified_table_name = qualified_table_name;
		result->column_names = column_names;
		result->column_types = column_types;
		return std::move(result);
	}

	//! The table to scan, qualified the way the remote server sees it (e.g. s1.child.t). Empty when this
	//! scan does not read a catalog table but the result of a query
	QualifiedName qualified_table_name;
	vector<Identifier> column_names;
	vector<LogicalType> column_types;
	vector<unique_ptr<DataChunkWrapper>> results;
	shared_ptr<QuackClientConnection> client_connection;
	optional_ptr<TableCatalogEntry> table_entry;
	bool needs_more_fetch = true;
	hugeint_t query_uuid;
	atomic<bool> completed;

	~QuackScanBindData() override {
		if (!completed && query_uuid != hugeint_t {0, 0}) {
			try {
				client_connection->CancelQuery(query_uuid);
			} catch (...) {
				// server may already be gone
			}
		}
	}
};

class TableFunction;

class QuackScanFunction {
public:
	static TableFunction GetFunction();
};

class QuackScanByNameFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
