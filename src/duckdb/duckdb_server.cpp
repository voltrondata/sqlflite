// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "duckdb_server.h"

#include <duckdb.hpp>

#include <boost/algorithm/string.hpp>
#include <map>
#include <random>
#include <sstream>
#include <iostream>

#include <arrow/api.h>
#include <arrow/flight/sql/server.h>

#include "duckdb_sql_info.h"
#include "duckdb_statement.h"
#include "duckdb_statement_batch_reader.h"

namespace arrow {
namespace flight {
namespace sql {
namespace duckdbflight {

namespace {


std::string PrepareQueryForGetTables(const GetTables& command) {
  std::stringstream table_query;

  table_query << "SELECT 'NOT_IMPLEMENTED' as catalog_name, table_schema as schema_name, table_name,"
                 "table_type FROM information_schema.tables where 1=1";

  if (command.catalog.has_value()) {
    table_query << " and table_catalog='" << command.catalog.value() << "'";
  }

  if (command.db_schema_filter_pattern.has_value()) {
    table_query << " and table_schame LIKE '" << command.db_schema_filter_pattern.value()
                << "'";
  }

  if (command.table_name_filter_pattern.has_value()) {
    table_query << " and table_name LIKE '" << command.table_name_filter_pattern.value()
                << "'";
  }

  if (!command.table_types.empty()) {
    table_query << " and table_type IN (";
    size_t size = command.table_types.size();
    for (size_t i = 0; i < size; i++) {
      table_query << "'" << command.table_types[i] << "'";
      if (size - 1 != i) {
        table_query << ",";
      }
    }

    table_query << ")";
  }

  table_query << " order by table_name";
  return table_query.str();
}
}  // namespace

class DuckDBFlightSqlServer::Impl {
  private:
    std::shared_ptr<duckdb::DuckDB> db_instance_;
    std::shared_ptr<duckdb::Connection> db_conn_;

  public:
    explicit Impl(
      std::shared_ptr<duckdb::DuckDB> db_instance,
      std::shared_ptr<duckdb::Connection> db_connection
    ) : db_instance_(std::move(db_instance)), db_conn_(std::move(db_connection)) {
    }

    ~Impl() { 
    }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) {
    const std::string& query = command.query;

    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db_conn_, query));
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());
    ARROW_ASSIGN_OR_RAISE(auto ticket_string, CreateStatementQueryTicket(query));
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{ticket_string}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto result,
                          FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))
    return std::unique_ptr<FlightInfo>(new FlightInfo(result));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) {
    const std::string& sql = command.statement_handle;

    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db_conn_, sql));

    std::shared_ptr<DuckDBStatementBatchReader> reader;
    reader = DuckDBStatementBatchReader::Create(statement).ValueOrDie();

    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
      const ServerCallContext& context, const GetTables& command) {
    std::string query = PrepareQueryForGetTables(command);
    std::shared_ptr<DuckDBStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db_conn_, query));

    std::shared_ptr<DuckDBStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, DuckDBStatementBatchReader::Create(
                                      statement, SqlSchema::GetTablesSchema()));
    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }


  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context, const GetTables& command,
      const FlightDescriptor& descriptor) {
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};

    bool include_schema = command.include_schema;

    ARROW_ASSIGN_OR_RAISE(
        auto result,
        FlightInfo::Make(include_schema ? *SqlSchema::GetTablesSchemaWithIncludedSchema()
                                        : *SqlSchema::GetTablesSchema(),
                         descriptor, endpoints, -1, -1))

    return std::unique_ptr<FlightInfo>(new FlightInfo(result));
  }

};

DuckDBFlightSqlServer::DuckDBFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

arrow::Result<std::shared_ptr<DuckDBFlightSqlServer>> DuckDBFlightSqlServer::Create(
    const std::string &path,
    const duckdb::DBConfig &config
) {
  std::shared_ptr<duckdb::DuckDB> db;
  std::shared_ptr<duckdb::Connection> con;

  db = std::make_shared<duckdb::DuckDB>(path);
  con = std::make_shared<duckdb::Connection>(*db);

  std::shared_ptr<Impl> impl = std::make_shared<Impl>(db, con);
  std::shared_ptr<DuckDBFlightSqlServer> result(new DuckDBFlightSqlServer(impl));

  for (const auto& id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }
  return result;
}

DuckDBFlightSqlServer::~DuckDBFlightSqlServer() = default;

arrow::Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(
    const ServerCallContext& context, const StatementQuery& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(
    const ServerCallContext& context, const StatementQueryTicket& command) {
  return impl_->DoGetStatement(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoTables(
    const ServerCallContext& context, const GetTables& command,
    const FlightDescriptor& descriptor) {

  return impl_->GetFlightInfoTables(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetTables(
    const ServerCallContext& context, const GetTables& command) {

  return impl_->DoGetTables(context, command);
}

}  // namespace sqlite
}  // namespace sql
}  // namespace flight
}  // namespace arrow
