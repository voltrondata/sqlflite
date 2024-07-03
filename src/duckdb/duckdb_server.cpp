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
#include <mutex>

#include <arrow/api.h>
#include <arrow/flight/sql/server.h>

#include "duckdb_sql_info.h"
#include "duckdb_statement.h"
#include "duckdb_statement_batch_reader.h"
#include "duckdb_tables_schema_batch_reader.h"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace arrow {
    namespace flight {
        namespace sql {
            namespace duckdbflight {

                namespace {

                    std::string PrepareQueryForGetTables(const GetTables &command) {
                        std::stringstream table_query;

                        table_query
                                << "SELECT table_catalog as catalog_name, table_schema as schema_name, table_name, "
                                   "table_type FROM information_schema.tables where 1=1";

                        table_query << " and table_catalog = "
                                    << (command.catalog.has_value() ?
                                                "'" + command.catalog.value() + "'" :
                                                "CURRENT_DATABASE()");

                        if (command.db_schema_filter_pattern.has_value()) {
                            table_query << " and table_schema LIKE '"
                                        << command.db_schema_filter_pattern.value() << "'";
                        }

                        if (command.table_name_filter_pattern.has_value()) {
                            table_query << " and table_name LIKE '"
                                        << command.table_name_filter_pattern.value() << "'";
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

                    Status SetParametersOnDuckDBStatement(std::shared_ptr<DuckDBStatement> stmt,
                                                          FlightMessageReader *reader) {
                        while (true) {
                            ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, reader->Next())
                            const std::shared_ptr<RecordBatch> &record_batch = chunk.data;
                            if (record_batch == nullptr)
                                break;

                            const int64_t num_rows = record_batch->num_rows();
                            const int &num_columns = record_batch->num_columns();

                            for (int row_index = 0; row_index < num_rows; ++row_index) {
                                for (int column_index = 0; column_index < num_columns;
                                     ++column_index) {
                                    const std::shared_ptr<Array> &column = record_batch->column(
                                            column_index);
                                    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> scalar,
                                                          column->GetScalar(row_index))

                                    stmt->bind_parameters.push_back(scalar->ToString());
                                }
                            }
                        }

                        return Status::OK();
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDuckDBQuery(
                            std::shared_ptr<duckdb::Connection> db,
                            const std::string &query,
                            const std::shared_ptr<Schema> &schema) {
                        std::shared_ptr<DuckDBStatement> statement;

                        ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db, query))

                        std::shared_ptr<DuckDBStatementBatchReader> reader;
                        ARROW_ASSIGN_OR_RAISE(reader,
                                              DuckDBStatementBatchReader::Create(statement, schema))

                        return std::make_unique<RecordBatchStream>(reader);
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
                            const FlightDescriptor &descriptor,
                            const std::shared_ptr<Schema> &schema) {
                        std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
                        ARROW_ASSIGN_OR_RAISE(auto result, FlightInfo::Make(*schema, descriptor,
                                                                            endpoints, -1, -1))

                        return std::make_unique<FlightInfo>(result);
                    }

                    std::string PrepareQueryForGetImportedOrExportedKeys(
                            const std::string &filter) {
                        return R"(SELECT * FROM (SELECT database_name                                                                         AS pk_catalog_name,
       schema_name                                                                           AS pk_schema_name,
       regexp_replace(constraint_text, '^FOREIGN KEY \(.*\) REFERENCES (.*)\(.*\)$', '\1')   AS pk_table_name,
       regexp_replace(constraint_text, '^FOREIGN KEY \(.*\) REFERENCES (.*)\((.*)\)$', '\2') AS pk_column_name,
       database_name                                                                         AS fk_catalog_name,
       schema_name                                                                           AS fk_schema_name,
       table_name                                                                            AS fk_table_name,
       UNNEST(constraint_column_names)                                                       AS fk_column_name,
       UNNEST(constraint_column_indexes)                                                     AS key_sequence,
       'fk_' || fk_table_name || '_to_' || pk_table_name
         || CASE WHEN COUNT(*) OVER (PARTITION BY pk_table_name
                                                , fk_table_name
                                    ) > 1
                    THEN '_' ||
                       DENSE_RANK() OVER (PARTITION BY pk_table_name
                                                     , fk_table_name
                                          ORDER BY  constraint_column_names ASC
                                         )
                 ELSE ''
            END                                                                              AS fk_key_name,
       'pk_' || pk_table_name                                                                AS pk_key_name,
       1                                                                                     AS update_rule, /* DuckDB only supports RESTRICT */
       1                                                                                     AS delete_rule /* DuckDB only supports RESTRICT */
FROM duckdb_constraints()
WHERE constraint_type = 'FOREIGN KEY') WHERE )" +
                               filter + R"( ORDER BY
  pk_catalog_name, pk_schema_name, pk_table_name, pk_key_name, key_sequence)";
                    }

                }  // namespace

                class DuckDBFlightSqlServer::Impl {
                private:
                    std::shared_ptr<duckdb::DuckDB> db_instance_;
                    std::shared_ptr<duckdb::Connection> db_conn_;
                    bool print_queries_;
                    std::map<std::string, std::shared_ptr<DuckDBStatement>> prepared_statements_;
                    std::unordered_map<std::string, std::shared_ptr<duckdb::Connection>>
                            open_transactions_;
                    std::default_random_engine gen_;
                    std::mutex mutex_;

                    arrow::Result<std::shared_ptr<DuckDBStatement>> GetStatementByHandle(
                            const std::string &handle) {
                        std::scoped_lock guard(mutex_);
                        auto search = prepared_statements_.find(handle);
                        if (search == prepared_statements_.end()) {
                            return Status::KeyError("Prepared statement not found");
                        }
                        return search->second;
                    }

                    arrow::Result<std::shared_ptr<duckdb::Connection>> GetConnection(
                            const std::string &transaction_id) {
                        if (transaction_id.empty())
                            return db_conn_;

                        std::scoped_lock guard(mutex_);
                        auto it = open_transactions_.find(transaction_id);
                        if (it == open_transactions_.end()) {
                            return Status::KeyError("Unknown transaction ID: ", transaction_id);
                        }
                        return it->second;
                    }

                    // Create a Ticket that combines a query and a transaction ID.
                    arrow::Result<Ticket> EncodeTransactionQuery(
                            const std::string &query,
                            const std::string &transaction_id) {
                        std::string transaction_query = transaction_id;
                        transaction_query += ':';
                        transaction_query += query;
                        ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                                              CreateStatementQueryTicket(transaction_query))
                        return Ticket{std::move(ticket_string)};
                    }

                    arrow::Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
                            const std::string &ticket) {
                        auto divider = ticket.find(':');
                        if (divider == std::string::npos) {
                            return Status::Invalid("Malformed ticket");
                        }
                        std::string transaction_id = ticket.substr(0, divider);
                        std::string query = ticket.substr(divider + 1);
                        return std::make_pair(std::move(query), std::move(transaction_id));
                    }

                public:
                    explicit Impl(std::shared_ptr<duckdb::DuckDB> db_instance,
                                  std::shared_ptr<duckdb::Connection> db_connection,
                                  const bool &print_queries)
                        : db_instance_(std::move(db_instance)),
                          db_conn_(std::move(db_connection)),
                          print_queries_(print_queries) {}

                    ~Impl() = default;

                    std::string GenerateRandomString(int length = 16) const {
                        const char charset[] =
                                "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                        const int charsetLength = sizeof(charset) - 1;

                        std::random_device rd;  // Create a random device to seed the generator
                        std::mt19937 gen(rd());  // Create a Mersenne Twister generator
                        std::uniform_int_distribution<> dis(
                                0,
                                charsetLength -
                                        1);  // Create a uniform distribution over the character set

                        std::string randomString;
                        for (int i = 0; i < length; i++) {
                            randomString += charset[dis(gen)];
                        }

                        return randomString;
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
                            const ServerCallContext &context,
                            const StatementQuery &command,
                            const FlightDescriptor &descriptor) {
                        const std::string &query = command.query;
                        ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(command.transaction_id))
                        ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db, query))
                        ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema())
                        ARROW_ASSIGN_OR_RAISE(auto ticket,
                                              EncodeTransactionQuery(query, command.transaction_id))
                        std::vector<FlightEndpoint> endpoints{
                                FlightEndpoint{std::move(ticket), {}}};
                        ARROW_ASSIGN_OR_RAISE(auto result, FlightInfo::Make(*schema, descriptor,
                                                                            endpoints, -1, -1))
                        return std::make_unique<FlightInfo>(result);
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
                            const ServerCallContext &context,
                            const StatementQueryTicket &command) {
                        ARROW_ASSIGN_OR_RAISE(auto pair,
                                              DecodeTransactionQuery(command.statement_handle))
                        const std::string &sql = pair.first;
                        const std::string transaction_id = pair.second;
                        ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(transaction_id))
                        ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db, sql))
                        ARROW_ASSIGN_OR_RAISE(auto reader,
                                              DuckDBStatementBatchReader::Create(statement))

                        return std::make_unique<RecordBatchStream>(reader);
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
                            const ServerCallContext &context,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor, SqlSchema::GetCatalogsSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
                            const ServerCallContext &context) {
                        std::string query =
                                "SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name";

                        return DoGetDuckDBQuery(db_conn_, query, SqlSchema::GetCatalogsSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
                            const ServerCallContext &context,
                            const GetDbSchemas &command,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor, SqlSchema::GetDbSchemasSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>>
                    DoGetDbSchemas(const ServerCallContext &context, const GetDbSchemas &command) {
                        std::stringstream query;
                        query << "SELECT catalog_name, schema_name AS db_schema_name FROM information_schema.schemata WHERE 1 = 1";

                        query << " AND catalog_name = "
                              << (command.catalog.has_value() ?
                                          "'" + command.catalog.value() + "'" :
                                          "CURRENT_DATABASE()");
                        if (command.db_schema_filter_pattern.has_value()) {
                            query << " AND schema_name LIKE '"
                                  << command.db_schema_filter_pattern.value() << "'";
                        }
                        query << " ORDER BY catalog_name, db_schema_name";

                        return DoGetDuckDBQuery(db_conn_, query.str(),
                                                SqlSchema::GetDbSchemasSchema());
                    }

                    arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
                            const ServerCallContext &context,
                            const ActionCreatePreparedStatementRequest &request) {
                        std::scoped_lock guard(mutex_);
                        std::shared_ptr<DuckDBStatement> statement;
                        ARROW_ASSIGN_OR_RAISE(statement,
                                              DuckDBStatement::Create(db_conn_, request.query))
                        const std::string handle = GenerateRandomString();
                        prepared_statements_[handle] = statement;

                        ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema())

                        std::shared_ptr<duckdb::PreparedStatement> stmt =
                                statement->GetDuckDBStmt();
                        const id_t parameter_count = stmt->n_param;
                        FieldVector parameter_fields;
                        parameter_fields.reserve(parameter_count);

                        duckdb::shared_ptr<duckdb::PreparedStatementData> parameter_data =
                                stmt->data;
                        auto bind_parameter_map = parameter_data->value_map;

                        for (id_t i = 0; i < parameter_count; i++) {
                            std::string parameter_idx_str = std::to_string(i + 1);
                            std::string parameter_name = std::string("parameter_") +
                                                         parameter_idx_str;
                            auto parameter_duckdb_type = parameter_data->GetType(parameter_idx_str);
                            auto parameter_arrow_type = GetDataTypeFromDuckDbType(
                                    parameter_duckdb_type);
                            parameter_fields.push_back(field(parameter_name, parameter_arrow_type));
                        }

                        const std::shared_ptr<Schema> &parameter_schema = arrow::schema(
                                parameter_fields);

                        ActionCreatePreparedStatementResult result{
                                .dataset_schema = dataset_schema,
                                .parameter_schema = parameter_schema,
                                .prepared_statement_handle = handle};

                        if (print_queries_) {
                            std::cout << "Client running SQL command: \n"
                                      << request.query << ";\n"
                                      << std::endl;
                        }

                        return result;
                    }

                    Status ClosePreparedStatement(
                            const ServerCallContext &context,
                            const ActionClosePreparedStatementRequest &request) {
                        std::scoped_lock guard(mutex_);
                        const std::string &prepared_statement_handle =
                                request.prepared_statement_handle;

                        if (auto search = prepared_statements_.find(prepared_statement_handle);
                            search != prepared_statements_.end()) {
                            prepared_statements_.erase(prepared_statement_handle);
                        } else {
                            return Status::Invalid("Prepared statement not found");
                        }

                        return Status::OK();
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
                            const ServerCallContext &context,
                            const PreparedStatementQuery &command,
                            const FlightDescriptor &descriptor) {
                        std::scoped_lock guard(mutex_);
                        const std::string &prepared_statement_handle =
                                command.prepared_statement_handle;

                        auto search = prepared_statements_.find(prepared_statement_handle);
                        if (search == prepared_statements_.end()) {
                            return Status::Invalid("Prepared statement not found");
                        }

                        std::shared_ptr<DuckDBStatement> statement = search->second;

                        ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema())

                        return GetFlightInfoForCommand(descriptor, schema);
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
                            const ServerCallContext &context,
                            const PreparedStatementQuery &command) {
                        std::scoped_lock guard(mutex_);
                        const std::string &prepared_statement_handle =
                                command.prepared_statement_handle;

                        auto search = prepared_statements_.find(prepared_statement_handle);
                        if (search == prepared_statements_.end()) {
                            return Status::Invalid("Prepared statement not found");
                        }

                        std::shared_ptr<DuckDBStatement> statement = search->second;

                        ARROW_ASSIGN_OR_RAISE(auto reader,
                                              DuckDBStatementBatchReader::Create(statement))

                        return std::make_unique<RecordBatchStream>(reader);
                    }

                    Status DoPutPreparedStatementQuery(const ServerCallContext &context,
                                                       const PreparedStatementQuery &command,
                                                       FlightMessageReader *reader,
                                                       FlightMetadataWriter *writer) {
                        const std::string &prepared_statement_handle =
                                command.prepared_statement_handle;
                        ARROW_ASSIGN_OR_RAISE(auto statement,
                                              GetStatementByHandle(prepared_statement_handle))

                        ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

                        return Status::OK();
                    }

                    arrow::Result<int64_t> DoPutPreparedStatementUpdate(
                            const ServerCallContext &context,
                            const PreparedStatementUpdate &command,
                            FlightMessageReader *reader) {
                        const std::string &prepared_statement_handle =
                                command.prepared_statement_handle;
                        ARROW_ASSIGN_OR_RAISE(auto statement,
                                              GetStatementByHandle(prepared_statement_handle))

                        ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

                        return statement->ExecuteUpdate();
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>>
                    DoGetTables(const ServerCallContext &context, const GetTables &command) {
                        std::string query = PrepareQueryForGetTables(command);
                        std::shared_ptr<DuckDBStatement> statement;
                        ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db_conn_, query))

                        ARROW_ASSIGN_OR_RAISE(auto reader,
                                              DuckDBStatementBatchReader::Create(
                                                      statement, SqlSchema::GetTablesSchema()))

                        if (command.include_schema) {
                            auto table_schema_reader =
                                    std::make_shared<DuckDBTablesWithSchemaBatchReader>(
                                            reader, query, db_conn_);
                            return std::make_unique<RecordBatchStream>(table_schema_reader);
                        } else {
                            return std::make_unique<RecordBatchStream>(reader);
                        }
                    }

                    arrow::Result<int64_t> DoPutCommandStatementUpdate(
                            const ServerCallContext &context,
                            const StatementUpdate &command) {
                        const std::string &sql = command.query;
                        ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(command.transaction_id))
                        ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db, sql))
                        return statement->ExecuteUpdate();
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
                            const ServerCallContext &context,
                            const GetTables &command,
                            const FlightDescriptor &descriptor) {
                        std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};

                        bool include_schema = command.include_schema;

                        ARROW_ASSIGN_OR_RAISE(
                                auto result,
                                FlightInfo::Make(
                                        include_schema ?
                                                *SqlSchema::GetTablesSchemaWithIncludedSchema() :
                                                *SqlSchema::GetTablesSchema(),
                                        descriptor, endpoints, -1, -1))

                        return std::make_unique<FlightInfo>(result);
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
                            const ServerCallContext &context,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor,
                                                       SqlSchema::GetTableTypesSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
                            const ServerCallContext &context) {
                        std::string query =
                                "SELECT DISTINCT table_type FROM information_schema.tables";

                        return DoGetDuckDBQuery(db_conn_, query, SqlSchema::GetTableTypesSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
                            const ServerCallContext &context,
                            const GetPrimaryKeys &command,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor,
                                                       SqlSchema::GetPrimaryKeysSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
                            const ServerCallContext &context,
                            const GetPrimaryKeys &command) {
                        std::stringstream table_query;

                        // The field key_name can not be recovered by the sqlite, so it is being set
                        // to null following the same pattern for catalog_name and schema_name.
                        table_query
                                << "SELECT database_name AS catalog_name\n"
                                   "     , schema_name\n"
                                   "     , table_name\n"
                                   "     , column_name\n"
                                   "     , column_index + 1 AS key_sequence\n"
                                   "     , 'pk_' || table_name AS key_name\n"
                                   "   FROM (SELECT dc.*\n"
                                   "              , UNNEST(dc.constraint_column_indexes) AS column_index\n"
                                   "              , UNNEST(dc.constraint_column_names) AS column_name\n"
                                   "           FROM duckdb_constraints() AS dc\n"
                                   "          WHERE constraint_type = 'PRIMARY KEY'\n"
                                   "        ) WHERE 1 = 1";

                        const TableRef &table_ref = command.table_ref;
                        table_query << " AND catalog_name = "
                                    << (table_ref.catalog.has_value() ?
                                                "'" + table_ref.catalog.value() + "'" :
                                                "CURRENT_DATABASE()");

                        if (table_ref.db_schema.has_value()) {
                            table_query << " and schema_name LIKE '" << table_ref.db_schema.value()
                                        << "'";
                        }

                        table_query << " and table_name LIKE '" << table_ref.table << "'";

                        return DoGetDuckDBQuery(db_conn_, table_query.str(),
                                                SqlSchema::GetPrimaryKeysSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
                            const ServerCallContext &context,
                            const GetImportedKeys &command,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor,
                                                       SqlSchema::GetImportedKeysSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
                            const ServerCallContext &context,
                            const GetImportedKeys &command) {
                        const TableRef &table_ref = command.table_ref;
                        std::string filter = "fk_table_name = '" + table_ref.table + "'";

                        filter += " AND fk_catalog_name = " +
                                  (table_ref.catalog.has_value() ?
                                           "'" + table_ref.catalog.value() + "'" :
                                           "CURRENT_DATABASE()");
                        if (table_ref.db_schema.has_value()) {
                            filter += " AND fk_schema_name = '" + table_ref.db_schema.value() + "'";
                        }
                        std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

                        return DoGetDuckDBQuery(db_conn_, query,
                                                SqlSchema::GetImportedKeysSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
                            const ServerCallContext &context,
                            const GetExportedKeys &command,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor,
                                                       SqlSchema::GetExportedKeysSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
                            const ServerCallContext &context,
                            const GetExportedKeys &command) {
                        const TableRef &table_ref = command.table_ref;
                        std::string filter = "pk_table_name = '" + table_ref.table + "'";
                        filter += " AND pk_catalog_name = " +
                                  (table_ref.catalog.has_value() ?
                                           "'" + table_ref.catalog.value() + "'" :
                                           "CURRENT_DATABASE()");
                        if (table_ref.db_schema.has_value()) {
                            filter += " AND pk_schema_name = '" + table_ref.db_schema.value() + "'";
                        }
                        std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

                        return DoGetDuckDBQuery(db_conn_, query,
                                                SqlSchema::GetExportedKeysSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
                            const ServerCallContext &context,
                            const GetCrossReference &command,
                            const FlightDescriptor &descriptor) {
                        return GetFlightInfoForCommand(descriptor,
                                                       SqlSchema::GetCrossReferenceSchema());
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
                            const ServerCallContext &context,
                            const GetCrossReference &command) {
                        const TableRef &pk_table_ref = command.pk_table_ref;
                        std::string filter = "pk_table_name = '" + pk_table_ref.table + "'";
                        filter += " AND pk_catalog_name = " +
                                  (pk_table_ref.catalog.has_value() ?
                                           "'" + pk_table_ref.catalog.value() + "'" :
                                           "CURRENT_DATABASE()");
                        if (pk_table_ref.db_schema.has_value()) {
                            filter += " AND pk_schema_name = '" + pk_table_ref.db_schema.value() +
                                      "'";
                        }

                        const TableRef &fk_table_ref = command.fk_table_ref;
                        filter += " AND fk_table_name = '" + fk_table_ref.table + "'";
                        filter += " AND fk_catalog_name = " +
                                  (fk_table_ref.catalog.has_value() ?
                                           "'" + fk_table_ref.catalog.value() + "'" :
                                           "CURRENT_DATABASE()");
                        if (fk_table_ref.db_schema.has_value()) {
                            filter += " AND fk_schema_name = '" + fk_table_ref.db_schema.value() +
                                      "'";
                        }
                        std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

                        return DoGetDuckDBQuery(db_conn_, query,
                                                SqlSchema::GetCrossReferenceSchema());
                    }

                    arrow::Result<ActionBeginTransactionResult> BeginTransaction(
                            const ServerCallContext &context,
                            const ActionBeginTransactionRequest &request) {
                        std::string handle = GenerateRandomString();
                        auto new_db = std::make_shared<duckdb::Connection>(*db_instance_);
                        ARROW_RETURN_NOT_OK(ExecuteSql(new_db, "BEGIN TRANSACTION"));

                        std::scoped_lock guard(mutex_);
                        open_transactions_[handle] = new_db;
                        return ActionBeginTransactionResult{std::move(handle)};
                    }

                    Status EndTransaction(const ServerCallContext &context,
                                          const ActionEndTransactionRequest &request) {
                        Status status;
                        std::shared_ptr<duckdb::Connection> transaction = nullptr;
                        {
                            std::scoped_lock guard(mutex_);
                            auto it = open_transactions_.find(request.transaction_id);
                            if (it == open_transactions_.end()) {
                                return Status::KeyError("Unknown transaction ID: ",
                                                        request.transaction_id);
                            }

                            if (request.action == ActionEndTransactionRequest::kCommit) {
                                status = ExecuteSql(it->second, "COMMIT");
                            } else {
                                status = ExecuteSql(it->second, "ROLLBACK");
                            }
                            transaction = it->second;
                            open_transactions_.erase(it);
                        }
                        transaction.reset();
                        return status;
                    }

                    Status ExecuteSql(const std::string &sql) {
                        return ExecuteSql(db_conn_, sql);
                    }

                    Status ExecuteSql(std::shared_ptr<duckdb::Connection> db_conn,
                                      const std::string &sql) {
                        if (std::unique_ptr<duckdb::MaterializedQueryResult> result =
                                    db_conn->Query(sql);
                            result->HasError()) {
                            return Status::Invalid(result->GetError());
                        }
                        return Status::OK();
                    }
                };

                DuckDBFlightSqlServer::DuckDBFlightSqlServer(std::shared_ptr<Impl> impl)
                    : impl_(std::move(impl)) {}

                arrow::Result<std::shared_ptr<DuckDBFlightSqlServer>> DuckDBFlightSqlServer::Create(
                        const std::string &path,
                        const duckdb::DBConfig &config,
                        const bool &print_queries) {
                    std::cout << "DuckDB version: " << duckdb_library_version() << std::endl;

                    std::shared_ptr<duckdb::DuckDB> db;
                    std::shared_ptr<duckdb::Connection> con;

                    db = std::make_shared<duckdb::DuckDB>(path);
                    con = std::make_shared<duckdb::Connection>(*db);

                    auto impl = std::make_shared<Impl>(db, con, print_queries);
                    std::shared_ptr<DuckDBFlightSqlServer> result(new DuckDBFlightSqlServer(impl));

                    for (const auto &id_to_result : GetSqlInfoResultMap()) {
                        result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
                    }
                    return result;
                }

                DuckDBFlightSqlServer::~DuckDBFlightSqlServer() = default;

                arrow::Status DuckDBFlightSqlServer::ExecuteSql(const std::string &sql) {
                    return impl_->ExecuteSql(sql);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoStatement(const ServerCallContext &context,
                                                              const StatementQuery &command,
                                                              const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoStatement(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetStatement(const ServerCallContext &context,
                                                      const StatementQueryTicket &command) {
                    return impl_->DoGetStatement(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoCatalogs(const ServerCallContext &context,
                                                             const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoCatalogs(context, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetCatalogs(const ServerCallContext &context) {
                    return impl_->DoGetCatalogs(context);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoSchemas(const ServerCallContext &context,
                                                            const GetDbSchemas &command,
                                                            const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoSchemas(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetDbSchemas(const ServerCallContext &context,
                                                      const GetDbSchemas &command) {
                    return impl_->DoGetDbSchemas(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoTables(const ServerCallContext &context,
                                                           const GetTables &command,
                                                           const FlightDescriptor &descriptor) {

                    return impl_->GetFlightInfoTables(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetTables(
                        const ServerCallContext &context,
                        const GetTables &command) {

                    return impl_->DoGetTables(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoTableTypes(const ServerCallContext &context,
                                                               const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoTableTypes(context, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetTableTypes(const ServerCallContext &context) {
                    return impl_->DoGetTableTypes(context);
                }

                arrow::Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(
                        const ServerCallContext &context,
                        const StatementUpdate &command) {
                    return impl_->DoPutCommandStatementUpdate(context, command);
                }

                arrow::Result<ActionCreatePreparedStatementResult>
                DuckDBFlightSqlServer::CreatePreparedStatement(
                        const ServerCallContext &context,
                        const ActionCreatePreparedStatementRequest &request) {
                    return impl_->CreatePreparedStatement(context, request);
                }

                Status DuckDBFlightSqlServer::ClosePreparedStatement(
                        const ServerCallContext &context,
                        const ActionClosePreparedStatementRequest &request) {
                    return impl_->ClosePreparedStatement(context, request);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoPreparedStatement(
                        const ServerCallContext &context,
                        const PreparedStatementQuery &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoPreparedStatement(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetPreparedStatement(
                        const ServerCallContext &context,
                        const PreparedStatementQuery &command) {
                    return impl_->DoGetPreparedStatement(context, command);
                }

                Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(
                        const ServerCallContext &context,
                        const PreparedStatementQuery &command,
                        FlightMessageReader *reader,
                        FlightMetadataWriter *writer) {
                    return impl_->DoPutPreparedStatementQuery(context, command, reader, writer);
                }

                arrow::Result<int64_t> DuckDBFlightSqlServer::DoPutPreparedStatementUpdate(
                        const ServerCallContext &context,
                        const PreparedStatementUpdate &command,
                        FlightMessageReader *reader) {
                    return impl_->DoPutPreparedStatementUpdate(context, command, reader);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoPrimaryKeys(
                        const ServerCallContext &context,
                        const GetPrimaryKeys &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoPrimaryKeys(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetPrimaryKeys(const ServerCallContext &context,
                                                        const GetPrimaryKeys &command) {
                    return impl_->DoGetPrimaryKeys(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoImportedKeys(
                        const ServerCallContext &context,
                        const GetImportedKeys &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoImportedKeys(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetImportedKeys(const ServerCallContext &context,
                                                         const GetImportedKeys &command) {
                    return impl_->DoGetImportedKeys(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoExportedKeys(
                        const ServerCallContext &context,
                        const GetExportedKeys &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoExportedKeys(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetExportedKeys(const ServerCallContext &context,
                                                         const GetExportedKeys &command) {
                    return impl_->DoGetExportedKeys(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>>
                DuckDBFlightSqlServer::GetFlightInfoCrossReference(
                        const ServerCallContext &context,
                        const GetCrossReference &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoCrossReference(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetCrossReference(const ServerCallContext &context,
                                                           const GetCrossReference &command) {
                    return impl_->DoGetCrossReference(context, command);
                }

                arrow::Result<ActionBeginTransactionResult> DuckDBFlightSqlServer::BeginTransaction(
                        const ServerCallContext &context,
                        const ActionBeginTransactionRequest &request) {
                    return impl_->BeginTransaction(context, request);
                }

                Status DuckDBFlightSqlServer::EndTransaction(
                        const ServerCallContext &context,
                        const ActionEndTransactionRequest &request) {
                    return impl_->EndTransaction(context, request);
                }

            }  // namespace duckdbflight
        }  // namespace sql
    }  // namespace flight
}  // namespace arrow
