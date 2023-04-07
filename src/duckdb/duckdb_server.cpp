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
                                << "SELECT 'NOT_IMPLEMENTED' as catalog_name, table_schema as schema_name, table_name,"
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

                    Status
                    SetParametersOnDuckDBStatement(std::shared_ptr<DuckDBStatement> stmt, FlightMessageReader *reader) {
                        while (true) {
                            ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, reader->Next());
                            std::shared_ptr<RecordBatch> &record_batch = chunk.data;
                            if (record_batch == nullptr) break;

                            const int64_t num_rows = record_batch->num_rows();
                            const int &num_columns = record_batch->num_columns();

                            for (int i = 0; i < num_rows; ++i) {
                                for (int c = 0; c < num_columns; ++c) {
                                    const std::shared_ptr<Array> &column = record_batch->column(c);
                                    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> scalar, column->GetScalar(i));

                                    stmt->bind_parameters.push_back(scalar->ToString());
                                }
                            }
                        }

                        return Status::OK();
                    }


                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
                            const FlightDescriptor &descriptor, const std::shared_ptr<Schema> &schema) {
                        std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd},
                                                                             {}}};
                        ARROW_ASSIGN_OR_RAISE(auto result,
                                              FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

                        return std::make_unique<FlightInfo>(result);
                    }

                }  // namespace

                class DuckDBFlightSqlServer::Impl {
                private:
                    std::shared_ptr<duckdb::DuckDB> db_instance_;
                    std::shared_ptr<duckdb::Connection> db_conn_;
                    bool print_queries_;
                    std::map<std::string, std::shared_ptr<DuckDBStatement>> prepared_statements_;
                    std::default_random_engine gen_;
                    std::mutex mutex_;

                    arrow::Result<std::shared_ptr<DuckDBStatement>> GetStatementByHandle(
                            const std::string &handle) {
                        std::lock_guard<std::mutex> guard(mutex_);
                        auto search = prepared_statements_.find(handle);
                        if (search == prepared_statements_.end()) {
                            return Status::KeyError("Prepared statement not found");
                        }
                        return search->second;
                    }

                public:
                    explicit Impl(
                            std::shared_ptr<duckdb::DuckDB> db_instance,
                            std::shared_ptr<duckdb::Connection> db_connection,
                            const bool &print_queries
                    ) : db_instance_(std::move(db_instance)), db_conn_(std::move(db_connection)) {
                        print_queries_ = print_queries;
                    }

                    ~Impl() {
                    }

                    std::string GenerateRandomString(int length = 16) {
                        const char charset[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                        const int charsetLength = sizeof(charset) - 1;

                        std::random_device rd; // Create a random device to seed the generator
                        std::mt19937 gen(rd()); // Create a Mersenne Twister generator
                        std::uniform_int_distribution<> dis(0, charsetLength - 1); // Create a uniform distribution over the character set

                        std::string randomString;
                        for (int i = 0; i < length; i++) {
                            randomString += charset[dis(gen)];
                        }

                        return randomString;
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
                            const ServerCallContext &context, const StatementQuery &command,
                            const FlightDescriptor &descriptor) {
                        const std::string &query = command.query;

                        ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db_conn_, query));
                        ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());
                        ARROW_ASSIGN_OR_RAISE(auto ticket_string, CreateStatementQueryTicket(query));
                        std::vector<FlightEndpoint> endpoints{FlightEndpoint{{ticket_string},
                                                                             {}}};
                        ARROW_ASSIGN_OR_RAISE(auto result,
                                              FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))
                        return std::unique_ptr<FlightInfo>(new FlightInfo(result));
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
                            const ServerCallContext &context, const StatementQueryTicket &command) {
                        const std::string &sql = command.statement_handle;

                        ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db_conn_, sql));

                        std::shared_ptr<DuckDBStatementBatchReader> reader;
                        reader = DuckDBStatementBatchReader::Create(statement).ValueOrDie();

                        return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
                    }

                    arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
                            const ServerCallContext &context,
                            const ActionCreatePreparedStatementRequest &request) {
                        std::shared_ptr<DuckDBStatement> statement;
                        ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db_conn_, request.query));
                        const std::string handle = GenerateRandomString();
                        prepared_statements_[handle] = statement;

                        ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema());

                        std::shared_ptr<duckdb::PreparedStatement> stmt = statement->GetDuckDBStmt();
                        const id_t parameter_count = stmt->n_param;
                        FieldVector parameter_fields;
                        parameter_fields.reserve(parameter_count);

                        std::shared_ptr<duckdb::PreparedStatementData> parameter_data = stmt->data;
                        auto bind_parameter_map = parameter_data->value_map;

                        for (id_t i = 0; i < parameter_count; i++) {
                            std::string parameter_name = std::string("parameter_") + std::to_string(i + 1);
                            auto parameter_duckdb_type = parameter_data->GetType(i + 1);
                            auto parameter_arrow_type = GetDataTypeFromDuckDbType(parameter_duckdb_type);
                            parameter_fields.push_back(field(parameter_name, parameter_arrow_type));
                        }

                        const std::shared_ptr<Schema> &parameter_schema = arrow::schema(parameter_fields);

                        ActionCreatePreparedStatementResult result{.dataset_schema = dataset_schema,
                                .parameter_schema = parameter_schema,
                                .prepared_statement_handle = handle};

                        if (print_queries_) {
                            std::cout << "Client running SQL command: \n" << request.query << ";\n" << std::endl;
                        };

                        return result;
                    }

                    Status ClosePreparedStatement(const ServerCallContext &context,
                                                  const ActionClosePreparedStatementRequest &request) {
                        const std::string &prepared_statement_handle = request.prepared_statement_handle;

                        auto search = prepared_statements_.find(prepared_statement_handle);
                        if (search != prepared_statements_.end()) {
                            prepared_statements_.erase(prepared_statement_handle);
                        } else {
                            return Status::Invalid("Prepared statement not found");
                        }

                        return Status::OK();
                    }

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
                            const ServerCallContext &context, const PreparedStatementQuery &command,
                            const FlightDescriptor &descriptor) {
                        const std::string &prepared_statement_handle = command.prepared_statement_handle;

                        auto search = prepared_statements_.find(prepared_statement_handle);
                        if (search == prepared_statements_.end()) {
                            return Status::Invalid("Prepared statement not found");
                        }

                        std::shared_ptr<DuckDBStatement> statement = search->second;

                        ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());

                        return GetFlightInfoForCommand(descriptor, schema);
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
                            const ServerCallContext &context, const PreparedStatementQuery &command) {
                        const std::string &prepared_statement_handle = command.prepared_statement_handle;

                        auto search = prepared_statements_.find(prepared_statement_handle);
                        if (search == prepared_statements_.end()) {
                            return Status::Invalid("Prepared statement not found");
                        }

                        std::shared_ptr<DuckDBStatement> statement = search->second;

                        std::shared_ptr<DuckDBStatementBatchReader> reader;
                        reader = DuckDBStatementBatchReader::Create(statement).ValueOrDie();

                        return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
                    }

                    Status DoPutPreparedStatementQuery(const ServerCallContext &context,
                                                       const PreparedStatementQuery &command,
                                                       FlightMessageReader *reader,
                                                       FlightMetadataWriter *writer) {
                        const std::string &prepared_statement_handle = command.prepared_statement_handle;
                        ARROW_ASSIGN_OR_RAISE(
                                auto statement,
                                GetStatementByHandle(prepared_statement_handle));

                        ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

                        return Status::OK();
                    }

                    arrow::Result<int64_t> DoPutPreparedStatementUpdate(
                            const ServerCallContext &context, const PreparedStatementUpdate &command,
                            FlightMessageReader *reader) {
                        const std::string &prepared_statement_handle = command.prepared_statement_handle;
                        ARROW_ASSIGN_OR_RAISE(
                                auto statement,
                                GetStatementByHandle(prepared_statement_handle));

                        ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

                        return statement->ExecuteUpdate();
                    }

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
                            const ServerCallContext &context, const GetTables &command) {
                        std::string query = PrepareQueryForGetTables(command);
                        std::shared_ptr<DuckDBStatement> statement;
                        ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db_conn_, query));

                        std::shared_ptr<DuckDBStatementBatchReader> reader;
                        ARROW_ASSIGN_OR_RAISE(reader, DuckDBStatementBatchReader::Create(
                                statement, SqlSchema::GetTablesSchema()));
                        return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
                    }


                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
                            const ServerCallContext &context, const GetTables &command,
                            const FlightDescriptor &descriptor) {
                        std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd},
                                                                             {}}};

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
                        const duckdb::DBConfig &config,
                        const bool &print_queries
                ) {
                    std::shared_ptr<duckdb::DuckDB> db;
                    std::shared_ptr<duckdb::Connection> con;

                    db = std::make_shared<duckdb::DuckDB>(path);
                    con = std::make_shared<duckdb::Connection>(*db);

                    std::shared_ptr<Impl> impl = std::make_shared<Impl>(db, con, print_queries);
                    std::shared_ptr<DuckDBFlightSqlServer> result(new DuckDBFlightSqlServer(impl));

                    for (const auto &id_to_result: GetSqlInfoResultMap()) {
                        result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
                    }
                    return result;
                }

                DuckDBFlightSqlServer::~DuckDBFlightSqlServer() = default;

                arrow::Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(
                        const ServerCallContext &context, const StatementQuery &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoStatement(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(
                        const ServerCallContext &context, const StatementQueryTicket &command) {
                    return impl_->DoGetStatement(context, command);
                }

                arrow::Result<std::unique_ptr<FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoTables(
                        const ServerCallContext &context, const GetTables &command,
                        const FlightDescriptor &descriptor) {

                    return impl_->GetFlightInfoTables(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>> DuckDBFlightSqlServer::DoGetTables(
                        const ServerCallContext &context, const GetTables &command) {

                    return impl_->DoGetTables(context, command);
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
                        const ServerCallContext &context, const PreparedStatementQuery &command,
                        const FlightDescriptor &descriptor) {
                    return impl_->GetFlightInfoPreparedStatement(context, command, descriptor);
                }

                arrow::Result<std::unique_ptr<FlightDataStream>>
                DuckDBFlightSqlServer::DoGetPreparedStatement(const ServerCallContext &context,
                                                              const PreparedStatementQuery &command) {
                    return impl_->DoGetPreparedStatement(context, command);
                }

                Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(
                        const ServerCallContext &context, const PreparedStatementQuery &command,
                        FlightMessageReader *reader, FlightMetadataWriter *writer) {
                    return impl_->DoPutPreparedStatementQuery(context, command, reader, writer);
                }

                arrow::Result<int64_t> DuckDBFlightSqlServer::DoPutPreparedStatementUpdate(
                        const ServerCallContext &context, const PreparedStatementUpdate &command,
                        FlightMessageReader *reader) {
                    return impl_->DoPutPreparedStatementUpdate(context, command, reader);
                }


            }  // namespace sqlite
        }  // namespace sql
    }  // namespace flight
}  // namespace arrow
