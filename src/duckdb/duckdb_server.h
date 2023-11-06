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

#pragma once

#include <duckdb.hpp>

#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/flight/sql/server.h>

namespace arrow {
    namespace flight {
        namespace sql {
            namespace duckdbflight {

/// \brief Convert a column type to a ArrowType.
/// \param duckdb_type the duckdb type.
/// \return            The equivalent ArrowType.
                std::shared_ptr<DataType> GetArrowType(const char *duckdb_type);

/// \brief Example implementation of FlightSqlServerBase backed by an in-memory DuckDB
///        database.
                class DuckDBFlightSqlServer : public FlightSqlServerBase {
                public:
                    ~DuckDBFlightSqlServer() override;

                    static arrow::Result<std::shared_ptr<DuckDBFlightSqlServer>> Create(const std::string &path,
                                                                                        const duckdb::DBConfig &config,
                                                                                        const bool &print_queries);

                    /// \brief Auxiliary method used to execute an arbitrary SQL statement on the underlying
                    ///        SQLite database.
                    Status ExecuteSql(const std::string &sql);

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
                            const ServerCallContext &context, const StatementQuery &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
                            const ServerCallContext &context, const StatementQueryTicket &command) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
                            const ServerCallContext &context, const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
                            const ServerCallContext &context) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
                            const ServerCallContext &context, const GetDbSchemas &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
                            const ServerCallContext &context, const GetDbSchemas &command) override;

                    arrow::Result<int64_t> DoPutCommandStatementUpdate(
                            const ServerCallContext &context, const StatementUpdate &update) override;

                    arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
                            const ServerCallContext &context,
                            const ActionCreatePreparedStatementRequest &request) override;

                    Status ClosePreparedStatement(
                            const ServerCallContext &context,
                            const ActionClosePreparedStatementRequest &request) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
                            const ServerCallContext &context, const PreparedStatementQuery &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
                            const ServerCallContext &context, const PreparedStatementQuery &command) override;

                    Status DoPutPreparedStatementQuery(const ServerCallContext &context,
                                                       const PreparedStatementQuery &command,
                                                       FlightMessageReader *reader,
                                                       FlightMetadataWriter *writer) override;

                    arrow::Result<int64_t> DoPutPreparedStatementUpdate(
                            const ServerCallContext &context, const PreparedStatementUpdate &command,
                            FlightMessageReader *reader) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
                            const ServerCallContext &context, const GetTables &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
                            const ServerCallContext &context, const GetTables &command) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
                            const ServerCallContext &context, const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
                            const ServerCallContext &context) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
                            const ServerCallContext &context, const GetImportedKeys &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
                            const ServerCallContext &context, const GetImportedKeys &command) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
                            const ServerCallContext &context, const GetExportedKeys &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
                            const ServerCallContext &context, const GetExportedKeys &command) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
                            const ServerCallContext &context, const GetCrossReference &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
                            const ServerCallContext &context, const GetCrossReference &command) override;

                    arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
                            const ServerCallContext &context, const GetPrimaryKeys &command,
                            const FlightDescriptor &descriptor) override;

                    arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
                            const ServerCallContext &context, const GetPrimaryKeys &command) override;

                    arrow::Result<ActionBeginTransactionResult> BeginTransaction(
                            const ServerCallContext &context,
                            const ActionBeginTransactionRequest &request) override;

                    Status EndTransaction(const ServerCallContext &context,
                                          const ActionEndTransactionRequest &request) override;

                private:
                    class Impl;

                    std::shared_ptr<Impl> impl_;

                    explicit DuckDBFlightSqlServer(std::shared_ptr<Impl> impl);
                };

            }  // namespace duckdbflight
        }  // namespace sql
    }  // namespace flight
}  // namespace arrow
