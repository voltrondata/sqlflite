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

#include "duckdb_statement.h"

#include <duckdb.h>
#include <iostream>

#include <boost/algorithm/string.hpp>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/c/bridge.h>
#include "duckdb_server.h"

using duckdb::QueryResult;

namespace arrow {
namespace flight {
namespace sql {
namespace duckdbflight {

std::shared_ptr<DataType> GetDataTypeFromDuckDbType(
  const duckdb::LogicalTypeId column_type,
  const duckdb::LogicalType column
) {
  switch (column_type) {
    case duckdb::LogicalTypeId::INTEGER:
      return int32();
    case duckdb::LogicalTypeId::DECIMAL: {
        uint8_t width = 0;
        uint8_t scale = 0;
        bool dec_properties = column.GetDecimalProperties(width, scale);
        return decimal(scale, width);
      }
    case duckdb::LogicalTypeId::FLOAT:
      return float32();
    case duckdb::LogicalTypeId::DOUBLE:
      return float64();
    case duckdb::LogicalTypeId::CHAR:
      return utf8();
    case duckdb::LogicalTypeId::VARCHAR:
      return large_utf8();
    case duckdb::LogicalTypeId::BLOB:
      return binary();
    case duckdb::LogicalTypeId::TINYINT:
      return int8();
    case duckdb::LogicalTypeId::SMALLINT:
      return int16();
    case duckdb::LogicalTypeId::BIGINT:
      return int64();
    case duckdb::LogicalTypeId::BOOLEAN:
      return boolean();
    case duckdb::LogicalTypeId::DATE:
      return date64();
    case duckdb::LogicalTypeId::TIME:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
      return timestamp(arrow::TimeUnit::MILLI);
    case duckdb::LogicalTypeId::TIMESTAMP:
      return timestamp(arrow::TimeUnit::MICRO);
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      return timestamp(arrow::TimeUnit::SECOND);
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return timestamp(arrow::TimeUnit::NANO);
    case duckdb::LogicalTypeId::INTERVAL:
      return duration(arrow::TimeUnit::MICRO); // ASSUMING MICRO AS DUCKDB's DOCS DOES NOT SPECIFY
    case duckdb::LogicalTypeId::UTINYINT:
      return uint8();
    case duckdb::LogicalTypeId::USMALLINT:
      return uint16();
    case duckdb::LogicalTypeId::UINTEGER:
      return uint32();
    case duckdb::LogicalTypeId::UBIGINT:
      return uint64();
    case duckdb::LogicalTypeId::INVALID:
    case duckdb::LogicalTypeId::SQLNULL:
    case duckdb::LogicalTypeId::UNKNOWN:
    case duckdb::LogicalTypeId::ANY:
    case duckdb::LogicalTypeId::USER:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
    case duckdb::LogicalTypeId::TIME_TZ:
    case duckdb::LogicalTypeId::JSON:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::POINTER:
    case duckdb::LogicalTypeId::HASH:
    case duckdb::LogicalTypeId::VALIDITY:
    case duckdb::LogicalTypeId::UUID:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::TABLE:
    case duckdb::LogicalTypeId::ENUM:
    case duckdb::LogicalTypeId::AGGREGATE_STATE:
    default:
      return null();
  }
}


// int32_t GetPrecisionFromColumn(int column_type) {
//   switch (column_type) {
//     case SQLITE_INTEGER:
//       return 10;
//     case SQLITE_FLOAT:
//       return 15;
//     case SQLITE_NULL:
//     default:
//       return 0;
//   }
// }

// ColumnMetadata GetColumnMetadata(int column_type, const char* table) {
//   ColumnMetadata::ColumnMetadataBuilder builder = ColumnMetadata::Builder();

//   builder.Scale(15).IsAutoIncrement(false).IsReadOnly(false);

//   if (table == NULLPTR) {
//     return builder.Build();
//   } else if (column_type == SQLITE_TEXT || column_type == SQLITE_BLOB) {
//     std::string table_name(table);
//     builder.TableName(table_name);
//   } else {
//     std::string table_name(table);
//     builder.TableName(table_name).Precision(GetPrecisionFromColumn(column_type));
//   }
//   return builder.Build();
// }

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    std::shared_ptr<duckdb::Connection> con, const std::string& sql) {
  std::shared_ptr<duckdb::PreparedStatement> stmt = con->Prepare(sql);
  std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(con, stmt));
  return result;
}

// arrow::Result<std::shared_ptr<Schema>> DuckDBStatement::GetSchema() const {
//   std::vector<std::shared_ptr<Field>> fields;

//   int column_count = sqlite3_column_count(stmt_);

//   for (int i = 0; i < column_count; i++) {
//     const char* column_name = sqlite3_column_name(stmt_, i);

//     // SQLite does not always provide column types, especially when the statement has not
//     // been executed yet. Because of this behaviour this method tries to get the column
//     // types in two attempts:
//     // 1. Use sqlite3_column_type(), which return SQLITE_NULL if the statement has not
//     //    been executed yet
//     // 2. Use sqlite3_column_decltype(), which returns correctly if given column is
//     //    declared in the table.
//     // Because of this limitation, it is not possible to know the column types for some
//     // prepared statements, in this case it returns a dense_union type covering any type
//     // SQLite supports.
//     const int column_type = sqlite3_column_type(stmt_, i);
//     const char* table = sqlite3_column_table_name(stmt_, i);
//     std::shared_ptr<DataType> data_type = GetDataTypeFromSqliteType(column_type);

//     if (data_type->id() == Type::NA) {
//       // Try to retrieve column type from sqlite3_column_decltype
//       const char* column_decltype = sqlite3_column_decltype(stmt_, i);

//       if (column_decltype != NULLPTR) {
//         data_type = GetArrowType(column_decltype);
//       } else {
//         // If it can not determine the actual column type, return a dense_union type
//         // covering any type SQLite supports.
//         data_type = GetUnknownColumnDataType();
//       }
//     }
//     ColumnMetadata column_metadata = GetColumnMetadata(column_type, table);

//     fields.push_back(
//         arrow::field(column_name, data_type, column_metadata.metadata_map()));
//   }
//   return arrow::schema(fields);
// }

DuckDBStatement::~DuckDBStatement() { 
  // duckdb_destroy_prepare(&stmt_); 
}

arrow::Result<int> DuckDBStatement::Execute() {
  printf("Executing...\n");
  auto res = stmt_->Execute();

  ArrowArray res_arr;
  ArrowSchema res_schema;
  
  QueryResult::ToArrowSchema(&res_schema, res->types, res->names);
  res->Fetch()->ToArrowArray(&res_arr);
  ARROW_ASSIGN_OR_RAISE(result_, arrow::ImportRecordBatch(&res_arr, &res_schema));
  schema_ = result_->schema();

  // // std::cout << "POOF" << std::endl;
  // QueryResult::ToArrowSchema(&res_schema, res->types, res->names);

  // ARROW_ASSIGN_OR_RAISE(schema_, arrow::ImportSchema(&res_schema));
  // // schema_ = std::make_shared<ArrowSchema>(res_schema);

  // std::cout << "SCHEMA: " << schema_->ToString() << std::endl;

  return 0;
}

arrow::Result<std::shared_ptr<RecordBatch>> DuckDBStatement::GetResult() {
  return result_;
}

arrow::Result<std::shared_ptr<Schema>> DuckDBStatement::GetSchema() const {
  // return schema_;
  // std::vector<std::shared_ptr<Field>> fields;

  // int column_count = stmt_->ColumnCount();
  // auto column_names = stmt_->GetNames();
  // auto column_types = stmt_->GetTypes();

  // // int i = 0;
  // // for (std::string nm : column_names) {
  // //   std::cout << nm << ": " << column_types[i].ToString() << GetDataTypeFromDuckDbType(column_types[i].id(), column_types[i]) << std::endl;
  // //   i++;
  // // }

  // for (int i = 0; i < column_count; i++) {
  //   std::string column_name = column_names[i];
  //   std::shared_ptr<arrow::DataType> data_type = GetDataTypeFromDuckDbType(column_types[i].id(), column_types[i]);
  // //   const char* column_name = sqlite3_column_name(stmt_, i);

  // //   // SQLite does not always provide column types, especially when the statement has not
  // //   // been executed yet. Because of this behaviour this method tries to get the column
  // //   // types in two attempts:
  // //   // 1. Use sqlite3_column_type(), which return SQLITE_NULL if the statement has not
  // //   //    been executed yet
  // //   // 2. Use sqlite3_column_decltype(), which returns correctly if given column is
  // //   //    declared in the table.
  // //   // Because of this limitation, it is not possible to know the column types for some
  // //   // prepared statements, in this case it returns a dense_union type covering any type
  // //   // SQLite supports.
  // //   const int column_type = sqlite3_column_type(stmt_, i);
  // //   const char* table = sqlite3_column_table_name(stmt_, i);
  // //   std::shared_ptr<DataType> data_type = GetDataTypeFromSqliteType(column_type);

  // //   if (data_type->id() == Type::NA) {
  // //     // Try to retrieve column type from sqlite3_column_decltype
  // //     const char* column_decltype = sqlite3_column_decltype(stmt_, i);

  // //     if (column_decltype != NULLPTR) {
  // //       data_type = GetArrowType(column_decltype);
  // //     } else {
  // //       // If it can not determine the actual column type, return a dense_union type
  // //       // covering any type SQLite supports.
  // //       data_type = GetUnknownColumnDataType();
  // //     }
  // //   }
  //   // ColumnMetadata column_metadata = GetColumnMetadata(column_type, table);

  // //   fields.push_back(
  // //       arrow::field(column_name, data_type, column_metadata.metadata_map()));
  // }
  // return arrow::schema(fields);

  return schema_;
}

// arrow::Result<int> DuckDBStatement::Step() {
//   int rc = sqlite3_step(stmt_);
//   if (rc == SQLITE_ERROR) {
//     return Status::ExecutionError("A SQLite runtime error has occurred: ",
//                                   sqlite3_errmsg(db_));
//   }

//   return rc;
// }

// arrow::Result<int> DuckDBStatement::Reset() {
//   int rc = sqlite3_reset(stmt_);
//   if (rc == SQLITE_ERROR) {
//     return Status::ExecutionError("A SQLite runtime error has occurred: ",
//                                   sqlite3_errmsg(db_));
//   }

//   return rc;
// }

std::shared_ptr<duckdb::PreparedStatement> DuckDBStatement::GetDuckDBStmt() const { return stmt_; }

// arrow::Result<int64_t> DuckDBStatement::ExecuteUpdate() {
//   ARROW_RETURN_NOT_OK(Step());
//   return sqlite3_changes(db_);
// }

}  // namespace sqlite
}  // namespace sql
}  // namespace flight
}  // namespace arrow
