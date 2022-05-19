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

#include "duckdb_statement_batch_reader.h"

#include <duckdb.h>

#include <iostream>

#include "arrow/builder.h"
#include <arrow/c/bridge.h>

#include "duckdb_statement.h"
// #include <duckdb/main/capi_internal.hpp>

// #define STRING_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                             \
//   case TYPE_CLASS##Type::type_id: {                                               \
//     int bytes = sqlite3_column_bytes(STMT, COLUMN);                               \
//     const unsigned char* string = sqlite3_column_text(STMT, COLUMN);              \
//     if (string == nullptr) {                                                      \
//       ARROW_RETURN_NOT_OK(                                                        \
//           (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).AppendNull());        \
//       break;                                                                      \
//     }                                                                             \
//     ARROW_RETURN_NOT_OK(                                                          \
//         (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).Append(string, bytes)); \
//     break;                                                                        \
//   }

// #define BINARY_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                  \
//   case TYPE_CLASS##Type::type_id: {                                                    \
//     int bytes = sqlite3_column_bytes(STMT, COLUMN);                                    \
//     const void* blob = sqlite3_column_blob(STMT, COLUMN);                              \
//     if (blob == nullptr) {                                                             \
//       ARROW_RETURN_NOT_OK(                                                             \
//           (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).AppendNull());             \
//       break;                                                                           \
//     }                                                                                  \
//     ARROW_RETURN_NOT_OK(                                                               \
//         (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).Append((char*)blob, bytes)); \
//     break;                                                                             \
//   }

// #define INT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                         \
//   case TYPE_CLASS##Type::type_id: {                                        \
//     if (sqlite3_column_type(stmt_, i) == SQLITE_NULL) {                    \
//       ARROW_RETURN_NOT_OK(                                                 \
//           (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).AppendNull()); \
//       break;                                                               \
//     }                                                                      \
//     sqlite3_int64 value = sqlite3_column_int64(STMT, COLUMN);              \
//     ARROW_RETURN_NOT_OK(                                                   \
//         (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).Append(value));  \
//     break;                                                                 \
//   }

// #define FLOAT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                       \
//   case TYPE_CLASS##Type::type_id: {                                        \
//     if (sqlite3_column_type(stmt_, i) == SQLITE_NULL) {                    \
//       ARROW_RETURN_NOT_OK(                                                 \
//           (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).AppendNull()); \
//       break;                                                               \
//     }                                                                      \
//     double value = sqlite3_column_double(STMT, COLUMN);                    \
//     ARROW_RETURN_NOT_OK(                                                   \
//         (reinterpret_cast<TYPE_CLASS##Builder&>(builder)).Append(value));  \
//     break;                                                                 \
//   }

namespace arrow {
namespace flight {
namespace sql {
namespace duckdbflight {

// // Batch size for SQLite statement results
static constexpr int kMaxBatchSize = 1024;

std::shared_ptr<Schema> DuckDBStatementBatchReader::schema() const { return schema_; }

DuckDBStatementBatchReader::DuckDBStatementBatchReader(
    std::shared_ptr<DuckDBStatement> statement, std::shared_ptr<Schema> schema)
    : statement_(std::move(statement)),
      schema_(std::move(schema)),
      rc_(DuckDBSuccess),
      already_executed_(false) {}

arrow::Result<std::shared_ptr<DuckDBStatementBatchReader>>
DuckDBStatementBatchReader::Create(const std::shared_ptr<DuckDBStatement>& statement_) {
  // ARROW_RETURN_NOT_OK(statement_->Step());
  // ARROW_RETURN_NOT_OK(statement_->Execute());

  ARROW_ASSIGN_OR_RAISE(auto schema, statement_->GetSchema());

  std::shared_ptr<DuckDBStatementBatchReader> result(
      new DuckDBStatementBatchReader(statement_, schema));

  return result;
}

arrow::Result<std::shared_ptr<DuckDBStatementBatchReader>>
DuckDBStatementBatchReader::Create(const std::shared_ptr<DuckDBStatement>& statement,
                                   const std::shared_ptr<Schema>& schema) {
  std::shared_ptr<DuckDBStatementBatchReader> result(
      new DuckDBStatementBatchReader(statement, schema));

  return result;
}

Status DuckDBStatementBatchReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  std::shared_ptr<duckdb::PreparedStatement> stmt_ = statement_->GetDuckDBStmt();

  std::cout << "QUERY: " << stmt_->query << std::endl;

  // const int num_fields = schema_->num_fields();
  // std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);

  // for (int i = 0; i < num_fields; i++) {
  //   const std::shared_ptr<Field>& field = schema_->field(i);
  //   const std::shared_ptr<DataType>& field_type = field->type();

  //   ARROW_RETURN_NOT_OK(MakeBuilder(default_memory_pool(), field_type, &builders[i]));
  // }

  if (!already_executed_) {
    ARROW_ASSIGN_OR_RAISE(rc_, statement_->Execute());
    std::cout << rc_ << std::endl;
    already_executed_ = true;
  }

  ARROW_ASSIGN_OR_RAISE(auto result, statement_->GetResult());
  ARROW_ASSIGN_OR_RAISE(auto result_schema, statement_->GetArrowSchema());
  // int64_t cols = duckdb_arrow_column_count((duckdb_arrow*)&result);
  // int64_t rows = duckdb_arrow_row_count((duckdb_arrow*)&result);
  int64_t cols = result_schema->n_children;
  int64_t rows = result->length;

  printf("No of cols: %d, no of rows: %d\n", cols, rows);



  // std::vector<std::shared_ptr<Array>> arrays(builders.size());

  // // for (int i = 0; i < cols; ++i) {
  //   ArrowArray * result_array = new ArrowArray();
  //   ArrowSchema * result_schema = new ArrowSchema();

  //   // printf("Iter: %d ", i);
  //   int rc_array = duckdb_query_arrow_array(result, (duckdb_arrow_array*)&result_array);
  //   int rc_schema = duckdb_query_arrow_schema(result, (duckdb_arrow_schema *)&result_schema);

  //   printf("RC_Array: %d, RC_Schema: %d \n", rc_array, rc_schema);

  //   // auto wrapper = (duckdb::ArrowResultWrapper *) result;
	//   // return wrapper->result->collection;

  //   printf("Length: %d\n", result_array->length);
  // auto test = (ArrowArray*)&result;

  //   // *out = arrow::ImportRecordBatch(result_array, result_schema);
    
    arrow::Result<std::shared_ptr<RecordBatch>> test = nullptr;
    // std::shared_ptr<RecordBatch> test_res = nullptr;
    test = arrow::ImportRecordBatch(result.get(), result_schema.get());
    // std::cout << "STATUS: " << test.status() << std::endl;
    
    if (test.status() == arrow::Status::OK()) {
      *out = test.ValueOrDie();

      // // print the table
      // std::cout << (*out)->ToString() << std::endl;

    } else {
      *out = NULLPTR;
    }

    // ARROW_ASSIGN_OR_RAISE(auto tst, arrow::ImportRecordBatch(result.get(), result_schema.get()));
    
    // ARROW_ASSIGN_OR_RAISE(*out, arrow::ImportRecordBatch((ArrowArray*)&result, (ArrowSchema*)&result_schema));



  //   // printf("%s\n", result_array->release());

  // // while (rows < kMaxBatchSize && rc_ == SQLITE_ROW) {
  // //   rows++;
  //   // for (int j = 0; j < num_fields; ++j) {
  //   //   const std::shared_ptr<Field>& field = schema_->field(i);
  //   //   const std::shared_ptr<DataType>& field_type = field->type();
  //     // printf("%d\n", (ArrowArray*)&result_array->length);
  //     // Array test = (Array) *result_array;

  //     // std::cout << test->ToString() << std::endl;
  //     // arrays.push_back(std::make_shared<Array>(*test));

  // //     ArrayBuilder& builder = *builders[i];

  // //     // NOTE: This is not the optimal way of building Arrow vectors.
  // //     // That would be to presize the builders to avoiding several resizing operations
  // //     // when appending values and also to build one vector at a time.
  // //     switch (field_type->id()) {
  // //       INT_BUILDER_CASE(Int64, stmt_, i)
  // //       INT_BUILDER_CASE(UInt64, stmt_, i)
  // //       INT_BUILDER_CASE(Int32, stmt_, i)
  // //       INT_BUILDER_CASE(UInt32, stmt_, i)
  // //       INT_BUILDER_CASE(Int16, stmt_, i)
  // //       INT_BUILDER_CASE(UInt16, stmt_, i)
  // //       INT_BUILDER_CASE(Int8, stmt_, i)
  // //       INT_BUILDER_CASE(UInt8, stmt_, i)
  // //       FLOAT_BUILDER_CASE(Double, stmt_, i)
  // //       FLOAT_BUILDER_CASE(Float, stmt_, i)
  // //       FLOAT_BUILDER_CASE(HalfFloat, stmt_, i)
  // //       BINARY_BUILDER_CASE(Binary, stmt_, i)
  // //       BINARY_BUILDER_CASE(LargeBinary, stmt_, i)
  // //       STRING_BUILDER_CASE(String, stmt_, i)
  // //       STRING_BUILDER_CASE(LargeString, stmt_, i)
  // //       default:
  // //         return Status::NotImplemented("Not implemented SQLite data conversion to ",
  // //                                       field_type->name());
  // //     }
  //   // }

  // // //   ARROW_ASSIGN_OR_RAISE(rc_, statement_->Step());
  //   // result_array->release(result_array);
  //   delete result_array;
  // // }

  // // if (rows > 0) {
  // //   std::vector<std::shared_ptr<Array>> arrays(builders.size());
  // //   for (int i = 0; i < num_fields; i++) {
  // //     ARROW_RETURN_NOT_OK(builders[i]->Finish(&arrays[i]));
  // //   }

  // //   *out = RecordBatch::Make(schema_, rows, arrays);
  // // } else {
  // //   *out = NULLPTR;
  // // }

  return Status::OK();
}

}  // namespace duckdbflight
}  // namespace sql
}  // namespace flight
}  // namespace arrow
