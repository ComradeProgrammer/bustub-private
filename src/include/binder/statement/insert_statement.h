
#pragma once

#include <string>
#include <vector>

#include "binder/parser.h"

namespace bustub {

class InsertStatement : public SQLStatement {
 public:
  explicit InsertStatement(Parser &parser, duckdb_libpgquery::PGInsertStmt *pg_stmt);

  string table_;
  vector<Column> columns_;
  vector<vector<Value>> values_;
};

}  // namespace bustub
