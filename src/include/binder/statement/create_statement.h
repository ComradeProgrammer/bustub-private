
#pragma once

#include <string>
#include <vector>

#include "binder/parser.h"

namespace bustub {

class CreateStatement : public SQLStatement {
 public:
  explicit CreateStatement(Parser &parser, duckdb_libpgquery::PGCreateStmt *pg_stmt);

  string table_;
  vector<Column> columns_;
};

}  // namespace bustub
