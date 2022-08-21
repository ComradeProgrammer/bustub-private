
#pragma once

#include <string>
#include <vector>

#include "binder/parser.h"

namespace bustub {

class SelectStatement : public SQLStatement {
 public:
  explicit SelectStatement(Parser &parser, duckdb_libpgquery::PGSelectStmt *pg_stmt);

  string table_;
  vector<Column> columns_;
};

}  // namespace bustub
