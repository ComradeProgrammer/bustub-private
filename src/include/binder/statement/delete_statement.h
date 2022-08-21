
#pragma once

#include <string>

#include "binder/parser.h"

namespace bustub {

class DeleteStatement : public SQLStatement {
 public:
  explicit DeleteStatement(Parser &parser, duckdb_libpgquery::PGDeleteStmt *pg_stmt);

  string table_;
};

}  // namespace bustub
