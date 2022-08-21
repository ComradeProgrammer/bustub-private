#include "binder/statement/select_statement.h"

namespace bustub {

SelectStatement::SelectStatement(Parser &parser, duckdb_libpgquery::PGSelectStmt *pg_stmt)
    : SQLStatement(StatementType::SELECT_STATEMENT) {
  bool found = false;

  // Extract the table name from the FROM clause.
  for (auto c = pg_stmt->fromClause->head; c != nullptr; c = lnext(c)) {
    auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
    switch (node->type) {
      case duckdb_libpgquery::T_PGRangeVar: {
        if (found) {
          throw Exception("shouldnt have multiple tables");
        }
        auto *table_ref = reinterpret_cast<duckdb_libpgquery::PGRangeVar *>(node);
        table_ = table_ref->relname;
        found = true;
        break;
      }
      default:
        throw Exception("Found unknown node type");
    }
  }
}

}  // namespace bustub
