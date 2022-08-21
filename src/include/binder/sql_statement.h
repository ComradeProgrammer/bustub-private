//===----------------------------------------------------------------------===//
//                         BusTub
//
// bustub/binder/sql_statement.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/enums/statement_type.h"
#include "common/exception.h"

namespace bustub {

//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
 public:
  explicit SQLStatement(StatementType type);
  virtual ~SQLStatement() = default;

  //! The statement type
  StatementType type_;

  //! The statement location within the query string
  int32_t stmt_location_;

  //! The statement length within the query string
  int32_t stmt_length_;

  //! The query text that corresponds to this SQL statement
  std::string query_;

  //  protected:
  //   SQLStatement(const SQLStatement &other) = default;

  //  public:
  //   virtual string ToString() const { throw Exception("ToString not supported for this type of SQLStatement"); }
  //   //! Create a copy of this SelectStatement
  //   virtual unique_ptr<SQLStatement> Copy() const = 0;
};

}  // namespace bustub
