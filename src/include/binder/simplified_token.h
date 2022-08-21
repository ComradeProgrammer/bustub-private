//===----------------------------------------------------------------------===//
//                         BusTub
//
// bustub/binder/simplified_token.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <string>

namespace bustub {

//! Simplified tokens are a simplified (dense) representation of the lexer
//! Used for simple syntax highlighting in the tests
enum class SimplifiedTokenType : uint8_t {
  SIMPLIFIED_TOKEN_IDENTIFIER,
  SIMPLIFIED_TOKEN_NUMERIC_CONSTANT,
  SIMPLIFIED_TOKEN_STRING_CONSTANT,
  SIMPLIFIED_TOKEN_OPERATOR,
  SIMPLIFIED_TOKEN_KEYWORD,
  SIMPLIFIED_TOKEN_COMMENT
};

struct SimplifiedToken {
  SimplifiedTokenType type_;
  int32_t start_;
};

enum class KeywordCategory : uint8_t { KEYWORD_RESERVED, KEYWORD_UNRESERVED, KEYWORD_TYPE_FUNC, KEYWORD_COL_NAME };

struct ParserKeyword {
  std::string name_;
  KeywordCategory category_;
};

}  // namespace bustub
