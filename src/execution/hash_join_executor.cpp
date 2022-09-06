//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
using std::vector, std::string;
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(move(left_child)), right_child_(move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  // construct the hashtable
  Tuple left_tuple;
  RID left_rid;
  hash_table_.clear();
  buffer_.clear();
  while (left_child_->Next(&left_tuple, &left_rid)) {
    Value left_join_key = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    hash_table_[{left_join_key}].push_back(left_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!buffer_.empty()) {
    *tuple = JoinTuple(buffer_[buffer_.size() - 1], right_tuple_);
    buffer_.pop_back();
    return true;
  }
  RID tmp;
  while (right_child_->Next(&right_tuple_, &tmp)) {
    Value right_join_key =
        plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, plan_->GetRightPlan()->OutputSchema());
    if (hash_table_.find({right_join_key}) == hash_table_.end()) {
      continue;
    }
    buffer_ = hash_table_[{right_join_key}];
    *tuple = JoinTuple(buffer_[buffer_.size() - 1], right_tuple_);
    buffer_.pop_back();
    return true;
  }
  return false;
}

Tuple HashJoinExecutor::JoinTuple(const Tuple &left, const Tuple &right) {
  vector<Value> values;
  const Schema *output_schema = plan_->OutputSchema();
  for (uint32_t i = 0; i < output_schema->GetColumnCount(); i++) {
    const auto &column = output_schema->GetColumn(i);
    Value v = column.GetExpr()->EvaluateJoin(&left, plan_->GetLeftPlan()->OutputSchema(), &right,
                                             plan_->GetRightPlan()->OutputSchema());
    values.push_back(v);
  }
  Tuple output_tuple(values, output_schema);
  return output_tuple;
}

}  // namespace bustub
