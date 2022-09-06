//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
using std::move, std::string, std::cout, std::endl, std::vector;
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(move(left_executor)),
      right_executor_(move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_valid_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_valid_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    bool right_ok = right_executor_->Next(&right_tuple, &right_rid);

    if (!right_ok) {
      // switch to next tuple in left table if all tuples in the right table has been traversed
      bool left_ok = left_executor_->Next(&left_tuple_, &left_rid_);
      if (!left_ok) {
        return false;
      }
      right_executor_->Init();
      continue;
    }

    // whether this combination is wanted
    if (!plan_->Predicate()
             ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                            plan_->GetRightPlan()->OutputSchema())
             .GetAs<bool>()) {
      continue;
    }

    // then reconstruct the new tuple
    vector<Value> values;
    const Schema *output_schema = plan_->OutputSchema();
    for (uint32_t i = 0; i < output_schema->GetColumnCount(); i++) {
      const auto &column = output_schema->GetColumn(i);
      Value v = column.GetExpr()->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                               plan_->GetRightPlan()->OutputSchema());
      values.push_back(v);
    }
    Tuple output_tuple(values, output_schema);
    (*tuple) = output_tuple;
    return true;
  }
}

}  // namespace bustub
