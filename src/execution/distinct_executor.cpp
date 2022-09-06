//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
using std::move;
namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)) {}

void DistinctExecutor::Init() {
  keys_.clear();
  child_executor_->Init();
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple output_tuple;
  RID output_rid;
  while (child_executor_->Next(&output_tuple, &output_rid)) {
    auto distinct_key = GetDistinctKey(output_tuple);
    if (keys_.find(distinct_key) == keys_.end()) {
      *tuple = output_tuple;
      *rid = output_rid;
      keys_.insert(distinct_key);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
