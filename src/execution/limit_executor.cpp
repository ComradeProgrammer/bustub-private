//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
using std::move;
namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  ptr_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple output_tuple;
  RID output_rid;
  while (child_executor_->Next(&output_tuple, &output_rid) && ptr_ < plan_->GetLimit()) {
    *tuple = output_tuple;
    *rid = output_rid;
    ptr_++;
    return true;
  }
  return false;
}

}  // namespace bustub
