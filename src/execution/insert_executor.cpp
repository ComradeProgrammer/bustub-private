//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
using std::cout, std::endl;
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_oid_t table_oid = plan->TableOid();
  table_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
}

void InsertExecutor::Init() {
  if (plan_->IsRawInsert()) {
    raw_insert_ptr_ = 0;
  } else {
    child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *unused_a, RID *unused_b) -> bool {
  RID new_rid;
  Tuple new_tuple;
  Transaction *txn = exec_ctx_->GetTransaction();

  if (plan_->IsRawInsert()) {
    if (raw_insert_ptr_ == plan_->RawValues().size()) {
      return false;
    }
    auto raw_value = plan_->RawValuesAt(raw_insert_ptr_);
    new_tuple = Tuple(raw_value, &(table_->schema_));
    raw_insert_ptr_++;
  } else {
    if (!child_executor_->Next(&new_tuple, &new_rid)) {
      return false;
    }
  }

  bool ok = table_->table_->InsertTuple(new_tuple, &new_rid, exec_ctx_->GetTransaction());
  if (!ok) {
    throw Exception(std::string("InsertExecutor::Next: failed to insert ") +
                    new_tuple.ToString(plan_->GetChildPlan()->OutputSchema()));
  }

  exec_ctx_->GetLockManager()->LockExclusive(txn, new_rid);

  // update index
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_);
  for (IndexInfo *index : indexes) {
    auto index_key = new_tuple.KeyFromTuple(table_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->InsertEntry(index_key, new_rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
