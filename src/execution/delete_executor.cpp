//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t table_oid = plan->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *unused_a, RID *unused_b) -> bool {
  RID rid;
  Tuple old_tuple;
  Transaction *txn = exec_ctx_->GetTransaction();

  if (!child_executor_->Next(&old_tuple, &rid)) {
    return false;
  }

  if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
    exec_ctx_->GetLockManager()->LockUpgrade(txn, rid);
  } else if (txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()) {
    exec_ctx_->GetLockManager()->LockExclusive(txn, rid);
  }

  bool ok = table_info_->table_->MarkDelete(rid, exec_ctx_->GetTransaction());
  if (!ok) {
    throw Exception(std::string("DeleteExecutor::Next: failed to delete ") +
                    old_tuple.ToString(plan_->GetChildPlan()->OutputSchema()));
  }
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (IndexInfo *index : indexes) {
    auto old_index_key = old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());

    index->index_->DeleteEntry(old_index_key, rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
