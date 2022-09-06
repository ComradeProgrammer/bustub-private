//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t table_oid = plan->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *unused_a, RID *unused_b) -> bool {
  RID rid;
  Tuple old_tuple;
  if (!child_executor_->Next(&old_tuple, &rid)) {
    return false;
  }
  Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
  bool ok = table_info_->table_->UpdateTuple(new_tuple, rid, exec_ctx_->GetTransaction());
  if (!ok) {
    throw Exception(std::string("UpdateExecutor::Next: failed to update ") +
                    new_tuple.ToString(plan_->GetChildPlan()->OutputSchema()));
  }
  // update index
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  for (IndexInfo *index : indexes) {
    auto old_index_key = old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    auto new_index_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());

    index->index_->DeleteEntry(old_index_key, rid, exec_ctx_->GetTransaction());
    index->index_->InsertEntry(new_index_key, rid, exec_ctx_->GetTransaction());
  }
  return true;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
