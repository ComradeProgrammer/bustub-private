//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
using std::cout, std::endl, std::vector, std::string;
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iterator_(nullptr, RID{}, nullptr) {
  table_oid_t table_id = plan_->GetTableOid();
  table_ = exec_ctx_->GetCatalog()->GetTable(table_id);
}

void SeqScanExecutor::Init() { iterator_ = table_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const Schema *output_schema = plan_->OutputSchema();
  while (iterator_ != table_->table_->End()) {
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(&(*iterator_), &(table_->schema_)).GetAs<bool>()) {
      // we need to construct it based on output schema
      Tuple old_tuple = *iterator_;

      vector<Value> values;
      for (uint32_t i = 0; i < output_schema->GetColumnCount(); i++) {
        values.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(&old_tuple, &table_->schema_));
        // string column_name = output_schema->GetColumn(i).GetName();
        // values.push_back(old_tuple.GetValue(&table_->schema_, table_->schema_.GetColIdx(column_name)));
      }
      Tuple output_tuple(values, output_schema);

      (*tuple) = output_tuple;

      *rid = iterator_->GetRid();
      ++iterator_;
      return true;
    }
    ++iterator_;
  }

  return false;
}
}  // namespace bustub
