//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
using std::move;
using std::vector;
namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  while (true) {
    vector<Value> group_bys = aht_iterator_.Key().group_bys_;
    vector<Value> aggrations = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;

    if (plan_->GetHaving() != nullptr) {
      // check having conition
      bool ok = plan_->GetHaving()->EvaluateAggregate(group_bys, aggrations).GetAs<bool>();
      if (!ok) {
        continue;
      }
    }

    vector<Value> values;
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      Value value = column.GetExpr()->EvaluateAggregate(group_bys, aggrations);
      values.push_back(value);
    }

    Tuple new_tuple(values, plan_->OutputSchema());
    *tuple = new_tuple;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
