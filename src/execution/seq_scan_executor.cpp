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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  exec_ctx_ = exec_ctx;
}

void SeqScanExecutor::Init() { 
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  while (!table_iterator_->IsEnd()) {
    auto [meta, curr_tuple] = table_info->table_->GetTuple(table_iterator_->GetRID());
    
    if (meta.is_deleted_) {
      ++(*table_iterator_);
      continue;
    }
    
    if (plan_->filter_predicate_ != nullptr) {
      auto eval_result = plan_->filter_predicate_->Evaluate(&curr_tuple, table_info->schema_);
      if (!eval_result.GetAs<bool>()) {
        ++(*table_iterator_);
        continue;
      }
    }
    
    *tuple = curr_tuple;
    *rid = table_iterator_->GetRID();
    ++(*table_iterator_);
    return true;
  }
  return false;
}


}  // namespace bustub
