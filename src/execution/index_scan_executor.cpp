//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/index/b_plus_tree_index.h"
#include <unordered_set>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->GetIndexOid());
  auto table_info = catalog->GetTable(plan_->table_oid_);

  index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  current_index_ = 0;
  scan_rids_.clear();
  bool is_point_lookup_ = (plan_->filter_predicate_ != nullptr);

  if (is_point_lookup_) {
    // Point lookup
    if (!plan_->pred_keys_.empty()) {
      // For each pred_key, perform a ScanKey and collect results
      // Use a set to avoid duplicates from multiple OR conditions
      std::unordered_set<uint64_t> rid_set;
      
      for (const auto &key_expr : plan_->pred_keys_) {
        std::vector<Value> key_values;
        key_values.push_back(key_expr->Evaluate(nullptr, table_info->schema_));
        Tuple key_tuple(key_values, &index_info->key_schema_);
        
        std::vector<RID> result_rids;
        index_->ScanKey(key_tuple, &result_rids, exec_ctx_->GetTransaction());
        
        for (const auto &rid : result_rids) {
          rid_set.insert(rid.Get());
        }
      }
      
      // Convert set back to vector
      for (auto rid_val : rid_set) {
        scan_rids_.push_back(RID(rid_val));
      }
    }
  } else {
    // Ordered scan
    auto iter = index_->GetBeginIterator();
    
    while (!iter.IsEnd()) {
      auto [key, rid] = *iter;
      RID rid_copy = rid;
      scan_rids_.push_back(rid_copy);
      ++iter;
    }
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

  // Iterate through the RIDs we collected in Init
  while (current_index_ < scan_rids_.size()) {
    RID current_rid = scan_rids_[current_index_++];
    
    auto [meta, current_tuple] = table_info->table_->GetTuple(current_rid);
    
    if (meta.is_deleted_) {
      continue;
    }
    
    *tuple = current_tuple;
    *rid = current_rid;
    return true;
  }
  return false;
}

}  // namespace bustub
