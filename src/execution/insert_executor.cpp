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

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  if (done_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int32_t insert_count = 0;

  // Pull all tuples from child executor and insert them
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto inserted_rid = table_info->table_->InsertTuple(
        TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), false},
        child_tuple,
        exec_ctx_->GetLockManager(),
        exec_ctx_->GetTransaction(),
        plan_->GetTableOid());
    
    if (inserted_rid) {
      insert_count++;
      
      // Update all indexes for this table
      for (auto &index_info : indexes) {
        index_info->index_->InsertEntry(child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,index_info->index_->GetKeyAttrs()), *inserted_rid, exec_ctx_->GetTransaction());
      }
    }
  }

  // Return the insert count as a tuple containing a single integer
  std::vector<Value> values;
  values.push_back(Value(TypeId::INTEGER, insert_count));
  *tuple = Tuple(values, &GetOutputSchema());
  
  done_ = true;
  return true;
}

}  // namespace bustub
