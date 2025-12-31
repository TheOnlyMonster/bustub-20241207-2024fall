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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid()).get()),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  int32_t update_count = 0;

  // Pull all tuples from child executor and update them
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // Evaluate target expressions to get new column values
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    // Create new tuple with updated values
    Tuple new_tuple(values, &table_info_->schema_);

    // First, remove the old tuple from all indexes
    for (auto &index_info : indexes) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                    index_info->index_->GetKeyAttrs()),
          child_rid, exec_ctx_->GetTransaction());
    }

    // Delete the old tuple from the table
    table_info_->table_->UpdateTupleMeta(
        TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), true}, child_rid);

    // Insert the new tuple
    auto new_rid = table_info_->table_->InsertTuple(
        TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), false},
        new_tuple,
        exec_ctx_->GetLockManager(),
        exec_ctx_->GetTransaction(),
        plan_->GetTableOid());

    if (new_rid) {
      update_count++;

      // Update all indexes with the new tuple
      for (auto &index_info : indexes) {
        index_info->index_->InsertEntry(
            new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                    index_info->index_->GetKeyAttrs()),
            *new_rid, exec_ctx_->GetTransaction());
      }
    }
  }

  // Return the update count as a tuple containing a single integer
  std::vector<Value> result_values;
  result_values.push_back(Value(TypeId::INTEGER, update_count));
  *tuple = Tuple(result_values, &GetOutputSchema());

  done_ = true;
  return true;
}

}  // namespace bustub
