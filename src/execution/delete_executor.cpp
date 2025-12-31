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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int32_t delete_count = 0;

  // Pull all tuples from child executor and delete them
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // First, remove the tuple from all indexes
    for (auto &index_info : indexes) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                    index_info->index_->GetKeyAttrs()),
          child_rid, exec_ctx_->GetTransaction());
    }

    // Mark the tuple as deleted
    table_info->table_->UpdateTupleMeta(
        TupleMeta{exec_ctx_->GetTransaction()->GetTransactionTempTs(), true}, child_rid);

    delete_count++;
  }

  // Return the delete count as a tuple containing a single integer
  std::vector<Value> result_values;
  result_values.push_back(Value(TypeId::INTEGER, delete_count));
  *tuple = Tuple(result_values, &GetOutputSchema());

  done_ = true;
  return true;
}

}  // namespace bustub
