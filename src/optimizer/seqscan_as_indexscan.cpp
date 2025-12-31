#include "optimizer/optimizer.h"
#include <memory>
#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

// Helper function to extract pred_keys and column index from an OR expression
// Returns {column_idx, pred_keys} if successful, otherwise {-1, {}}
auto ExtractOrKeys(const AbstractExpressionRef &expr, uint32_t &col_idx) -> std::vector<AbstractExpressionRef> {
  std::vector<AbstractExpressionRef> keys;
  col_idx = UINT32_MAX;

  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    if (comp_expr->comp_type_ == ComparisonType::Equal) {
      const auto *left_col = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
      const auto *right_const = dynamic_cast<const ConstantValueExpression *>(comp_expr->GetChildAt(1).get());

      if (left_col == nullptr || right_const == nullptr) {
        const auto *right_col = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
        const auto *left_const = dynamic_cast<const ConstantValueExpression *>(comp_expr->GetChildAt(0).get());
        if (right_col != nullptr && left_const != nullptr && right_col->GetTupleIdx() == 0) {
          keys.push_back(comp_expr->GetChildAt(0));
          col_idx = right_col->GetColIdx();
          return keys;
        }
      } else if (left_col->GetTupleIdx() == 0) {
        keys.push_back(comp_expr->GetChildAt(1));
        col_idx = left_col->GetColIdx();
        return keys;
      }
    }
  } else if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ == LogicType::Or) {
      uint32_t left_col, right_col;
      auto left_keys = ExtractOrKeys(logic_expr->GetChildAt(0), left_col);
      auto right_keys = ExtractOrKeys(logic_expr->GetChildAt(1), right_col);

      if (!left_keys.empty() && !right_keys.empty() && left_col == right_col && left_col != UINT32_MAX) {
        col_idx = left_col;
        keys.insert(keys.end(), left_keys.begin(), left_keys.end());
        keys.insert(keys.end(), right_keys.begin(), right_keys.end());
        return keys;
      }
    }
  }

  return keys;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // First, recursively optimize children
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // Check if this is a SeqScan plan with a filter predicate
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    
    // Only try to optimize if there's a filter predicate
    if (seq_scan.filter_predicate_ != nullptr) {
      const auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);

      uint32_t col_idx;
      auto pred_keys = ExtractOrKeys(seq_scan.filter_predicate_, col_idx);

      // If we found pred_keys for a valid column
      if (!pred_keys.empty() && col_idx != UINT32_MAX) {
        // Look for an index on this column
        for (const auto &index : indices) {
          const auto &key_attrs = index->index_->GetKeyAttrs();
          if (!key_attrs.empty() && key_attrs[0] == col_idx) {
            // Found a matching index! Create an IndexScanPlanNode
            return std::make_shared<IndexScanPlanNode>(
                seq_scan.output_schema_, 
                seq_scan.GetTableOid(),
                index->index_oid_,
                seq_scan.filter_predicate_,
                pred_keys);
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
