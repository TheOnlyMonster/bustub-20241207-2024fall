//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
  prev_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrevPageId() const -> page_id_t { return prev_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrevPageId(page_id_t prev_page_id) { prev_page_id_ = prev_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ENSURE(index >= 0 && index < GetSize(), "Index out of bounds");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int index = KeyIndex(key, comparator);
  BUSTUB_ENSURE(index >= 0 && index <= GetSize(), "Index out of bounds");
  if (GetSize() + 1 >= static_cast<int>(LEAF_PAGE_SLOT_CNT)) {
    return false;
  }

  for (int i = GetSize(); i > index; --i) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }

  key_array_[index] = key;
  rid_array_[index] = value;

  ChangeSizeBy(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ENSURE(index >= 0 && index < GetSize(), "Index out of bounds");
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = KeyIndex(key, comparator);
  if (index >= GetSize() || comparator(key_array_[index], key) != 0) {
    // Key not found
    return false;
  }
  // Shift keys and values to the left to fill the gap
  for (int i = index; i < GetSize() - 1; ++i) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  // Decrease the size of the leaf page
  ChangeSizeBy(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key_array_[mid], key) == 0) {
      return mid;
    }

    if (comparator(key_array_[mid], key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage *sibling, KeyType *middle_key, BufferPoolManager *bpm)
    -> void {
  int total_size = GetSize();
  int mid_index = (total_size + 1) / 2;  // Ceiling division for proper split
  *middle_key = KeyAt(mid_index);

  // Move the second half of the entries to the sibling (including mid_index for leaf pages)
  for (int i = mid_index; i < total_size; ++i) {
    sibling->key_array_[sibling->GetSize()] = key_array_[i];
    sibling->rid_array_[sibling->GetSize()] = rid_array_[i];
    sibling->ChangeSizeBy(1);
  }

  sibling->SetNextPageId(this->GetNextPageId());
  sibling->SetPrevPageId(this->GetPageId());

  this->SetNextPageId(sibling->GetPageId());

  if (sibling->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_sibling_guard = bpm->WritePage(sibling->GetNextPageId());
    auto next_sibling_page = next_sibling_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    next_sibling_page->SetPrevPageId(sibling->GetPageId());
  }

  for (int i = mid_index; i < total_size; ++i) {
    // Clear the moved entries (optional)
    key_array_[i] = KeyType();
    rid_array_[i] = ValueType();
  }

  // Update the size of the current leaf page
  SetSize(mid_index);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *sibling, BufferPoolManager *bpm) -> void {
  int original_size = GetSize();
  int sibling_size = sibling->GetSize();

  // Move all entries from sibling to this page
  for (int i = 0; i < sibling_size; ++i) {
    key_array_[original_size + i] = sibling->key_array_[i];
    rid_array_[original_size + i] = sibling->rid_array_[i];
  }

  // Update the size of this page
  SetSize(original_size + sibling_size);

  // Update the next page id to skip the sibling
  this->SetNextPageId(sibling->GetNextPageId());
  if (sibling->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_sibling_guard = bpm->WritePage(sibling->GetNextPageId());
    auto next_sibling_page = next_sibling_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    next_sibling_page->SetPrevPageId(this->GetPageId());
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
