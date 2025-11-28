//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }

/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    KeyType mid_key = KeyAt(mid);
    if (comparator(mid_key, key) < 0) {
      left = mid + 1;
    } else if (comparator(mid_key, key) > 0) {
      right = mid - 1;
    } else {
      return mid;
    }
  }
  return left - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(ValueType old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> void {
  int index = ValueIndex(old_value);
  BUSTUB_ENSURE(index != -1, "Old value not found in internal page.");

  // Shift keys and values to the right to make space for the new key-value pair
  for (int i = GetSize(); i > index + 1; --i) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
  }

  // Insert the new key-value pair
  key_array_[index + 1] = new_key;
  page_id_array_[index + 1] = new_value;

  // Increase the size of the internal page
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *sibling, KeyType *middle_key) -> void {
  int total_size = GetSize();
  int mid_index = total_size / 2;
  *middle_key = KeyAt(mid_index);

  // The sibling's first value pointer should be the middle key's value (which is mid_index)
  sibling->SetValueAt(0, ValueAt(mid_index));
  sibling->SetSize(1);

  // Move the keys and values after mid_index to the sibling
  // Note: skip the first key of sibling (it must remain invalid)
  for (int i = mid_index + 1; i < total_size; ++i) {
    sibling->key_array_[sibling->GetSize()] = key_array_[i];
    sibling->page_id_array_[sibling->GetSize()] = page_id_array_[i];
    sibling->ChangeSizeBy(1);
  }

  for (int i = mid_index; i < total_size; ++i) {
    // Clear the moved entries (optional)
    key_array_[i] = KeyType();
    page_id_array_[i] = ValueType();
  }

  // Update the size of the current internal page (keep mid_index, not mid_index+1)
  SetSize(mid_index);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) -> void {
  BUSTUB_ENSURE(index >= 0 && index < GetSize(), "Index out of bounds");

  // Shift keys and values to the left to fill the gap
  for (int i = index; i < GetSize() - 1; ++i) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }

  // Decrease the size of the internal page
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *sibling) -> void {
  int original_size = GetSize();
  int sibling_size = sibling->GetSize();

  // Move all entries from sibling to this page
  for (int i = 0; i < sibling_size; ++i) {
    key_array_[original_size + i] = sibling->key_array_[i];
    page_id_array_[original_size + i] = sibling->page_id_array_[i];
  }

  // Update the size of this page
  SetSize(original_size + sibling_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator)
    -> bool {
  int index = KeyIndex(key, comparator);
  if (index == 0 || index >= GetSize() || comparator(key_array_[index], key) != 0) {
    // Key not found or trying to remove the invalid first key
    return false;
  }
  // Shift keys and values to the left to fill the gap
  for (int i = index; i < GetSize() - 1; ++i) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  // Decrease the size of the internal page
  ChangeSizeBy(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { page_id_array_[index] = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFront(const KeyType &new_key, const ValueType &new_value) -> void {
  // Shift keys [1, size) -> [2, size+1)
  for (int i = GetSize(); i >= 2; --i) {
    key_array_[i] = key_array_[i - 1];
  }
  // Shift pointers [0, size) -> [1, size+1)
  for (int i = GetSize(); i >= 1; --i) {
    page_id_array_[i] = page_id_array_[i - 1];
  }

  // Insert new key at index 1, new pointer at index 0
  key_array_[1] = new_key;
  page_id_array_[0] = new_value;

  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertBack(const KeyType &new_key, const ValueType &new_value) -> void {
  key_array_[GetSize()] = new_key;
  page_id_array_[GetSize()] = new_value;

  ChangeSizeBy(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
