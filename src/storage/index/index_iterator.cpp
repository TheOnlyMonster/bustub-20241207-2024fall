/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator()
    : bpm_(nullptr), current_page_id_(INVALID_PAGE_ID), index_(0), page_guard_(std::nullopt) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : bpm_(bpm), current_page_id_(page_id), index_(index), page_guard_(std::nullopt) {
  if (page_id != INVALID_PAGE_ID) {
    page_guard_ = bpm_->ReadPage(page_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::GetLeafPage() const -> const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * {
  if (!page_guard_.has_value()) {
    return nullptr;
  }
  return page_guard_->As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return current_page_id_ == INVALID_PAGE_ID || !page_guard_.has_value(); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto *leaf_page = GetLeafPage();
  BUSTUB_ASSERT(leaf_page != nullptr, "Attempting to dereference end iterator");
  return std::make_pair(leaf_page->KeyAt(index_), leaf_page->ValueAt(index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto *leaf_page = GetLeafPage();
  BUSTUB_ASSERT(leaf_page != nullptr, "Attempting to increment end iterator");

  index_++;

  // If we've reached the end of current leaf page, move to next leaf page
  if (index_ >= leaf_page->GetSize()) {
    page_id_t next_page_id = leaf_page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // No more pages, set to end
      page_guard_ = std::nullopt;
      current_page_id_ = INVALID_PAGE_ID;
      index_ = 0;
    } else {
      // Move to next page
      page_guard_ = bpm_->ReadPage(next_page_id);
      current_page_id_ = next_page_id;
      index_ = 0;
    }
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return current_page_id_ == itr.current_page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
