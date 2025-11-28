#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id),
      bpm_(buffer_pool_manager) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  Context ctx;

  auto header_page = bpm_->ReadPage(header_page_id_).As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  ctx.root_page_id_ = header_page->root_page_id_;

  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));

  while (true) {
    auto current_page = ctx.read_set_.back().As<BPlusTreePage>();

    if (current_page->IsLeafPage()) {
      auto leaf = ctx.read_set_.back().As<LeafPage>();

      int idx = leaf->KeyIndex(key, comparator_);
      if (idx < leaf->GetSize() && comparator_(leaf->KeyAt(idx), key) == 0) {
        if (result != nullptr) {
          result->push_back(leaf->ValueAt(idx));
        }
        return true;
      }
      return false;
    }

    auto internal = ctx.read_set_.back().As<InternalPage>();
    int next_idx = internal->KeyIndex(key, comparator_);
    page_id_t next_child_id = internal->ValueAt(next_idx);

    ctx.read_set_.push_back(bpm_->ReadPage(next_child_id));
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;

  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t new_page_id = bpm_->NewPage();
    BUSTUB_ENSURE(new_page_id != INVALID_PAGE_ID, "new page fail");

    WritePageGuard new_root_guard = bpm_->WritePage(new_page_id);
    auto leaf = new_root_guard.AsMut<LeafPage>();
    leaf->Init(leaf_max_size_);
    leaf->Insert(key, value, comparator_);

    header_page->root_page_id_ = new_page_id;
    ctx.root_page_id_ = new_page_id;

    return true;
  }

  ctx.root_page_id_ = header_page->root_page_id_;

  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));

  while (true) {
    auto page = ctx.write_set_.back().As<BPlusTreePage>();
    page_id_t page_id = ctx.write_set_.back().GetPageId();

    if (page->IsLeafPage()) {
      page_id_t leaf_id = ctx.write_set_.back().GetPageId();
      auto leaf_guard = std::move(ctx.write_set_.back());
      auto leaf = leaf_guard.AsMut<LeafPage>();
      ctx.write_set_.pop_back();

      leaf->SetPageId(leaf_id);
      int idx = leaf->KeyIndex(key, comparator_);

      if (idx < leaf->GetSize() && comparator_(leaf->KeyAt(idx), key) == 0) {
        return false;
      }

      auto res = leaf->Insert(key, value, comparator_);

      if (!res) {
        return false;
      }

      if (leaf->GetSize() <= leaf->GetMaxSize()) {
        return true;
      }

      page_id_t new_id = bpm_->NewPage();
      BUSTUB_ENSURE(new_id != INVALID_PAGE_ID, "new page fail");

      WritePageGuard right_guard = bpm_->WritePage(new_id);

      auto right_leaf = right_guard.AsMut<LeafPage>();
      right_leaf->Init(leaf_max_size_);
      right_leaf->SetPageId(new_id);

      KeyType middle_key;
      leaf->Split(right_leaf, &middle_key, bpm_);
      CoalesceOrSplit(&ctx, leaf_id, middle_key, new_id);

      return true;
    }

    auto internal = ctx.write_set_.back().AsMut<InternalPage>();
    internal->SetPageId(page_id);

    int child_idx = internal->KeyIndex(key, comparator_);
    page_id_t child_id = internal->ValueAt(child_idx);

    ctx.write_set_.push_back(bpm_->WritePage(child_id));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrSplit(Context *ctx, page_id_t left_id, const KeyType &key, page_id_t right_id) {
  if (ctx->IsRootPage(left_id)) {
    page_id_t new_root_id = bpm_->NewPage();
    BUSTUB_ENSURE(new_root_id != INVALID_PAGE_ID, "new root fail");

    WritePageGuard g = bpm_->WritePage(new_root_id);
    auto new_root = g.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    new_root->SetValueAt(0, left_id);
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, right_id);
    new_root->SetSize(2);

    auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_id;

    return;
  }
  BUSTUB_ENSURE(!ctx->write_set_.empty(), "Parent page not found in read set");

  page_id_t parent_id = ctx->write_set_.back().GetPageId();

  auto parent_guard = std::move(ctx->write_set_.back());
  auto parent = parent_guard.AsMut<InternalPage>();
  ctx->write_set_.pop_back();

  parent->SetPageId(parent_id);

  parent->InsertNodeAfter(left_id, key, right_id);

  if (parent->GetSize() <= parent->GetMaxSize()) {
    return;
  }

  // internal node split
  page_id_t old_id = parent->GetPageId();
  page_id_t sib_id = bpm_->NewPage();
  BUSTUB_ENSURE(sib_id != INVALID_PAGE_ID, "new page fail");

  WritePageGuard sib_guard = bpm_->WritePage(sib_id);
  auto sibling = sib_guard.AsMut<InternalPage>();
  sibling->Init(internal_max_size_);

  KeyType middle;
  parent->Split(sibling, &middle);

  CoalesceOrSplit(ctx, old_id, middle, sib_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Remove(const KeyType &key) -> void {
  Context ctx;

  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  ctx.root_page_id_ = header_page->root_page_id_;

  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));

  while (true) {
    auto page = ctx.write_set_.back().AsMut<BPlusTreePage>();

    if (page->IsLeafPage()) {
      page_id_t leaf_id = ctx.write_set_.back().GetPageId();
      auto leaf_guard = std::move(ctx.write_set_.back());
      auto leaf = leaf_guard.AsMut<LeafPage>();
      leaf->SetPageId(leaf_id);
      int idx = leaf->KeyIndex(key, comparator_);

      if (idx == leaf->GetSize() || comparator_(leaf->KeyAt(idx), key) != 0) {
        return;
      }

      leaf->RemoveAndDeleteRecord(key, comparator_);

      if (ctx.IsRootPage(leaf_id)) {
        if (leaf->GetSize() == 0) {
          bpm_->DeletePage(leaf_id);
          header_page->root_page_id_ = INVALID_PAGE_ID;
          ctx.root_page_id_ = INVALID_PAGE_ID;
        }
        return;
      }

      if (leaf->GetSize() >= leaf->GetMinSize()) {
        return;
      }

      ctx.write_set_[ctx.write_set_.size() - 1] = std::move(leaf_guard);

      FixUnderflow(&ctx);

      break;
    }

    auto internal_guard = std::move(ctx.write_set_.back());
    page_id_t internal_id = internal_guard.GetPageId();
    auto internal_page = internal_guard.AsMut<InternalPage>();
    internal_page->SetPageId(internal_id);

    int child_idx = internal_page->KeyIndex(key, comparator_);
    page_id_t child_id = internal_page->ValueAt(child_idx);

    ctx.write_set_[ctx.write_set_.size() - 1] = std::move(internal_guard);
    ctx.write_set_.push_back(bpm_->WritePage(child_id));
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FixUnderflow(Context *ctx) -> void {
  auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();

  if (ctx->write_set_.empty()) {
    return;
  }

  auto curr_guard = std::move(ctx->write_set_.back());
  auto curr_node = curr_guard.AsMut<BPlusTreePage>();
  auto page_id = curr_node->GetPageId();
  bool is_leaf = curr_node->IsLeafPage();

  if (ctx->IsRootPage(page_id) && curr_node->GetSize() == 1 && !curr_node->IsLeafPage()) {
    auto node = curr_guard.AsMut<InternalPage>();
    page_id_t new_root_id = node->ValueAt(0);
    bpm_->DeletePage(page_id);
    ctx->root_page_id_ = new_root_id;
    header_page->root_page_id_ = new_root_id;
    return;
  }

  auto parent_guard = std::move(ctx->write_set_[ctx->write_set_.size() - 2]);
  auto parent_node = parent_guard.AsMut<InternalPage>();

  int child_index = parent_node->ValueIndex(page_id);
  assert(child_index != -1);

  page_id_t left_sibling_id = (child_index > 0) ? parent_node->ValueAt(child_index - 1) : INVALID_PAGE_ID;
  page_id_t right_sibling_id =
      (child_index + 1 < parent_node->GetSize()) ? parent_node->ValueAt(child_index + 1) : INVALID_PAGE_ID;

  auto try_borrow_from_left = [&]() -> bool {
    if (left_sibling_id == INVALID_PAGE_ID) {
      return false;
    }

    auto left_guard = bpm_->WritePage(left_sibling_id);
    auto left_node = left_guard.AsMut<BPlusTreePage>();

    if (is_leaf) {
      if (left_node->GetSize() <= left_node->GetMinSize()) {
        return false;
      }

      auto left_leaf = left_guard.AsMut<LeafPage>();
      auto curr_leaf = curr_guard.AsMut<LeafPage>();

      KeyType key_to_move = left_leaf->KeyAt(left_leaf->GetSize() - 1);
      ValueType value_to_move = left_leaf->ValueAt(left_leaf->GetSize() - 1);

      left_leaf->RemoveAndDeleteRecord(key_to_move, comparator_);
      curr_leaf->Insert(key_to_move, value_to_move, comparator_);

      parent_node->SetKeyAt(child_index, curr_leaf->KeyAt(0));

    } else {
      if (left_node->GetSize() - 1 <= left_node->GetMinSize()) {
        return false;
      }

      auto left_internal = left_guard.AsMut<InternalPage>();
      auto curr_internal = curr_guard.AsMut<InternalPage>();

      KeyType separator_key = parent_node->KeyAt(child_index);

      page_id_t child_to_move = left_internal->ValueAt(left_internal->GetSize() - 1);

      KeyType key_to_push_up = left_internal->KeyAt(left_internal->GetSize() - 1);

      left_internal->Remove(left_internal->GetSize() - 1);

      curr_internal->InsertFront(separator_key, child_to_move);

      parent_node->SetKeyAt(child_index, key_to_push_up);
    }
    return true;
  };

  auto try_borrow_from_right = [&]() -> bool {
    if (right_sibling_id == INVALID_PAGE_ID) {
      return false;
    }

    auto right_guard = bpm_->WritePage(right_sibling_id);
    auto right_node = right_guard.AsMut<BPlusTreePage>();

    if (is_leaf) {
      if (right_node->GetSize() <= right_node->GetMinSize()) {
        return false;
      }

      auto right_leaf = right_guard.AsMut<LeafPage>();
      auto curr_leaf = curr_guard.AsMut<LeafPage>();

      KeyType key_to_move = right_leaf->KeyAt(0);
      ValueType value_to_move = right_leaf->ValueAt(0);

      right_leaf->RemoveAndDeleteRecord(key_to_move, comparator_);
      curr_leaf->Insert(key_to_move, value_to_move, comparator_);

      parent_node->SetKeyAt(child_index + 1, right_leaf->KeyAt(0));

    } else {
      if (right_node->GetSize() - 1 <= right_node->GetMinSize()) {
        return false;
      }

      auto right_internal = right_guard.AsMut<InternalPage>();
      auto curr_internal = curr_guard.AsMut<InternalPage>();

      KeyType separator_key = parent_node->KeyAt(child_index + 1);

      page_id_t child_to_move = right_internal->ValueAt(0);
      KeyType new_separator = right_internal->KeyAt(1);  // Get this BEFORE removing

      right_internal->Remove(0);

      curr_internal->InsertBack(separator_key, child_to_move);

      parent_node->SetKeyAt(child_index + 1, new_separator);  // Use the key we saved
    }
    return true;
  };

  // Merge with left sibling
  auto merge_with_left = [&]() {
    auto left_guard = bpm_->WritePage(left_sibling_id);

    if (is_leaf) {
      auto left_leaf = left_guard.AsMut<LeafPage>();
      auto curr_leaf = curr_guard.AsMut<LeafPage>();
      left_leaf->Merge(curr_leaf, bpm_);
    } else {
      auto left_internal = left_guard.AsMut<InternalPage>();
      auto curr_internal = curr_guard.AsMut<InternalPage>();

      KeyType separator = parent_node->KeyAt(child_index);

      left_internal->InsertBack(separator, curr_internal->ValueAt(0));

      int left_size = left_internal->GetSize();
      int curr_size = curr_internal->GetSize();

      for (int i = 1; i < curr_size; ++i) {
        left_internal->SetKeyAt(left_size + i - 1, curr_internal->KeyAt(i));
        left_internal->SetValueAt(left_size + i - 1, curr_internal->ValueAt(i));
      }
      left_internal->SetSize(left_size + curr_size - 1);
      curr_internal->SetSize(0);
    }

    parent_node->Remove(child_index);
    bpm_->DeletePage(page_id);
  };

  auto merge_with_right = [&]() {
    auto right_guard = bpm_->WritePage(right_sibling_id);

    if (is_leaf) {
      auto curr_leaf = curr_guard.AsMut<LeafPage>();
      auto right_leaf = right_guard.AsMut<LeafPage>();
      curr_leaf->Merge(right_leaf, bpm_);
    } else {
      auto curr_internal = curr_guard.AsMut<InternalPage>();
      auto right_internal = right_guard.AsMut<InternalPage>();

      KeyType separator = parent_node->KeyAt(child_index + 1);

      curr_internal->InsertBack(separator, right_internal->ValueAt(0));

      int curr_size = curr_internal->GetSize();
      int right_size = right_internal->GetSize();

      for (int i = 1; i < right_size; ++i) {
        curr_internal->SetKeyAt(curr_size + i - 1, right_internal->KeyAt(i));
        curr_internal->SetValueAt(curr_size + i - 1, right_internal->ValueAt(i));
      }
      curr_internal->SetSize(curr_size + right_size - 1);
      right_internal->SetSize(0);
    }

    parent_node->Remove(child_index + 1);
    bpm_->DeletePage(right_sibling_id);
  };

  ctx->write_set_.pop_back();

  if (try_borrow_from_left() || try_borrow_from_right()) {
    ctx->write_set_.push_back(std::move(parent_guard));
    return;
  }

  if (left_sibling_id != INVALID_PAGE_ID) {
    merge_with_left();
  } else if (right_sibling_id != INVALID_PAGE_ID) {
    merge_with_right();
  } else {
    FixUnderflow(ctx);
    return;
  }

  if (ctx->IsRootPage(parent_node->GetPageId())) {
    if (parent_node->GetSize() <= 1) {
      page_id_t new_root_id = parent_node->ValueAt(0);
      bpm_->DeletePage(parent_node->GetPageId());
      ctx->root_page_id_ = new_root_id;
      header_page->root_page_id_ = new_root_id;
    }
    ctx->write_set_.push_back(std::move(parent_guard));
    return;
  }

  if (parent_node->GetSize() - 1 < parent_node->GetMinSize()) {
    ctx->write_set_[ctx->write_set_.size() - 1] = std::move(parent_guard);
    FixUnderflow(ctx);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t current_page_id = root_page_id;
  while (true) {
    ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
    auto current_page = current_guard.As<BPlusTreePage>();
    if (current_page->IsLeafPage()) {
      auto leaf_page = current_guard.As<LeafPage>();
      return INDEXITERATOR_TYPE(bpm_, leaf_page->GetPageId(), 0);
    }
    auto internal_page = current_guard.As<InternalPage>();
    current_page_id = internal_page->ValueAt(0);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t current_page_id = root_page_id;
  while (true) {
    ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
    auto current_page = current_guard.As<BPlusTreePage>();
    if (current_page->IsLeafPage()) {
      auto leaf_page = current_guard.As<LeafPage>();
      int index = leaf_page->KeyIndex(key, comparator_);
      return INDEXITERATOR_TYPE(bpm_, leaf_page->GetPageId(), index);
    }
    auto internal_page = current_guard.As<InternalPage>();
    int index = internal_page->KeyIndex(key, comparator_);
    current_page_id = internal_page->ValueAt(index);
  }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
