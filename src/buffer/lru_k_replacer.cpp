//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t reg_victim_frame = INVALID_FRAME_ID;
  frame_id_t inf_victim_frame = INVALID_FRAME_ID;

  size_t inf_candidate_time = std::numeric_limits<size_t>::max();
  size_t reg_candidate_distance = 0;

  for (auto &[fid, node] : node_store_) {
    if (!node.is_evictable_) {
      continue;
    }

    if (node.history_.size() < k_) {
      size_t oldest = node.history_.back();
      if (oldest < inf_candidate_time) {
        inf_candidate_time = oldest;
        inf_victim_frame = fid;
      }
    } else {
      size_t distance = current_timestamp_ - node.history_.back();
      if (distance > reg_candidate_distance || reg_victim_frame == INVALID_FRAME_ID) {
        reg_candidate_distance = distance;
        reg_victim_frame = fid;
      }
    }
  }

  current_timestamp_++;

  if (inf_victim_frame != INVALID_FRAME_ID) {
    node_store_.erase(inf_victim_frame);
    curr_size_--;
    return inf_victim_frame;
  }
  if (reg_victim_frame != INVALID_FRAME_ID) {
    node_store_.erase(reg_victim_frame);
    curr_size_--;
    return reg_victim_frame;
  }
  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);

  BUSTUB_ENSURE(static_cast<size_t>(frame_id) <= replacer_size_, "Invalid frame id");

  auto &node = node_store_[frame_id];

  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode new_node;
    new_node.fid_ = frame_id;
    new_node.history_.push_front(current_timestamp_);
    new_node.is_evictable_ = false;
    node_store_[frame_id] = std::move(new_node);
  } else {
    node.history_.push_front(current_timestamp_);
  }
  if (node.history_.size() > k_) {
    node.history_.pop_back();
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);

  auto &node = node_store_[frame_id];

  BUSTUB_ENSURE(node_store_.find(frame_id) != node_store_.end(), "This frame is not available");

  if (node.is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!node.is_evictable_ && set_evictable) {
    curr_size_++;
  }
  node.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  auto node = node_store_[frame_id];

  BUSTUB_ENSURE(node.is_evictable_ == true, "This frame is not evictable");

  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
