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
#include <algorithm>
#include <memory>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

void LRUKNode::add_timestamp(size_t timestamp) {
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  BUSTUB_ASSERT(frame_id != nullptr, "the frame_id is nullptr");
  std::scoped_lock<std::mutex> latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  if (!less_k_list.empty()) {
    auto node = std::find_if(less_k_list.rbegin(), less_k_list.rend(),
                             [](const std::shared_ptr<LRUKNode> &node) { return node->is_evictable(); });
    if (node != less_k_list.rend()) {
      *frame_id = (*node)->get_fid();
      less_k_list.remove(*node);
      node_store_.erase(*frame_id);
      --curr_size_;
      return true;
    }
  }
  if (!more_k_list.empty()) {
    auto node = std::find_if(more_k_list.rbegin(), more_k_list.rend(),
                             [](const std::shared_ptr<LRUKNode> &node) { return node->is_evictable(); });
    if (node != more_k_list.rend()) {
      *frame_id = (*node)->get_fid();
      more_k_list.remove(*node);
      node_store_.erase(*frame_id);
      --curr_size_;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "frame_id is invalid");
  std::scoped_lock<std::mutex> latch(latch_);
  ++current_timestamp_;

  auto iter = node_store_.find(frame_id);
  // the frame not found, we need to create a new node and add it to the node_store and less_k_list
  if (iter == node_store_.end()) {
    auto n_node = std::make_shared<LRUKNode>(frame_id, k_);
    n_node->add_timestamp(current_timestamp_);
    node_store_[frame_id] = n_node;
    ++curr_size_;
    less_k_list.push_front(n_node);
    return;
  }
  auto node = iter->second;
  if (node->get_access_count() < k_) {
    node->add_timestamp(current_timestamp_);
    if (node->get_access_count() == k_) {
      more_k_list_push_node(node);
      less_k_list.remove(node);
      return;
    }
    // we need to change the node postion in the less_k_list by lru
    less_k_list.remove(node);
    less_k_list.push_front(node);
    return;
  }
  // the node is in the more_k_list
  node->add_timestamp(current_timestamp_);
  more_k_list.remove(node);
  more_k_list_push_node(node);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "frame_id is invalid");
  std::scoped_lock<std::mutex> latch(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }

  auto node = iter->second;
  if (node->is_evictable() == set_evictable) {
    return;
  }
  node->set_evictable(set_evictable);
  set_evictable ? ++curr_size_ : --curr_size_;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "frame_id is invalid");
  std::scoped_lock<std::mutex> latch(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  auto node = iter->second;
  if (!node->is_evictable()) {
    throw Exception("The frame is not evictable");
  }
  node_store_.erase(iter);
  if (node->get_access_count() < k_) {
    less_k_list.remove(node);
  } else {
    more_k_list.remove(node);
  }
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> latch(latch_);
  return curr_size_;
}

auto LRUKReplacer::more_k_list_push_node(std::shared_ptr<LRUKNode> node) -> void {
  auto iter = std::find_if(more_k_list.begin(), more_k_list.end(), [&node](const std::shared_ptr<LRUKNode> &n) {
    return n->get_least_recent_timestamp() < node->get_least_recent_timestamp();
  });
  more_k_list.insert(iter, node);
}

}  // namespace bustub
