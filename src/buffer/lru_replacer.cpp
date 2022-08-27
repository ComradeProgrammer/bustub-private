//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {
using std::mutex, std::lock_guard;
LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() {
  for (auto p : page_map_) {
    delete p.second;
  }
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  lock_guard<mutex> sector_lock(lock_);
  if (page_list_head_ == nullptr) {
    return false;
  }
  PageNode *remove = page_list_head_;
  page_list_head_ = page_list_head_->next_;
  if (page_list_tail_ == remove) {
    page_list_tail_ = nullptr;
  }
  *frame_id = remove->frame_id_;
  page_map_.erase(remove->frame_id_);
  delete remove;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_guard<mutex> sector_lock(lock_);

  if (page_map_.find(frame_id) == page_map_.end()) {
    return;
  }
  PageNode *remove = page_map_[frame_id];
  if (page_list_head_ == remove) {
    page_list_head_ = page_list_head_->next_;
  } else {
    remove->prev_->next_ = remove->next_;
  }

  if (page_list_tail_ == remove) {
    page_list_tail_ = page_list_tail_->prev_;
  } else {
    remove->next_->prev_ = remove->prev_;
  }
  page_map_.erase(remove->frame_id_);
  delete remove;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<mutex> sector_lock(lock_);
  if (page_map_.find(frame_id) != page_map_.end()) {
    return;
  }
  auto *page_node = new PageNode();
  page_node->frame_id_ = frame_id;
  page_map_[frame_id] = page_node;
  if (page_list_head_ == nullptr) {
    page_list_head_ = page_node;
  } else {
    page_list_tail_->next_ = page_node;
    page_node->prev_ = page_list_tail_;
  }
  page_list_tail_ = page_node;
}

auto LRUReplacer::Size() -> size_t {
  lock_guard<mutex> sector_lock(lock_);
  return page_map_.size();
}

}  // namespace bustub
