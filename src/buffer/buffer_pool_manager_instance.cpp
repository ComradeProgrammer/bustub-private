//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"
using std::lock_guard, std::mutex;
namespace bustub {
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  // LOG_INFO("[BufferPool %d/%d] pool_size %d", instance_index_, num_instances_, static_cast<int>(pool_size));
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // LOG_INFO("[BufferPool %d/%d] Flush Page %d", instance_index_, num_instances_, page_id);

  // Make sure you call DiskManager::WritePage!
  lock_guard<mutex> lock_sector(latch_);
  return FlushPgInner(page_id);
}

auto BufferPoolManagerInstance::FlushPgInner(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &(pages_[frame_id]);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // LOG_INFO("[BufferPool %d/%d] flush all page ", instance_index_, num_instances_);

  // You can do it!
  lock_guard<mutex> lock_sector(latch_);
  for (auto tuple : page_table_) {
    FlushPgInner(tuple.first);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  lock_guard<mutex> lock_sector(latch_);

  frame_id_t frame_id;
  Page *page = nullptr;
  // whether there is a free page
  if (!free_list_.empty()) {
    // there is a free page
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &(pages_[frame_id]);

  } else {
    // try to pick a victim by lru
    if (replacer_->Victim(&frame_id)) {
      page = &(pages_[frame_id]);
      FlushPgInner(page->GetPageId());
      page_table_.erase(page->GetPageId());
    } else {
      // cannot pick out a victim
      // LOG_INFO("[BufferPool %d/%d] New Page: Out of pages", instance_index_, num_instances_);
      return nullptr;
    }
  }
  *page_id = AllocatePage();
  // LOG_INFO("[BufferPool %d/%d] New Page %d", instance_index_, num_instances_, *page_id);
  page->ResetMemory();
  page->page_id_ = *page_id;
  // should I pin this page?
  page->pin_count_ = 1;
  // record in page table
  page_table_[*page_id] = frame_id;

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // LOG_INFO("[BufferPool %d/%d] Fetch Page %d", instance_index_, num_instances_, page_id);

  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  lock_guard<mutex> lock_sector(latch_);
  Page *page = nullptr;
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    page = &(pages_[frame_id]);
  } else {
    // page doesn't exist
    if (!free_list_.empty()) {
      // there is a free page
      frame_id = free_list_.front();
      free_list_.pop_front();
      page = &(pages_[frame_id]);

    } else {
      // try to pick a victim by lru
      if (replacer_->Victim(&frame_id)) {
        page = &(pages_[frame_id]);
        FlushPgInner(page->GetPageId());
        page_table_.erase(page->GetPageId());
      } else {
        // cannot pick out a victim
        return nullptr;
      }
    }
    page->ResetMemory();
    page->page_id_ = page_id;
    page->pin_count_ = 0;
    // record in page table
    page_table_[page_id] = frame_id;
    // load from disk
    disk_manager_->ReadPage(page_id, page->GetData());
  }
  // pin this page
  if (page->GetPinCount() == 0) {
    replacer_->Pin(frame_id);
  }
  page->pin_count_++;

  return page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // LOG_INFO("[BufferPool %d/%d] Delete Page %d", instance_index_, num_instances_, page_id);

  lock_guard<mutex> lock_sector(latch_);

  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
  // list.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &(pages_[frame_id]);

  if (page->GetPinCount() != 0) {
    // If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    return false;
  }

  // flush it, and remove it from lru system
  if (page->IsDirty()) {
    FlushPgInner(page_id);
  }
  replacer_->Pin(frame_id);
  // clear meta data for this page
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  // return to free list
  free_list_.push_back(frame_id);
  // deallocate the page
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // LOG_INFO("[BufferPool %d/%d] Unpin Page %d, is_dirty=%d", instance_index_, num_instances_, page_id, is_dirty);
  lock_guard<mutex> lock_sector(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &(pages_[frame_id]);
  page->is_dirty_ |= is_dirty;
  if (page->GetPinCount() > 0) {
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
  } else {
    return false;
  }

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
