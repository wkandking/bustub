//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::getNewFrame(page_id_t *page_id, bool is_new) -> Page * {
  // get the frame id from the replacer
  frame_id_t frame_id{INVALID_FRAME_ID};
  if (!free_list_.empty()) {
    // if free list is not empty, we can allocate a frame from free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_ != nullptr && replacer_->Evict(&frame_id)) { // if free list is empty, we need to evict a frame
    // flush the page to disk if it is dirty    
    auto page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
    // remove the page from the page table
    page_table_.erase(page_id);
  } else {
    // if free list is empty and we cannot evict a frame, return nullptr
    return nullptr;
  }

  // get the frame and reset its metadata and memory
  auto frame = &pages_[frame_id];
  frame->ResetMetadata();
  frame->ResetMemory();
  // allocate the new page id
  if (is_new) {
    *page_id = AllocatePage();
  }
  // set the page id and record the access
  frame->SetPageId(*page_id);
  replacer_->RecordAccess(frame_id);
  // pin the frame and set it to be not evictable
  frame->Pin();
  replacer_->SetEvictable(frame_id, false);
  // add the page to the page table
  page_table_[*page_id] = frame_id;
  return frame;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  return getNewFrame(page_id);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  Page *frame = nullptr;
  if (iter == page_table_.end()) {
    // if the page is not in the page table, we need to create a new page
    frame = getNewFrame(&page_id, false);
    if (frame == nullptr) {
      return nullptr;
    }
    // read the page from disk
    disk_manager_->ReadPage(page_id, frame->GetData());
  } else {
    // if the page is in the page table, we need to fetch the page
    frame = &pages_[iter->second];
    frame->Pin();
    replacer_->SetEvictable(iter->second, false);
    replacer_->RecordAccess(iter->second);
  }
  return frame;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto frame_id = iter->second;
  auto frame = &pages_[frame_id];
  if (frame->GetPinCount() == 0) {
    return false;
  }
  frame->MarkDirty(is_dirty | frame->IsDirty());
  frame->Unpin();
  // if the pin count is 0, set the frame to be evictable
  if (frame->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto frame_id = iter->second;
  auto frame = &pages_[frame_id];
  disk_manager_->WritePage(page_id, frame->GetData());
  frame->MarkDirty(false);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (auto &entry : page_table_) {
    auto page_id = entry.first;
    auto frame_id = entry.second;
    auto frame = &pages_[frame_id];
    disk_manager_->WritePage(page_id, frame->GetData());
    frame->MarkDirty(false);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  auto frame_id = iter->second;
  auto frame = &pages_[frame_id];
  if (frame->GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(frame_id);
  page_table_.erase(iter);
  frame->ResetMemory();
  frame->ResetMetadata();
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return BasicPageGuard(this, nullptr);
  }

  return BasicPageGuard(this, page);
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return ReadPageGuard(this, nullptr);
  }
  page->RLatch();
  return ReadPageGuard(this, page);
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return WritePageGuard(this, nullptr);
  }

  page->WLatch();
  return WritePageGuard(this, page);
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  if (page == nullptr) {
    return BasicPageGuard(this, nullptr);
  }
  return BasicPageGuard(this, page);
}

}  // namespace bustub
