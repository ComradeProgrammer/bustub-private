
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
using std::cout, std::endl;
namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // cout << "BUCKET_ARRAY_SIZE" << BUCKET_ARRAY_SIZE << "  ;  ";
  // 1. first, allocate the directory page
  Page *dir_page = buffer_pool_manager_->NewPage(&directory_page_id_, nullptr);
  auto *directory = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  directory->SetPageId(directory_page_id_);
  // 2. allocate a page for bucket 0;
  page_id_t bucket_0;
  buffer_pool_manager_->NewPage(&bucket_0, nullptr);
  directory->SetBucketPageId(0, bucket_0);

  // 3. and then write them back
  buffer_pool_manager->UnpinPage(bucket_0, false);
  buffer_pool_manager->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & (dir_page->GetGlobalDepthMask());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_, nullptr);
  auto *directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  if (page == nullptr) {
    return nullptr;
  }
  auto *bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());

  return bucket;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  // cout << "GetValue " << key << endl;
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  RLatchFromBucketPage(bucket_page);
  bool res = bucket_page->GetValue(key, comparator_, result);
  RUnLatchFromBucketPage(bucket_page);

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // cout << "Insert " << key << " , " << value << endl;
  table_latch_.WLock();

  HashTableDirectoryPage *directory_page = FetchDirectoryPage();

  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(KeyToPageId(key, directory_page));
  WLatchFromBucketPage(bucket);

  while (bucket->IsFull()) {
    uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
    page_id_t bucket_page_id = KeyToPageId(key, directory_page);

    int same_key_counter = 0;
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      KeyType key_tmp = bucket->KeyAt(i);
      // cout << KeyToDirectoryIndex(key_tmp, directory_page) << " ";
      if (comparator_(key, key_tmp) == 0) {
        same_key_counter++;
      }
    }

    if (same_key_counter == BUCKET_ARRAY_SIZE) {
      // can never be able to insert
      WUnLatchFromBucketPage(bucket);
      buffer_pool_manager_->UnpinPage(KeyToPageId(key, directory_page), true);
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
      table_latch_.WUnlock();
      return false;
    }

    // split for a bit
    uint32_t old_depth = directory_page->GetGlobalDepth();
    // cout << "SPLIT: for bucket" << bucket_index << ", current global depth " << old_depth << endl;
    // cout << "current directory" << endl;
    // decide whether we need to increase
    if (directory_page->GetLocalDepth(bucket_index) == old_depth) {
      //  increase the global depth
      directory_page->IncrGlobalDepth();
      // and set up other pages
      for (uint32_t i = 0; i < static_cast<uint32_t>(1 << old_depth); i++) {
        uint8_t current_depth = directory_page->GetLocalDepth(i);
        uint32_t current_bucket = directory_page->GetBucketPageId(i);
        directory_page->SetLocalDepth(i + static_cast<uint32_t>(1 << old_depth), current_depth);
        directory_page->SetBucketPageId(i + static_cast<uint32_t>(1 << old_depth), current_bucket);
      }
    }

    // allocate a new page for new bucket, and insert it into page directory
    uint32_t new_bucket_index = GetSplitPageIndex(bucket_index, directory_page);
    page_id_t new_bucket_page_id;
    Page *new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id, nullptr);
    if (new_bucket_page == nullptr) {
      // out of memory
      WUnLatchFromBucketPage(bucket);
      buffer_pool_manager_->UnpinPage(KeyToPageId(key, directory_page), true);
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
      table_latch_.WUnlock();
      return false;
    }
    directory_page->SetBucketPageId(new_bucket_index, new_bucket_page_id);
    auto *new_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_bucket_page->GetData());
    WLatchFromBucketPage(new_bucket);

    //  transfer corresponding items to new bucket
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      KeyType key_tmp = bucket->KeyAt(i);
      ValueType value_tmp = bucket->ValueAt(i);
      // cout << KeyToDirectoryIndex(key_tmp, directory_page) << " ";
      if (KeyToDirectoryIndex(key_tmp, directory_page) == new_bucket_index) {
        new_bucket->Insert(key_tmp, value_tmp, comparator_);
        bucket->RemoveAt(i);
      }
    }
    // then update the local depth
    directory_page->IncrLocalDepth(new_bucket_index);
    directory_page->IncrLocalDepth(bucket_index);

    // and unpin the page we don't need
    if (KeyToDirectoryIndex(key, directory_page) == new_bucket_index) {
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      WUnLatchFromBucketPage(bucket);
      bucket = new_bucket;
    } else {
      buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
      WUnLatchFromBucketPage(new_bucket);
    }
    // cout << "After split" << endl;
    // PrintPageDirectory();
  }
  bool ok = bucket->Insert(key, value, comparator_);

  WUnLatchFromBucketPage(bucket);
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, directory_page), true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();

  return ok;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  return MergeInner(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  MergeInner(transaction, key, value);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::MergeInner(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, directory_page);
  // cout << "REMOVE " << key << " , " << value << " from " << bucket_id << endl;

  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  WLatchFromBucketPage(bucket);

  bool ok = bucket->Remove(key, value, comparator_);
  if (!ok) {
    WUnLatchFromBucketPage(bucket);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    table_latch_.WUnlock();
    return false;
  }
  // cout << "bucket size " << bucket->NumReadable() << endl;
  while (bucket->IsEmpty() && directory_page->GetGlobalDepth() != 0 &&
         directory_page->GetLocalDepth(bucket_id) == directory_page->GetGlobalDepth()) {
    // find its split image
    uint32_t split_bucket_id = GetSplitPageIndex(bucket_id, directory_page);
    page_id_t split_bucket_page_id = directory_page->GetBucketPageId(split_bucket_id);

    if (split_bucket_page_id == bucket_page_id) {
      break;
    }

    if (directory_page->GetLocalDepth(bucket_id) != directory_page->GetLocalDepth(split_bucket_id)) {
      break;
    }

    // cout << "MERGE! " << endl;
    // cout << "bucket " << bucket_id << " split_bucket " << split_bucket_id << endl;
    // PrintPageDirectory();

    // remove the current page
    WUnLatchFromBucketPage(bucket);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_page_id);
    // use split page instead
    directory_page->SetBucketPageId(bucket_id, split_bucket_page_id);

    // decrease
    directory_page->DecrLocalDepth(split_bucket_id);
    directory_page->DecrLocalDepth(bucket_id);

    // cout << "AFTER MERGE " << endl;
    // PrintPageDirectory();

    while (directory_page->CanShrink()) {
      // cout << "SHRINK! " << endl;
      directory_page->DecrGlobalDepth();
      // there may be some empty buckets that can be merged
      if (directory_page->GetGlobalDepth() != 0) {
        // now we do not contain any lock of pages,
        for (uint32_t idx = 0; idx < static_cast<uint32_t>(1 << (directory_page->GetGlobalDepth() - 1)); idx++) {
          uint32_t idx2 = GetSplitPageIndex(idx, directory_page);
          page_id_t page_id1 = directory_page->GetBucketPageId(idx);
          page_id_t page_id2 = directory_page->GetBucketPageId(idx2);
          if (page_id1 != page_id2) {
            HASH_TABLE_BUCKET_TYPE *bucket1 = FetchBucketPage(page_id1);
            HASH_TABLE_BUCKET_TYPE *bucket2 = FetchBucketPage(page_id2);
            WLatchFromBucketPage(bucket1);
            WLatchFromBucketPage(bucket2);
            if (bucket1->NumReadable() == 0) {
              directory_page->SetBucketPageId(idx, page_id2);
              buffer_pool_manager_->UnpinPage(page_id1, false);
              buffer_pool_manager_->DeletePage(page_id1);
              buffer_pool_manager_->UnpinPage(page_id2, false);
              directory_page->DecrLocalDepth(idx);
              directory_page->DecrLocalDepth(idx2);
              WUnLatchFromBucketPage(bucket1);
              WUnLatchFromBucketPage(bucket2);
            } else if (bucket2->NumReadable() == 0) {
              directory_page->SetBucketPageId(idx2, page_id1);
              buffer_pool_manager_->UnpinPage(page_id2, false);
              buffer_pool_manager_->DeletePage(page_id2);
              buffer_pool_manager_->UnpinPage(page_id1, false);
              directory_page->DecrLocalDepth(idx);
              directory_page->DecrLocalDepth(idx2);
              WUnLatchFromBucketPage(bucket1);
              WUnLatchFromBucketPage(bucket2);
            } else {
              buffer_pool_manager_->UnpinPage(page_id1, false);
              buffer_pool_manager_->UnpinPage(page_id2, false);
              WUnLatchFromBucketPage(bucket1);
              WUnLatchFromBucketPage(bucket2);
            }
          }
        }
      }
      // cout << "after SHRINK " << endl;
      // PrintPageDirectory();
    }

    // now update the bucket pointer
    bucket_id = KeyToDirectoryIndex(key, directory_page);
    bucket_page_id = KeyToPageId(key, directory_page);
    bucket = FetchBucketPage(bucket_page_id);
    WLatchFromBucketPage(bucket);
  }

  WUnLatchFromBucketPage(bucket);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();

  return ok;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetSplitPageIndex(uint32_t bucket_index, HashTableDirectoryPage *dir_page) {
  uint32_t global_depth = dir_page->GetGlobalDepth();
  uint32_t highest_bit = (1 << (global_depth - 1));
  return bucket_index ^ highest_bit;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::RLatchFromBucketPage(HASH_TABLE_BUCKET_TYPE *bucket_page) {
  reinterpret_cast<Page *>(bucket_page)->RLatch();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::RUnLatchFromBucketPage(HASH_TABLE_BUCKET_TYPE *bucket_page) {
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::WLatchFromBucketPage(HASH_TABLE_BUCKET_TYPE *bucket_page) {
  reinterpret_cast<Page *>(bucket_page)->WLatch();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::WUnLatchFromBucketPage(HASH_TABLE_BUCKET_TYPE *bucket_page) {
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintPageDirectory() {
  // HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // // dir_page->PrintDirectory();
  // printf("======== DIRECTORY (global_depth_: %u) ========\n", dir_page->global_depth_);
  // printf("| bucket_idx | page_id | local_depth | size \n");
  // for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << (dir_page->global_depth_)); idx++) {
  //   page_id_t bucket_page_id = dir_page->bucket_page_ids_[idx];
  //   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  //   auto size = bucket->NumReadable();
  //   printf("|      %u     |     %u     |     %u     |     %u     \n", idx, dir_page->bucket_page_ids_[idx],
  //          dir_page->local_depths_[idx], size);
  //   buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  // }
  // printf("================ END DIRECTORY ================\n");

  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub