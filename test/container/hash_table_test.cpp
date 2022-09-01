//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, SampleTest2) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah2", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 496 * 5; i++) {
    // std::cout << "insert" << i << "," << i << std::endl;

    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
    ht.VerifyIntegrity();
  }
  // std::cout << "verify1" << std::endl;
  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 496 * 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
  // std::cout << "verify2" << std::endl;
  ht.VerifyIntegrity();

  // test merge
  for (int i = 0; i < 496 * 5; i++) {
    // std::cout << "delete" << i << "," << i << std::endl;
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    ht.VerifyIntegrity();
  }
  // std::cout << "verify3" << std::endl;
  ht.VerifyIntegrity();
  EXPECT_EQ(ht.GetGlobalDepth(), 0);

  // try insert more values
  for (int i = 0; i < 496 * 2; i++) {
    for (int j = 0; j < 3; j++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i + j));
    }
  }
  ht.VerifyIntegrity();

  for (int i = 0; i < 496 * 2; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(3, res.size()) << "Failed to keep " << i << std::endl;
    sort(res.begin(), res.end());
    EXPECT_EQ(res[0], i);
    EXPECT_EQ(res[1], i + 1);
    EXPECT_EQ(res[2], i + 2);
  }

  // try insert more values
  for (int i = 0; i < 496 * 2; i++) {
    for (int j = 0; j < 3; j++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i + j));
    }
  }
  ht.VerifyIntegrity();
  EXPECT_EQ(ht.GetGlobalDepth(), 0);
  // ht.PrintPageDirectory();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, SampleTest3) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(20, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah2", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 496 * 8; i++) {
    // std::cout << "insert" << i << "," << i << std::endl;

    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
    ht.VerifyIntegrity();
  }
  // std::cout << "verify1" << std::endl;
  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 496 * 8 - 1; i >= 0; i--) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
  // std::cout << "verify2" << std::endl;
  ht.VerifyIntegrity();
  ht.PrintPageDirectory();

  // test merge
  for (int i = 496 * 8 - 1; i >= 0; i--) {
    // std::cout << "delete" << i << "," << i << std::endl;
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    ht.VerifyIntegrity();
  }
  // std::cout << "verify3" << std::endl;
  ht.VerifyIntegrity();
  EXPECT_EQ(ht.GetGlobalDepth(), 0);

  // ht.PrintPageDirectory();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
