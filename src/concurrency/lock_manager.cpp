//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {
using std::unique_lock, std::mutex, std::cout, std::endl;
auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  unique_lock<mutex> lock_function(latch_);
  // cout << "S LOCK FROM " << txn->GetTransactionId() << " ON " << rid << endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  txn->SetState(TransactionState::GROWING);

  txn_id_t txn_id = txn->GetTransactionId();

  // deadlock prevention
  if (locks_status_.find(rid) != locks_status_.end() && locks_status_[rid] == LockMode::EXCLUSIVE) {
    auto tmp = locks_txn_[rid];
    for (txn_id_t locking_id : tmp) {
      if (locking_id > txn_id) {
        Transaction *locking_txn = TransactionManager::GetTransaction(locking_id);
        locking_txn->SetState(TransactionState::ABORTED);
        RemoveFromEverywhere(locking_txn, &lock_function);
      }
    }
    // auto tmp2 = lock_table_[rid].request_queue_;
    // for (auto req : tmp2) {
    //   txn_id_t locking_id = req.txn_id_;
    //   if (locking_id > txn_id) {
    //     Transaction *locking_txn = TransactionManager::GetTransaction(locking_id);
    //     locking_txn->SetState(TransactionState::ABORTED);
    //     RemoveFromEverywhere(locking_txn, &lock_function);
    //   }
    // }
  }

  // currently exclusively locked, append to waiting list
  if (locks_status_.find(rid) != locks_status_.end() && locks_status_[rid] == LockMode::EXCLUSIVE) {
    LockRequest request(txn_id, LockMode::SHARED);
    lock_table_[rid].request_queue_.push_back(request);
    lock_table_[rid].pending_txns_.insert(txn_id);
  }

  while (lock_table_[rid].pending_txns_.find(txn_id) != lock_table_[rid].pending_txns_.end() &&
         txn->GetState() != TransactionState::ABORTED) {
    // continue to wait
    lock_table_[rid].cv_.wait(lock_function);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  if (locks_status_.find(rid) == locks_status_.end()) {
    // currently unlocked
    locks_status_[rid] = LockMode::SHARED;
  }
  locks_txn_[rid].insert(txn_id);
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  unique_lock<mutex> lock_function(latch_);
  // cout << "X LOCK FROM " << txn->GetTransactionId() << " ON " << rid << endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  txn->SetState(TransactionState::GROWING);

  txn_id_t txn_id = txn->GetTransactionId();
  /// deadlock prevention
  if (locks_status_.find(rid) != locks_status_.end()) {
    auto tmp = locks_txn_[rid];
    for (txn_id_t locking_id : tmp) {
      if (locking_id > txn_id) {
        Transaction *locking_txn = TransactionManager::GetTransaction(locking_id);
        locking_txn->SetState(TransactionState::ABORTED);
        RemoveFromEverywhere(locking_txn, &lock_function);
      }
    }
    // auto tmp2 = lock_table_[rid].request_queue_;
    // for (auto req : tmp2) {
    //   txn_id_t locking_id = req.txn_id_;
    //   if (locking_id > txn_id) {
    //     Transaction *locking_txn = TransactionManager::GetTransaction(locking_id);
    //     locking_txn->SetState(TransactionState::ABORTED);
    //     RemoveFromEverywhere(locking_txn, &lock_function);
    //   }
    // }
  }

  // for exclusive lock, it can only directly pass when there is no lock
  if (locks_status_.find(rid) != locks_status_.end()) {
    LockRequest request(txn_id, LockMode::EXCLUSIVE);
    lock_table_[rid].request_queue_.push_back(request);
    lock_table_[rid].pending_txns_.insert(txn_id);
  }

  while (lock_table_[rid].pending_txns_.find(txn_id) != lock_table_[rid].pending_txns_.end() &&
         txn->GetState() != TransactionState::ABORTED) {
    // continue to wait
    lock_table_[rid].cv_.wait(lock_function);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  locks_status_[rid] = LockMode::EXCLUSIVE;
  locks_txn_[rid].insert(txn_id);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  unique_lock<mutex> lock_function(latch_);
  // cout << "UPGRADE FROM " << txn->GetTransactionId() << " ON " << rid << endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  txn_id_t txn_id = txn->GetTransactionId();
  if (locks_status_.find(rid) == locks_status_.end() || locks_status_[rid] != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // check whether this txn has a shared lock
  if (locks_txn_[rid].find(txn_id) == locks_txn_[rid].end()) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // check whether another transaction is pending
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  lock_table_[rid].upgrading_ = txn_id;

  // deadlock prevention
  auto tmp = locks_txn_[rid];
  for (txn_id_t locking_id : tmp) {
    if (locking_id > txn_id) {
      Transaction *locking_txn = TransactionManager::GetTransaction(locking_id);
      locking_txn->SetState(TransactionState::ABORTED);
      RemoveFromEverywhere(locking_txn, &lock_function);
    }
  }

  if (locks_txn_[rid].size() == 1) {
    locks_status_[rid] = LockMode::EXCLUSIVE;
  } else {
    // wait because there is more than 1 txns using this thread
    while (locks_status_[rid] != LockMode::EXCLUSIVE && txn->GetState() != TransactionState::ABORTED) {
      lock_table_[rid].cv_.wait(lock_function);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  lock_table_[rid].upgrading_ = INVALID_TXN_ID;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}
auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  unique_lock<mutex> lock_function(latch_);
  // cout << "UNLOCK FROM " << txn->GetTransactionId() << " ON " << rid << endl;
  return UnlockInner(txn, rid, &lock_function);
}
auto LockManager::UnlockInner(Transaction *txn, const RID &rid, unique_lock<mutex> *lock_function) -> bool {
  if (!(locks_status_[rid] == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    // in repeatable reads level, all growing should be change to shrinking
    // in read commited, s lock can  be released immediately
    // read uncommitted doesn't have s  lock
    txn->SetState(TransactionState::SHRINKING);
  }

  txn_id_t txn_id = txn->GetTransactionId();

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  locks_txn_[rid].erase(txn_id);
  if (locks_status_[rid] == LockMode::SHARED) {
    if (locks_txn_[rid].size() == 1) {
      // check upgrade request
      if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
        // notify pending upgrading request
        // locks_txn_[rid].erase(lock_table_[rid].upgrading_);
        locks_status_[rid] = LockMode::EXCLUSIVE;
        // lock_function.unlock();
        lock_table_[rid].cv_.notify_all();
        return true;
      }
    }
    if (!locks_txn_[rid].empty()) {
      return true;
    }
  }
  // first remove lock status
  locks_status_.erase(rid);
  if (!lock_table_[rid].request_queue_.empty()) {
    if (lock_table_[rid].request_queue_.front().lock_mode_ == LockMode::EXCLUSIVE) {
      // if next txn in queue is exclusive lock, then only this txn will pass
      LockRequest next = lock_table_[rid].request_queue_.front();
      lock_table_[rid].pending_txns_.erase(next.txn_id_);
      lock_table_[rid].request_queue_.pop_front();
    } else {
      // next txn is shared lock, all shared lock should be allowed to pass
      for (auto itr = lock_table_[rid].request_queue_.begin(); itr != lock_table_[rid].request_queue_.end();) {
        if (itr->lock_mode_ == LockMode::SHARED) {
          lock_table_[rid].pending_txns_.erase(itr->txn_id_);
          itr = lock_table_[rid].request_queue_.erase(itr);
        } else {
          itr++;
        }
      }
    }
  }

  // lock_function.unlock();
  lock_table_[rid].cv_.notify_all();

  return true;
}

void LockManager::RemoveFromEverywhere(Transaction *txn, unique_lock<mutex> *lock_function) {
  txn_id_t txn_id = txn->GetTransactionId();

  // remove all pending requests
  for (auto &p : lock_table_) {
    for (auto itr = p.second.request_queue_.begin(); itr != p.second.request_queue_.end(); itr++) {
      if (itr->txn_id_ == txn_id) {
        p.second.request_queue_.erase(itr);
        p.second.cv_.notify_all();
        break;
      }
    }
    if (p.second.upgrading_ == txn_id) {
      p.second.upgrading_ = INVALID_TXN_ID;
      p.second.cv_.notify_all();
    }
  }
  // remove all holding locks
  // auto tmp = *(txn->GetExclusiveLockSet());
  // for (RID rid : tmp) {
  //   UnlockInner(txn, rid, lock_function);
  // }
  // tmp = *(txn->GetSharedLockSet());
  // for (RID rid : tmp) {
  //   UnlockInner(txn, rid, lock_function);
  // }
  // txn->GetSharedLockSet()->clear();
  // txn->GetExclusiveLockSet()->clear();
}

}  // namespace bustub
