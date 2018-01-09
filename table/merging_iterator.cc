//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/heap.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

#include <iostream>

namespace rocksdb {
// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {
typedef BinaryHeap<IteratorWrapper*, MaxIteratorComparator> MergerMaxIterHeap;
typedef BinaryHeap<IteratorWrapper*, MinIteratorComparator> MergerMinIterHeap;
}  // namespace

const size_t kNumIterReserve = 4;

class MergingIterator : public InternalIterator {
 public:
  MergingIterator(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode)
      : is_arena_mode_(is_arena_mode),
        comparator_(comparator),
        current_(nullptr),
        direction_(kForward),
        minHeap_(comparator_),
        prefix_seek_mode_(prefix_seek_mode),
	upper_key_(nullptr), // huanchen
        pinned_iters_mgr_(nullptr) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    for (auto& child : children_) {
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    current_ = CurrentForward();
  }

  virtual void AddIterator(InternalIterator* iter) {
    assert(direction_ == kForward);
    children_.emplace_back(iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    auto new_wrapper = children_.back();
    if (new_wrapper.Valid()) {
      minHeap_.push(&new_wrapper);
      current_ = CurrentForward();
    }
  }

  virtual ~MergingIterator() {
    for (auto& child : children_) {
      child.DeleteIter(is_arena_mode_);
    }
  }

  virtual bool Valid() const override { return (current_ != nullptr); }

  virtual void SeekToFirst() override {
      // huanchen
      target_ = std::string("\0\0\0\0\0\0\0\0", 8);
      Slice target = Slice(target_);
      Seek(target);
  }

  virtual void SeekToLast() override {
      // TODO: rewrite
    ClearHeaps();
    InitMaxHeap();
    for (auto& child : children_) {
      child.SeekToLast();
      if (child.Valid()) {
        maxHeap_->push(&child);
      }
    }
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

    // huanchen
    void SetUpperKey(const Slice* upper_key) override {
	upper_key_ = upper_key;
    }

  virtual void Seek(const Slice& target) override {
      // huanchen
      ClearHeaps();
      children_key_.clear();
      children_is_real_key_.clear();
      all_in_heap_ = false;

      target_ = std::string(target.data(), target.size());

      FillinFilterKeysForward(target, true);
      std::vector<int> min_indexes;
      GetFilterMinKeyIndexes(target, min_indexes);
      FillinRealKeysForward(target, min_indexes);

      direction_ = kForward;
      {
	  PERF_TIMER_GUARD(seek_min_heap_time);
	  current_ = CurrentForward();
      }
  }

  virtual void SeekForPrev(const Slice& target) override {
      // huanchen
      ClearHeaps();
      InitMaxHeap();
      children_key_.clear();
      children_is_real_key_.clear();
      all_in_heap_ = false;

      target_ = std::string(target.data(), target.size());

      FillinFilterKeysBackward(target, true);
      std::vector<int> max_indexes;
      GetFilterMaxKeyIndexes(target, max_indexes);
      FillinRealKeysBackward(target, max_indexes);

      direction_ = kReverse;
      {
	  PERF_TIMER_GUARD(seek_max_heap_time);
	  current_ = CurrentReverse();
      }
  }

  // huanchen
  virtual void Next() override {
      assert(Valid());
      
      if (direction_ != kForward) {
	  SwitchToForward();
	  assert(current_ == CurrentForward());
      }
      assert(current_ == CurrentForward());

      current_->Next();
      if (!current_->Valid()) {
	  minHeap_.pop();
	  current_ = CurrentForward();
      }
      
      if (current_ != nullptr && current_->Valid()) {
	  minHeap_.replace_top(current_);
	  FillinRealKeyMin(current_->key());
	  current_ = CurrentForward();
      }
  }

  virtual void Prev() override {
      assert(Valid());

      if (direction_ != kReverse) {
	  ClearHeaps();
	  InitMaxHeap();
	  for (auto& child : children_) {
	      if (&child != current_) {
		  if (!prefix_seek_mode_) {
		      child.Seek(key());
		      if (child.Valid()) {
			  // Child is at first entry >= key().  Step back one to be < key()
			  TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev",
						   &child);
			  child.Prev();
		      } else {
			  // Child has no entries >= key().  Position at last entry.
			  TEST_SYNC_POINT("MergeIterator::Prev:BeforeSeekToLast");
			  child.SeekToLast();
		      }
		  } else {
		      child.SeekForPrev(key());
		      if (child.Valid() && comparator_->Equal(key(), child.key())) {
			  child.Prev();
		      }
		  }
	      }
	      if (child.Valid()) {
		  maxHeap_->push(&child);
	      }
	  }
	  direction_ = kReverse;
	  if (!prefix_seek_mode_) {
	      current_ = CurrentReverse();
	  }
	  assert(current_ == CurrentReverse());
      }
      
      assert(current_ == CurrentReverse());

      current_->Prev();
      if (!current_->Valid()) {
	  maxHeap_->pop();
	  current_ = CurrentReverse();
      }
      
      if (current_ != nullptr && current_->Valid()) {
	  maxHeap_->replace_top(current_);
	  FillinRealKeyMax(current_->key());
	  current_ = CurrentReverse();
      }
  }

  virtual Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const override {
    Status s;
    for (auto& child : children_) {
      s = child.status();
      if (!s.ok()) {
        break;
      }
    }
    return s;
  }

  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  virtual bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  virtual bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps();
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();

  bool is_arena_mode_;
  const InternalKeyComparator* comparator_;
  autovector<IteratorWrapper, kNumIterReserve> children_;

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper* current_;
  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
  MergerMinIterHeap minHeap_;
  bool prefix_seek_mode_;

    // huanchen
    std::string target_;
    const Slice* upper_key_;
    std::vector<std::string> children_key_;
    std::vector<bool> children_is_real_key_;
    std::vector<int> suffix_bitlen_;
    bool all_in_heap_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

    // huanchen
    // helper
    bool isPrefix(const std::string& a, const unsigned a_bitlen,
		  const std::string& b, const unsigned b_bitlen) {
	std::string s, l; // shorter, longer string
	unsigned s_bitlen;
	if ((a.length() < b.length())
	    || ((a.length() == b.length()) && (a_bitlen <= b_bitlen))) {
	    s = a; l = b;
	    s_bitlen = a_bitlen;
	} else {
	    s = b; l = a;
	    s_bitlen = b_bitlen;
	}
	if (s_bitlen == 8)
	    return (memcmp(s.data(), l.data(), s.length()) == 0);
	std::string s_prefix = s.substr(0, s.length() - 1);
	if (memcmp(s_prefix.data(), l.data(), s_prefix.length()) != 0)
	    return false;
	uint8_t mask = 0xFF << (8 - s_bitlen);
	uint8_t s_suf = (uint8_t)s[s.length() - 1] & mask;
	uint8_t l_suf = (uint8_t)l[s.length() - 1] & mask;
	return (s_suf == l_suf);
    }

    // huanchen
    // helper
    int compareKeys(const std::string& a, const unsigned a_bitlen,
		    const std::string& b, const unsigned b_bitlen) {
	std::string s, l; // shorter, longer string
	unsigned s_bitlen;
	bool order_reversed = false;
	if ((a.length() < b.length())
	    || ((a.length() == b.length()) && (a_bitlen <= b_bitlen))) {
	    s = a; l = b;
	    s_bitlen = a_bitlen;
	} else {
	    s = b; l = a;
	    s_bitlen = b_bitlen;
	    order_reversed = true;
	}
	if (s_bitlen == 8) {
	    int compare = memcmp(s.data(), l.data(), s.length());
	    if (order_reversed) return (0 - compare);
	    return compare;
	}
	std::string s_prefix = s.substr(0, s.length() - 1);
	int compare = memcmp(s_prefix.data(), l.data(), s_prefix.length());
	if (compare != 0) {
	    if (order_reversed) return (0 - compare);
	    return compare;
	}
	uint8_t mask = 0xFF << (8 - s_bitlen);
	uint8_t s_suf = (uint8_t)s[s.length() - 1] & mask;
	uint8_t l_suf = (uint8_t)l[s.length() - 1] & mask;
	if (s_suf < l_suf) {
	    if (order_reversed) return 1;
	    return -1;
	} else if (s_suf == l_suf) {
	    return 0;
	} else {
	    if (order_reversed) return -1;
	    return 1;
	}
    }

    // huanchen
    inline void FillinFilterKeysForward(const Slice& target, const bool inclusive) {
	for (int idx = 0; idx < (int)children_.size(); idx++) {
	    auto& child = children_[idx];
	    unsigned bitlen = 0;
	    std::string filter_key
		= child.FilterSeek(target, &bitlen, inclusive).ToString();
	    if (bitlen == 0)
		bitlen = 8;
	    children_key_.push_back(filter_key);
	    children_is_real_key_.push_back(false);
	    suffix_bitlen_.push_back(bitlen);
	}
    }

    // huanchen
    inline void FillinFilterKeysBackward(const Slice& target, const bool inclusive) {
	for (int idx = 0; idx < (int)children_.size(); idx++) {
	    auto& child = children_[idx];
	    unsigned bitlen = 0;
	    std::string filter_key
		= child.FilterSeekForPrev(target, &bitlen, inclusive).ToString();
	    if (bitlen == 0)
		bitlen = 8;
	    children_key_.push_back(filter_key);
	    children_is_real_key_.push_back(false);
	    suffix_bitlen_.push_back(bitlen);
	}
    }

    // huanchen
    inline void GetFilterMinKeyIndexes(const Slice& target, std::vector<int>& min_indexes) {
	std::string target_str = std::string(target.data(), target.size());
	std::string filter_key_min;
	unsigned suffix_bitlen_min = 8;
	std::vector<int> no_filter_indexes, target_prefix_indexes;
	for (int idx = 0; idx < (int)children_key_.size(); idx++) {
	    if (children_key_[idx].size() == 0) {
		no_filter_indexes.push_back(idx);
		continue;
	    }
	    if (upper_key_ != nullptr) {
		std::string upper_key
		    = std::string(upper_key_->data(), upper_key_->size());
		if (compareKeys(children_key_[idx], suffix_bitlen_[idx], upper_key, 8) > 0)
		    continue;
	    }
	    if (isPrefix(children_key_[idx], suffix_bitlen_[idx], target_str, 8)) {
		target_prefix_indexes.push_back(idx);
	    } else if (filter_key_min.size() == 0) {
		filter_key_min = children_key_[idx];
		suffix_bitlen_min = suffix_bitlen_[idx];
		min_indexes.push_back(idx);
	    } else if (isPrefix(children_key_[idx], suffix_bitlen_[idx], filter_key_min, suffix_bitlen_min)) {
		min_indexes.push_back(idx);
	    } else if (compareKeys(children_key_[idx], suffix_bitlen_[idx], filter_key_min, suffix_bitlen_min) < 0) {
		filter_key_min = children_key_[idx];
		suffix_bitlen_min = suffix_bitlen_[idx];
		min_indexes.clear();
		min_indexes.push_back(idx);
	    }
	}
	for (int i = 0; i < (int)no_filter_indexes.size(); i++)
	    min_indexes.push_back(no_filter_indexes[i]);
	for (int i = 0; i < (int)target_prefix_indexes.size(); i++)
	    min_indexes.push_back(target_prefix_indexes[i]);
    }

    // huanchen
    inline void GetFilterMaxKeyIndexes(const Slice& target, std::vector<int>& max_indexes) {
	std::string target_str = std::string(target.data(), target.size());
	std::string filter_key_max;
	unsigned suffix_bitlen_max = 8;
	std::vector<int> no_filter_indexes, target_prefix_indexes;
	for (int idx = 0; idx < (int)children_key_.size(); idx++) {
	    if (children_key_[idx].size() == 0) {
		no_filter_indexes.push_back(idx);
		continue;
	    }

	    if (isPrefix(children_key_[idx], suffix_bitlen_[idx], target_str, 8)) {
		target_prefix_indexes.push_back(idx);
	    } else if (filter_key_max.size() == 0) {
		filter_key_max = children_key_[idx];
		suffix_bitlen_max = suffix_bitlen_[idx];
		max_indexes.push_back(idx);
	    } else if (isPrefix(children_key_[idx], suffix_bitlen_[idx], filter_key_max, suffix_bitlen_max)) {
		max_indexes.push_back(idx);
	    } else if (compareKeys(children_key_[idx], suffix_bitlen_[idx], filter_key_max, suffix_bitlen_max) > 0) {
		filter_key_max = children_key_[idx];
		suffix_bitlen_max = suffix_bitlen_[idx];
		max_indexes.clear();
		max_indexes.push_back(idx);
	    }
	}
	for (int i = 0; i < (int)no_filter_indexes.size(); i++)
	    max_indexes.push_back(no_filter_indexes[i]);
	for (int i = 0; i < (int)target_prefix_indexes.size(); i++)
	    max_indexes.push_back(target_prefix_indexes[i]);
    }

    // huanchen
    inline void FillinSingleRealKeyForward(const Slice& target, int idx) {
	auto& child = children_[idx];
	{
	    PERF_TIMER_GUARD(seek_child_seek_time);
	    child.Seek(target);
	}
	PERF_COUNTER_ADD(seek_child_seek_count, 1);
	if (child.Valid())
	    minHeap_.push(&child);
	children_is_real_key_[idx] = true;
    }

    // huanchen
    inline void FillinRealKeysForward(const Slice& target, std::vector<int>& indexes) {
	for (int i = 0; i < (int)indexes.size(); i++) {
	    int idx = indexes[i];
	    FillinSingleRealKeyForward(target, idx);
	}
    }

    // huanchen
    inline void FillinSingleRealKeyBackward(const Slice& target, int idx) {	
	auto& child = children_[idx];
	{
	    PERF_TIMER_GUARD(seek_child_seek_time);
	    child.SeekForPrev(target);
	}
	PERF_COUNTER_ADD(seek_child_seek_count, 1);
	if (child.Valid())
	    maxHeap_->push(&child);
	children_is_real_key_[idx] = true;
    }

    // huanchen
    inline void FillinRealKeysBackward(const Slice& target, std::vector<int>& indexes) {
	for (int i = 0; i < (int)indexes.size(); i++) {
	    int idx = indexes[i];
	    FillinSingleRealKeyBackward(target, idx);
	}
    }

    // huanchen
    inline void FillinRealKeyMin(const Slice& target) {
	if (all_in_heap_) return;
	std::string target_str = std::string(target.data(), target.size());
	int count = 0;
	for (int i = 0; i < (int)children_is_real_key_.size(); i++) {
	    if (children_is_real_key_[i]) {
		count++;
		continue;
	    }
	    if (isPrefix(children_key_[i], suffix_bitlen_[i], target_str, 8)
		|| (compareKeys(children_key_[i], suffix_bitlen_[i], target_str, 8) < 0)) {
		Slice seek_target = Slice(target_);
		FillinSingleRealKeyForward(seek_target, i);
	    }
	}
	if (count == (int)children_is_real_key_.size())
	    all_in_heap_ = true;
    }

    // huanchen
    inline void FillinRealKeyMax(const Slice& target) {
	if (all_in_heap_) return;
	std::string target_str = std::string(target.data(), target.size());
	int count = 0;
	for (int i = 0; i < (int)children_is_real_key_.size(); i++) {
	    if (children_is_real_key_[i]) {
		count++;
		continue;
	    }
	    if (isPrefix(children_key_[i], suffix_bitlen_[i], target_str, 8)
		|| (compareKeys(children_key_[i], suffix_bitlen_[i], target_str, 8) > 0)) {
		Slice seek_target = Slice(target_);
		FillinSingleRealKeyBackward(seek_target, i);
	    }
	}
	if (count == (int)children_is_real_key_.size())
	    all_in_heap_ = true;
    }

  void SwitchToForward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    return !maxHeap_->empty() ? maxHeap_->top() : nullptr;
  }
};

void MergingIterator::SwitchToForward() {
  // Otherwise, advance the non-current children.  We advance current_
  // just after the if-block.
  ClearHeaps();
  for (auto& child : children_) {
    if (&child != current_) {
      child.Seek(key());
      if (child.Valid() && comparator_->Equal(key(), child.key())) {
        child.Next();
      }
    }
    if (child.Valid()) {
      minHeap_.push(&child);
    }
  }
  direction_ = kForward;
}

void MergingIterator::ClearHeaps() {
  minHeap_.clear();
  if (maxHeap_) {
    maxHeap_->clear();
  }
}

void MergingIterator::InitMaxHeap() {
  if (!maxHeap_) {
    maxHeap_.reset(new MergerMaxIterHeap(comparator_));
  }
}

InternalIterator* NewMergingIterator(const InternalKeyComparator* cmp,
                                     InternalIterator** list, int n,
                                     Arena* arena, bool prefix_seek_mode) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator(arena);
  } else if (n == 1) {
    return list[0];
  } else {
    if (arena == nullptr) {
      return new MergingIterator(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIterator));
      return new (mem) MergingIterator(cmp, list, n, true, prefix_seek_mode);
    }
  }
}

MergeIteratorBuilder::MergeIteratorBuilder(
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode)
    : first_iter(nullptr), use_merging_iter(false), arena(a) {
  auto mem = arena->AllocateAligned(sizeof(MergingIterator));
  merge_iter =
      new (mem) MergingIterator(comparator, nullptr, 0, true, prefix_seek_mode);
}

MergeIteratorBuilder::~MergeIteratorBuilder() {
  if (first_iter != nullptr) {
    first_iter->~InternalIterator();
  }
  if (merge_iter != nullptr) {
    merge_iter->~MergingIterator();
  }
}

void MergeIteratorBuilder::AddIterator(InternalIterator* iter) {
  if (!use_merging_iter && first_iter != nullptr) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
    first_iter = nullptr;
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(iter);
  } else {
    first_iter = iter;
  }
}

InternalIterator* MergeIteratorBuilder::Finish() {
  InternalIterator* ret = nullptr;
  if (!use_merging_iter) {
    ret = first_iter;
    first_iter = nullptr;
  } else {
    ret = merge_iter;
    merge_iter = nullptr;
  }
  return ret;
}

}  // namespace rocksdb
