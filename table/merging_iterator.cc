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
    ClearHeaps();
    for (auto& child : children_) {
      child.SeekToFirst();
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    direction_ = kForward;
    current_ = CurrentForward();
  }

  virtual void SeekToLast() override {
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
      std::vector<int> min_indexes;
      FilterChildrenForward(target, min_indexes);

    ClearHeaps();
    //for (auto& child : children_) { // ori
    for (int i = 0; i < (int)min_indexes.size(); i++) { // huanchen
	int idx = min_indexes[i]; // huanchen
	auto& child = children_[idx]; // huanchen
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.Seek(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      if (child.Valid()) {
        PERF_TIMER_GUARD(seek_min_heap_time);
        minHeap_.push(&child);
      }
    }
    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  virtual void SeekForPrev(const Slice& target) override {
      // huanchen
      std::vector<int> max_indexes;
      FilterChildrenBackward(target, max_indexes);
      
    ClearHeaps();
    InitMaxHeap();

    //for (auto& child : children_) { // ori
    for (int i = 0; i < (int)max_indexes.size(); i++) { // huanchen
	int idx = max_indexes[i]; // huanchen
	auto& child = children_[idx]; // huanchen
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.SeekForPrev(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      if (child.Valid()) {
        PERF_TIMER_GUARD(seek_max_heap_time);
        maxHeap_->push(&child);
      }
    }
    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }
    /*
  virtual void SeekForPrev(const Slice& target) override {
    ClearHeaps();
    InitMaxHeap();

    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.SeekForPrev(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      if (child.Valid()) {
        PERF_TIMER_GUARD(seek_max_heap_time);
        maxHeap_->push(&child);
      }
    }
    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }
    */
  virtual void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      SwitchToForward();
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
      assert(current_ == CurrentForward());
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());

    // as the current points to the current record. move the iterator forward.
    current_->Next();
    if (current_->Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      minHeap_.pop();
    }
    current_ = CurrentForward();
  }

  virtual void Prev() override {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (direction_ != kReverse) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
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
        // Note that we don't do assert(current_ == CurrentReverse()) here
        // because it is possible to have some keys larger than the seek-key
        // inserted between Seek() and SeekToLast(), which makes current_ not
        // equal to CurrentReverse().
        current_ = CurrentReverse();
      }
      // The loop advanced all non-current children to be < key() so current_
      // should still be strictly the smallest key.
      assert(current_ == CurrentReverse());
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());

    current_->Prev();
    if (current_->Valid()) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      maxHeap_->replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      maxHeap_->pop();
    }
    current_ = CurrentReverse();
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

    const Slice* upper_key_; // huanchen

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

    // huanchen
    // helper
    bool isPrefix(const std::string& a, const unsigned a_bitlen,
		  const std::string& b, const unsigned b_bitlen) {
	/*
	std::cout << "a = ";
	for (int j = 0; j < (int)a.size(); j++)
	    std::cout << std::hex << (uint16_t)a[j] << " ";
	std::cout << std::dec << "\n";
	std::cout << "a_bitlen = " << a_bitlen << "\n";

	std::cout << "b = ";
	for (int j = 0; j < (int)b.size(); j++)
	    std::cout << std::hex << (uint16_t)b[j] << " ";
	std::cout << std::dec << "\n";
	std::cout << "b_bitlen = " << b_bitlen << "\n";
	*/
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
    void FilterChildrenForward(const Slice& target, std::vector<int>& min_indexes) {
	std::string target_str = std::string(target.data(), target.size());
	std::string filter_key_min;
	unsigned bitlen_min = 0;
	std::vector<int> no_filter_indexes;
	std::vector<int> target_prefix_indexes;
	for (int idx = 0; idx < (int)children_.size(); idx++) {
	    auto& child = children_[idx];
	    unsigned bitlen = 0;
	    std::string filter_key_cur
		= child.FilterSeek(target, &bitlen).ToString();
	    if (bitlen == 0)
		bitlen = 8;

	    if (filter_key_cur.size() == 0) {
		no_filter_indexes.push_back(idx);
		continue;
	    }

	    if (upper_key_ != nullptr) {
		std::string upper_key = std::string(upper_key_->data(), upper_key_->size());
		int compare = compareKeys(filter_key_cur, bitlen, upper_key, 8);
		if (compare > 0)
		    continue;
	    }

	    if (isPrefix(filter_key_cur, bitlen, target_str, 8)) {
		target_prefix_indexes.push_back(idx);
	    } else if (filter_key_min.size() == 0) {
		filter_key_min = filter_key_cur;
		bitlen_min = bitlen;
		min_indexes.push_back(idx);
	    } else if (isPrefix(filter_key_cur, bitlen, filter_key_min, bitlen_min)) {
		min_indexes.push_back(idx);
	    } else if (compareKeys(filter_key_cur, bitlen, filter_key_min, bitlen_min) < 0) {
		filter_key_min = filter_key_cur;
		bitlen_min = bitlen;
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
    void FilterChildrenBackward(const Slice& target, std::vector<int>& max_indexes) {
	std::string target_str = std::string(target.data(), target.size());
	std::string filter_key_max;
	unsigned bitlen_max = 0;
	std::vector<int> no_filter_indexes;
	std::vector<int> target_prefix_indexes;
	for (int idx = 0; idx < (int)children_.size(); idx++) {
	    auto& child = children_[idx];
	    unsigned bitlen = 0;
	    std::string filter_key_cur
		= child.FilterSeekForPrev(target, &bitlen).ToString();
	    /*
	    std::cout << "\tidx = " << idx << "\t";
	    std::cout << "filter_key_cur.size() = " << filter_key_cur.size() << "\t";
	    for (int j = 0; j < (int)filter_key_cur.size(); j++)
	    	std::cout << std::hex << (uint16_t)filter_key_cur[j] << " ";
	    std::cout << std::dec << "\n";
	    */
	    if (bitlen == 0)
		bitlen = 8;

	    if (filter_key_cur.size() == 0) {
		no_filter_indexes.push_back(idx);
		continue;
	    }

	    if (isPrefix(filter_key_cur, bitlen, target_str, 8)) {
		target_prefix_indexes.push_back(idx);
	    } else if (filter_key_max.size() == 0) {
		filter_key_max = filter_key_cur;
		bitlen_max = bitlen;
		max_indexes.push_back(idx);
	    } else if (isPrefix(filter_key_cur, bitlen, filter_key_max, bitlen_max)) {
		max_indexes.push_back(idx);
	    } else if (compareKeys(filter_key_cur, bitlen, filter_key_max, bitlen_max) > 0) {
		filter_key_max = filter_key_cur;
		bitlen_max = bitlen;
		max_indexes.clear();
		max_indexes.push_back(idx);
	    }
	    /*
	    std::cout << "\tfilter_key_max = ";
	    for (int j = 0; j < (int)filter_key_max.size(); j++)
	    	std::cout << std::hex << (uint16_t)filter_key_max[j] << " ";
	    std::cout << std::dec << "\n";

	    std::cout << "\tmax indexes: ";
	    for (int k = 0; k < (int)max_indexes.size(); k++) {
	    	std::cout << max_indexes[k] << " ";
	    }
	    std::cout << "\n";
	    */
	}

	for (int i = 0; i < (int)no_filter_indexes.size(); i++)
	    max_indexes.push_back(no_filter_indexes[i]);

	for (int i = 0; i < (int)target_prefix_indexes.size(); i++)
	    max_indexes.push_back(target_prefix_indexes[i]);
	/*
	std::cout << "\tmax indexes: ";
	for (int k = 0; k < (int)max_indexes.size(); k++) {
	    std::cout << max_indexes[k] << " ";
	}
	std::cout << "\n";
	*/
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
