// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "gtest/gtest.h"

#include <endian.h>
#include <errno.h>
#include <time.h>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <thread>
#include <atomic>

#include <stdio.h>
#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <algorithm>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace rocksdb {

namespace filtertest {

static const std::string kFilePath = "../../test_filter/words.txt";
static const int kTestSize = 234369;
static const int kValueSize = 100;

class FilterTest : public ::testing::Test {
public:
    void loadWordList();
    void loadInts();
    void setupDir();
    void loadDataFilePaths();
    
    void query();
    void initSingleConfig(Options& options,
			  const std::string& db_path,
			  int key_type);
    void init_options();
    void init_key_types();

    void pointQuerySingleConfig(int key_type);
    void rangeQuerySingleConfig(int key_type);

private:
    std::vector<std::string> words_;
    std::vector<uint64_t> ints_;
    std::vector<uint64_t> ints_sorted_;
    
    std::vector<std::string> datafiles_;
    DB* db_;
    std::vector<Options> options_vec_;
    std::vector<int> key_type_vec_;
};

void exec(const std::string& cmd) {
    std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
}
    
void FilterTest::loadDataFilePaths() {
    datafiles_.push_back(std::string("./bloom_word"));
    datafiles_.push_back(std::string("./surf_hash_word"));
    datafiles_.push_back(std::string("./surf_real_word"));

    datafiles_.push_back(std::string("./bloom_int"));
    datafiles_.push_back(std::string("./surf_hash_int"));
    datafiles_.push_back(std::string("./surf_real_int"));

    //datafiles_.push_back(std::string("./bloom_word_compress"));
    //datafiles_.push_back(std::string("./surf_hash_word_compress"));
    //datafiles_.push_back(std::string("./surf_real_word_compress"));

    //datafiles_.push_back(std::string("./bloom_int_compress"));
    //datafiles_.push_back(std::string("./surf_hash_int_compress"));
    //datafiles_.push_back(std::string("./surf_real_int_compress"));
}

void FilterTest::setupDir() {
    std::string rm = "rm -rf ";
    std::string mkdir = "mkdir ";

    for (int i = 0; i < (int)datafiles_.size(); i++) {
	std::string cmd = rm + datafiles_[i];
	exec(cmd);
    }

    for (int i = 0; i < (int)datafiles_.size(); i++) {
	std::string cmd = mkdir + datafiles_[i];
	exec(cmd);
    }
}

void FilterTest::loadWordList() {
    std::ifstream infile(kFilePath);
    std::string key;
    int count = 0;
    while (infile.good() && count < kTestSize) {
        infile >> key;
        words_.push_back(key);
        count++;
    }
}

void FilterTest::loadInts() {
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);
    for (int i = 0; i < kTestSize; i++) {
	uint64_t num = dist(e);
	ints_.push_back(num);
	ints_sorted_.push_back(num);
    }
    std::sort(ints_sorted_.begin(), ints_sorted_.end());
}

void FilterTest::init_options() {
    for (int i = 0; i < (int)datafiles_.size(); i++) {
	BlockBasedTableOptions table_options;
	table_options.block_cache = NewLRUCache(100 * 1048576);
	table_options.pin_l0_filter_and_index_blocks_in_cache = true;
	table_options.cache_index_and_filter_blocks = true;

	if (i % 3 == 0)
	    table_options.filter_policy.reset(NewBloomFilterPolicy(14, false));
	else if (i % 3 == 1)
	    table_options.filter_policy.reset(NewSuRFPolicy(1, 4, true, 16, false));
	else
	    table_options.filter_policy.reset(NewSuRFPolicy(2, 4, true, 16, false));
	
	Options options;
	options.table_factory.reset(NewBlockBasedTableFactory(table_options));
	
	options.max_open_files = -1; // pre-load indexes and filters
	options.write_buffer_size = 256 * 1024;
	options.max_bytes_for_level_base = 1024 * 1024;
	options.target_file_size_base = 256 * 1024;

	options.statistics = CreateDBStatistics();

	if (i < 6)
	    options.compression = CompressionType::kNoCompression;
	else
	    options.compression = CompressionType::kSnappyCompression;

	options_vec_.push_back(options);
    }
}

void FilterTest::init_key_types() {
    for (int i = 0; i < (int)datafiles_.size(); i++) {
	if ((i / 3 == 0) || (i / 3 == 2))
	    key_type_vec_.push_back(0);
	else
	    key_type_vec_.push_back(1);
    }
}

void FilterTest::initSingleConfig(Options& options,
				  const std::string& db_path,
				  int key_type) {
    Status status = DB::Open(options, db_path, (&db_));
    if (!status.ok()) {
	options.create_if_missing = true;
 	status = DB::Open(options, db_path, (&db_));

	if (!status.ok()) {
	    std::cout << status.ToString().c_str() << "\n";
	    ASSERT_TRUE(false);
	}

	char value_buf[kValueSize];
	if (key_type == 0) {
	    for (int i = 0; i < kTestSize; i++) {
		Slice s_key(words_[i].c_str(), words_[i].size());
		memset(value_buf, 0, kValueSize);
		memcpy(value_buf, words_[i].c_str(), words_[i].size());
		Slice s_value(value_buf, kValueSize);
		
		status = db_->Put(WriteOptions(), s_key, s_value);
		if (!status.ok()) {
		    std::cout << status.ToString().c_str() << "\n";
		    ASSERT_TRUE(false);
		}
	    }
	}
	else {
	    for (int i = 0; i < kTestSize; i++) {
		uint64_t key = ints_[i];
		memset(value_buf, 0, kValueSize);
		memcpy(value_buf, reinterpret_cast<const char*>(&key), sizeof(key));
		
		key = htobe64(key);
		Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
		Slice s_value(value_buf, kValueSize);
		
		status = db_->Put(WriteOptions(), s_key, s_value);
		if (!status.ok()) {
		    std::cout << status.ToString().c_str() << "\n";
		    ASSERT_TRUE(false);
		}
	    }
	}

	options.create_if_missing = false;
    }
}

void FilterTest::pointQuerySingleConfig(int key_type) {
    if (key_type == 0) {
	for (int i = 0; i < kTestSize; i++) {
	    Slice s_key(words_[i].c_str(), words_[i].size());
	    std::string s_value;
	    Status status = db_->Get(ReadOptions(), s_key, &s_value);
	    if (!status.ok()) {
		std::cout << status.ToString().c_str() << "\n";
		ASSERT_TRUE(false);
	    }
	    else {
		ASSERT_EQ(kValueSize, s_value.size());
		int cmp = memcmp(words_[i].c_str(), s_value.c_str(), words_[i].size());
		ASSERT_EQ(0, cmp);
	    }
	}
    }
    else {
	for (int i = 0; i < kTestSize; i++) {
	    uint64_t key = ints_[i];
	    key = htobe64(key);
	    Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	    std::string s_value;
	    Status status = db_->Get(ReadOptions(), s_key, &s_value);
	    if (!status.ok()) {
		std::cout << status.ToString().c_str() << "\n";
		ASSERT_TRUE(false);
	    }
	    else {
		ASSERT_EQ(kValueSize, s_value.size());
		int cmp = memcmp(reinterpret_cast<const char*>(&ints_[i]), s_value.c_str(), sizeof(ints_[i]));
		ASSERT_EQ(0, cmp);
	    }
	}
    }
}

void FilterTest::rangeQuerySingleConfig(int key_type) {
    if (key_type == 1) {
	Iterator* it = db_->NewIterator(ReadOptions());

	for (int i = 0; i < kTestSize - 1; i++) {
	    uint64_t key = ints_sorted_[i] + (ints_sorted_[i+1] - ints_sorted_[i]) / 2;
	    key = htobe64(key);
	    Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	    std::string s_value;
	    
	    it->Seek(s_key);
	    ASSERT_TRUE(it->Valid());
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    found_key = be64toh(found_key);
	    ASSERT_EQ(ints_sorted_[i+1], found_key);

	    for (int j = 0; j < 10; j++) {
		if (it->Valid()) {
		    it->Next();
		    if ((i + j + 2) < kTestSize) {
			ASSERT_TRUE(it->Valid());
			found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
			found_key = be64toh(found_key);
			ASSERT_EQ(ints_sorted_[i+j+2], found_key);
		    } else {
			ASSERT_FALSE(it->Valid());
		    }
		}
	    }
	}

	for (int i = 0; i < kTestSize - 1; i++) {
	    uint64_t key = ints_sorted_[i] + (ints_sorted_[i+1] - ints_sorted_[i]) / 2;
	    key = htobe64(key);
	    Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	    std::string s_value;
	    
	    it->SeekForPrev(s_key);
	    ASSERT_TRUE(it->Valid());
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    found_key = be64toh(found_key);
	    ASSERT_EQ(ints_sorted_[i], found_key);

	    for (int j = 0; j < 10; j++) {
		if (it->Valid()) {
		    it->Prev();
		    if ((i - j) > 0) {
			ASSERT_TRUE(it->Valid());
			found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
			found_key = be64toh(found_key);
			ASSERT_EQ(ints_sorted_[i-j-1], found_key);
		    } else {
			ASSERT_FALSE(it->Valid());
		    }
		}
	    }
	}

    }
}

void FilterTest::query() {
    loadWordList();
    loadInts();
    loadDataFilePaths();
    init_options();
    init_key_types();

    //setupDir();
    for (int i = 0; i < (int)datafiles_.size(); i++) {
	std::cout << datafiles_[i] << "----------------------------------\n";
	initSingleConfig(options_vec_[i], datafiles_[i], key_type_vec_[i]);
	pointQuerySingleConfig(key_type_vec_[i]);
	rangeQuerySingleConfig(key_type_vec_[i]);
    }

}


TEST_F (FilterTest, EmptyTest) {
    ASSERT_TRUE(true);
}

TEST_F (FilterTest, QueryTest) {
    query();
    ASSERT_TRUE(true);
}

} // namespace filtertest

} // namespace rocksdb

int main (int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
