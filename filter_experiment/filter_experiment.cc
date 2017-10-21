// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <endian.h>
#include <errno.h>
#include <time.h>
#include <cinttypes>
#include <cstdio>
#include <thread>
#include <atomic>

#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

//#include "rocksdb/utilities/leveldb_options.h"

/*
void genRandomValue(char* value_buf, int size) {
    for (int i = 0; i < size; i++)
	value_buf[i] = rand() % 256;
}
*/
void init(const std::string& db_path, rocksdb::DB** db,
	  rocksdb::Options* options, rocksdb::BlockBasedTableOptions* table_options,
	  uint64_t key_count, uint64_t value_size, int filter_type, int compression_type) {
    //const std::string kDBPath = "/home/huanchen/rocksdb_datafiles_no_filter";
    //const std::string kDBPath = "/home/huanchen/rocksdb_datafiles_bloom";
    const std::string kKeyPath = "/home/huanchen/rocksdb/filter_experiment/poisson_timestamps.csv";
    char value_buf[value_size];
    memset(value_buf, 0, value_size);

    if (filter_type == 1)
	table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

    if (table_options->filter_policy == nullptr)
	std::cout << "Filter DISABLED\n";
    else
	std::cout << "Using " << table_options->filter_policy->Name() << "\n";

    if (compression_type == 0) {
	options->compression = rocksdb::CompressionType::kNoCompression;
	std::cout << "No Compression\n";
    } else if (compression_type == 1) {
	options->compression = rocksdb::CompressionType::kSnappyCompression;
	std::cout << "Snappy Compression\n";
    }

    //table_options->block_cache = rocksdb::NewLRUCache(10 * 1048576);
    table_options->block_cache = rocksdb::NewLRUCache(10 * 1048576, -1, false, 0.5);
    table_options->cache_index_and_filter_blocks = true;

    options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(*table_options));

    options->max_open_files = 50000;
    options->write_buffer_size = 2 * 1048576;
    options->max_bytes_for_level_base = 10 * 1048576;
    options->target_file_size_base = 2 * 1048576;
    options->use_direct_reads = true;

    options->statistics = rocksdb::CreateDBStatistics();

    //options->create_if_missing = false;
    //options->error_if_exists = false;
    
    //options->prefix_extractor = nullptr;
    //options->disable_auto_compactions = false;
    rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
	std::cout << "creating new DB\n";
	options->create_if_missing = true;
	status = rocksdb::DB::Open(*options, db_path, db);

	if (!status.ok()) {
	    std::cout << status.ToString().c_str() << "\n";
	    assert(false);
	}

	std::cout << "loading timestamp keys\n";
	std::ifstream keyFile(kKeyPath);
	std::vector<uint64_t> keys;

	uint64_t key = 0;
	for (uint64_t i = 0; i < key_count; i++) {
	    keyFile >> key;
	    keys.push_back(key);
	}

	std::cout << "inserting keys\n";
	for (uint64_t i = 0; i < key_count; i++) {
	    key = keys[i];
	    key = htobe64(key);
	    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	    //genRandomValue(value_buf, value_size);
	    rocksdb::Slice s_value(value_buf, value_size);

	    status = (*db)->Put(rocksdb::WriteOptions(), s_key, s_value);
	    if (!status.ok()) {
		std::cout << status.ToString().c_str() << "\n";
		assert(false);
	    }

	    if (i % (key_count / 100) == 0)
		std::cout << i << "/" << key_count << " [" << ((i + 0.0)/(key_count + 0.0) * 100.) << "]\n";
	}

	//std::cout << "compacting\n";
	//rocksdb::CompactRangeOptions compact_range_options;
	//(*db)->CompactRange(compact_range_options, NULL, NULL);
    }
}

void close(rocksdb::DB* db) {
    delete db;
}

void warmup(rocksdb::DB* db, uint64_t key_count, uint64_t key_gap, uint64_t query_count) {
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    std::cout << "warming up\n";
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = key_count * key_gap / query_count * i + 1;
	key = htobe64(key);

	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void benchPointQuery(rocksdb::DB* db, uint64_t key_count, uint64_t key_gap, uint64_t query_count) {
    std::mt19937_64 e(1);
    std::uniform_int_distribution<unsigned long long> dist(0, (key_count * key_gap));

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t r = dist(e);
	query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("point query\n");
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = query_keys[i];
	key = htobe64(key);

	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}
/*
void benchOpenRangeQuery(rocksdb::DB* db, uint64_t key_count, uint64_t key_gap,
			 uint64_t query_count, uint64_t scan_length) {
    std::random_device rd;
    std::mt19937_64 e(rd());
    std::uniform_int_distribution<unsigned long long> dist(0, (key_count * key_gap));

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t r = dist(e);
	query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("open range query\n");
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = query_keys[i];
	key = htobe64(key);
	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	
	std::string s_value;
	uint64_t value;

	uint64_t j = 0;
	for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++) {
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    assert(it->value().size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(it->value().data());
	    (void)value;
	    break;
	}
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";

    delete it;
}

void benchClosedRangeQuery(rocksdb::DB* db, uint64_t key_count, uint64_t key_gap,
			   uint64_t query_count, uint64_t range_size) {
    std::random_device rd;
    std::mt19937_64 e(rd());
    std::uniform_int_distribution<unsigned long long> dist(0, (key_count * key_gap));

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t r = dist(e);
	query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("closed range query\n");
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    uint64_t count = 0;
    
    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = query_keys[i];
	key = htobe64(key);
	rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));

	uint64_t until_key = query_keys[i] + 1000000;
	until_key = htobe64(until_key);
	rocksdb::Slice s_until_key(reinterpret_cast<const char*>(&until_key), sizeof(until_key));
	bool inclusive = false;
	
	std::string s_value;
	uint64_t value;

	uint64_t j = 0;
	for (it->SeekUntil(s_key, s_until_key, inclusive); it->Valid(); it->Next(), j++) {
	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
	    if (be64toh(found_key) >= be64toh(until_key))
		break;
	    count++;
	}
    }

    std::cout << "count per op = " << ((count + 0.0) / query_count) << "\n";
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";

    delete it;
}
*/

void printIO() {
    FILE* fp = fopen("/sys/block/sda/sda2/stat", "r");
    if (fp == NULL) {
	printf("Error: empty fp\n");
	printf("%s\n", strerror(errno));
	return;
    }
    char buf[4096];
    if (fgets(buf, sizeof(buf), fp) != NULL)
	printf("%s", buf);
    fclose(fp);
    printf("\n");
}

int main(int argc, const char* argv[]) {
    if (argc < 4) {
	std::cout << "Usage:\n";
	std::cout << "arg 1: path to datafiles\n";
	std::cout << "arg 2: filter type\n";
	std::cout << "\t0: no filter\n";
	std::cout << "\t1: Bloom filter\n";
	//std::cout << "\t2: SuRF\n";
	std::cout << "arg 3: compression?\n";
	std::cout << "\t0: no compression\n";
	std::cout << "\t1: Snappy\n";
	return -1;
    }

    std::string db_path = std::string(argv[1]);
    int filter_type = atoi(argv[2]);
    int compression_type = atoi(argv[3]);
    uint64_t scan_length = 1;
    uint64_t range_size = 100000;

    //const uint64_t kKeyCount = 100000000;
    const uint64_t kKeyCount = 2000000;
    const uint64_t kValueSize = 1000;
    const uint64_t kKeyGap = 100000;

    const uint64_t kWarmupQueryCount = 100000;
    const uint64_t kQueryCount = 50000;

    //=========================================================================
    
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    /*
    rocksdb::LevelDBOptions level_options;

    if (filter_type == 1)
	level_options.filter_policy = rocksdb::NewBloomFilterPolicy(10, false);

    if (level_options.filter_policy == nullptr)
	std::cout << "Filter DISABLED\n";
    else
	std::cout << "Using " << level_options.filter_policy->Name() << "\n";
    
    rocksdb::Options options = rocksdb::ConvertOptions(level_options);
    */
    
    init(db_path, &db, &options, &table_options, kKeyCount, kValueSize, filter_type, compression_type);

    //=========================================================================
    
    std::cout << options.statistics->ToString() << "\n";
    printIO();
    warmup(db, kKeyCount, kKeyGap, kWarmupQueryCount);
    //std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    std::cout << options.statistics->ToString() << "\n";
    printIO();
    benchPointQuery(db, kKeyCount, kKeyGap, kQueryCount);
    //benchOpenRangeQuery(db, kKeyCount, kKeyGap, kQueryCount, scan_length);
    //benchClosedRangeQuery(db, kKeyCount, kKeyGap, kQueryCount, range_size);
    
    std::cout << options.statistics->ToString() << "\n";
    //std::string stats;
    //db->GetProperty(rocksdb::Slice("rocksdb.stats"), &stats);
    //std::cout << stats << "\n";
    printIO();

    close(db);

    return 0;
}
