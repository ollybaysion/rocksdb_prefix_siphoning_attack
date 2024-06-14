# Prefix Siphoning on RocksDB with SuRF
- Re-Implementation of Prefix Siphoning [USENIX ATC23](https://www.usenix.org/conference/atc23/presentation/kaufman)
- Succint Range Filter: [SuRF](https://github.com/efficient/SuRF)
- RocksDB with SuRF: [RocksDB](https://github.com/efficient/rocksdb)

## Build
    git submodule init
    git submodule update
    mkdir build
    cd build
    cmake ..
    make -j

## Generate Workloads
We used YCSB Basic.

...at third_party/SuRF/bench

    cd workload_gen
    bash ycsb_download.sh
    bash gen_workload.sh

    cd ..
    bash run.sh

You can find result in bench/workloads

## Run Benchmarks
...at build/filter_experiment (keypath from the above)

### Insert Keys on your DB
    ./filter_experiment --db=/path/to --benchmarks=fillrandom --distribution=real --filter_type=surf_real --num=500000 --value_size=1000 --key_path=/your/keypath

### Scan Real Key
    ./filter_experiment --db=/path/to --benchmarks=scan --distribution=real --filter_type=surf_real --num=100000 --key_path=/your/keypath

### Attack
    ./filter_experiment --db=/path/to --benchmarks=scan,attack --findfpk=1 --distribution=random --key_path=/your/keypath --th_findfpk_min=20 --th_findfpk_max=3000
    

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage
If you want to know RocksDB more, visit [RocksDB](https://github.com/facebook/rocksdb)
