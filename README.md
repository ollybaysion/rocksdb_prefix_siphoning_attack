# RocksDB Equipped With SuRF
In this repository, we integrate [SuRF](https://github.com/efficient/SuRF)
into RocksDB. Please refer to our
[SIGMOD paper](http://www.cs.cmu.edu/~huanche1/publications/surf_paper.pdf)
for details.
Most of the changes to the source code are marked with "*// huanchen*".

## Install Dependencies
    sudo apt-get install build-essential cmake libgtest.dev libsnappy-dev
    cd /usr/src/gtest
    sudo cmake CMakeLists.txt
    sudo make
    sudo cp *.a /usr/lib

## Build
    git submodule init
    git submodule update
    mkdir build
    cd build
    cmake -DWITH_SNAPPY=ON ..
    make -j 8

## Generate Workloads and RocksDB Instances
The experiment results presented in our
[SIGMOD paper](http://www.cs.cmu.edu/~huanche1/publications/surf_paper.pdf)
uses 100GB datasets. Because it takes a long time to run, we scale
down to 2GB datasets here (the 100GB configs are still included in
**filter_experiment/filter_experiment.cc**, but commented out).

    cd filter_experiment
    python poisson.py
    mkdir data_no_filter data_bloom data_surf

Run the executable **../build/filter_experiment/filter_experiment** will show the usage information.

To initialize the RocksDB instances with no filter, bloom filters and SuRFs (with real suffixes):

    ../build/filter_experiment/filter_experiment data_no_filter 0 1 0 0 0 0
    ../build/filter_experiment/filter_experiment data_bloom     1 1 0 0 0 0
    ../build/filter_experiment/filter_experiment data_surf      4 1 0 0 0 0

## Run Benchmarks
You may want to clear system cache (echo 3 | sudo tee /proc/sys/vm/drop_caches)
before running each experiment. The source file for the experiments is
**filter_experiment/filter_experiment.cc**. To get correct I/O counts, you need
to specify the device in function **getIOCount()** and **printIO()**.
**filter_experiment.cc** includes more configurations (e.g., specify filter sizes)
to run different experiments besides the following examples:

    // point queries
    ../build/filter_experiment/filter_experiment data_no_filter 0 1 1 1 0 0
    ../build/filter_experiment/filter_experiment data_bloom     1 1 1 1 0 0
    ../build/filter_experiment/filter_experiment data_surf      4 1 1 1 0 0

    // closed-range queries (50% queries return empty results)
    ../build/filter_experiment/filter_experiment data_no_filter 0 1 1 3 69310 0
    ../build/filter_experiment/filter_experiment data_bloom     1 1 1 3 69310 0
    ../build/filter_experiment/filter_experiment data_surf      4 1 1 3 69310 0


## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)


RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/
