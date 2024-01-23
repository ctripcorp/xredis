# ROR Build On Mac Silicon Manual

## Pre-Requirement
1. Install and configure snappy and bzip.
    ```bash
    # example for homebrew
    brew install snappy bzip2
    ```

## Git Pull Submodules
1. Clone submodules like below.
2. Tips:
   1. ROR relys on Rocksdb function `rocksdb_options_add_compact_on_deletion_collector_factory(rocksdb_options_t*, size_t window_size, size_t num_dels_trigger, double deletion_ratio);` (deps/rocksdb/include/rocksdb/c.h) which is modified in later version, so **REMEMBER TO REVERT TO A QUALIFIED VERSION TO MATCH THE CALLER**.
    ```bash
    # Following steps are all executed at Redis-On-Rocks dir
    $ git submodule update --init --recursive
    $ cd deps/rocksdb && git checkout 05df30a856d9b9baf8dbd8396722558d7f07aac9

    # Following is my version of submodule
    $ git submodule status
    071c33846d576edc1e59d26b1199eba968e71ca2 deps/rocksdb (blob_st_lvl-pre-621-g071c33846)
    456989ff7c9a601a88a7dd14d32789b266dc6fc7 deps/xredis-gtid (heads/master)

    # Apply ./build_support_on_mac_silicon/rocksdb.patch to deps/rocksdb
    $ cp ./build_support_on_mac_silicon/rocksdb.patch ./deps/rocksdb && cd deps/rocksdb && git apply ./rocksdb.patch
    ```

## Begin to Build
1. Now you can build like following
   ```bash
   # cd Redis-On-Rocks dir
   $ make
   ```