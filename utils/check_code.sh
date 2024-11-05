#!/bin/bash

BASELINE_COMMIT=34505d26f
CFLAGS="-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src -I../deps/hdr_histogram -I../deps/rocksdb/include -I../deps/xredis-gtid/include"
EXCLUDE_FILES="../src/ae_kqueue.c ../src/ae_evport.c"

pushd .

cd "$(dirname "$0")"

diff_file_count=0
match_file_count=0
while read -r source_file; do
    if echo $EXCLUDE_FILES | grep $source_file >/dev/null; then
        continue
    fi

    git show ${BASELINE_COMMIT}:$source_file > tmp.c
    gcc $CFLAGS -E tmp.c > orig_cpped.c
    cp $source_file tmp.c
    gcc $CFLAGS -E tmp.c > now_cpped.c
    source_diff=$(diff orig_cpped.c now_cpped.c)
    if [[ "$source_diff" != "" ]]; then
        ((diff_file_count++))
        echo "[×] $source_file"
    else
        ((match_file_count++))
        echo "[√] $source_file"
    fi
done <<< "$(git ls-tree --name-only ${BASELINE_COMMIT} ../src/*.c)"

rm -f tmp.c orig_cpped.c now_cpped.c

popd >/dev/null

echo "$match_file_count matched, $diff_file_count diffs."
