#!/bin/bash

BASELINE_COMMIT=34505d26f
CFLAGS="-I../src -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src -I../deps/hdr_histogram -I../deps/rocksdb/include -I../deps/xredis-gtid/include -pedantic -DREDIS_STATIC='' -std=c11 -Wall -W -Wno-missing-field-initializers -O2 -g -ggdb -fno-omit-frame-pointer -DUSE_JEMALLOC -DNDEBUG"
EXCLUDE_FILES="../src/ae_kqueue.c ../src/ae_evport.c"
EXCLUDE_PATTERN='_serverAssert\|_serverPanic\|rdbReportError'

pushd .

cd "$(dirname "$0")"

single_file=""
if (( $# > 0 )); then
    single_file=$1
fi

diff_file_count=0
match_file_count=0
while read -r source_file; do
    if echo $EXCLUDE_FILES | grep $source_file >/dev/null; then
        continue
    fi

    if [[ $single_file != "" ]]; then
        if ! echo $source_file | grep $single_file >/dev/null; then
            continue
        fi
    fi

    git show ${BASELINE_COMMIT}:$source_file > tmp.c
    if [[ $single_file != "" ]]; then cp -f tmp.c ../orig.c; fi
    gcc $CFLAGS -E -P tmp.c | grep -v "${EXCLUDE_PATTERN}" > orig_cpped.c

    cp $source_file tmp.c
    if [[ $single_file != "" ]]; then cp -f tmp.c ../now.c; fi
    gcc $CFLAGS -E -P tmp.c | grep -v "${EXCLUDE_PATTERN}" > now_cpped.c

    source_diff=$(diff orig_cpped.c now_cpped.c)
    if [[ "$source_diff" != "" ]]; then
        ((diff_file_count++))
        echo "[×] $source_file"
    else
        ((match_file_count++))
        echo "[√] $source_file"
    fi
done <<< "$(git ls-tree --name-only ${BASELINE_COMMIT} ../src/*.c)"

if [[ $single_file == "" ]]; then rm -f tmp.c orig_cpped.c now_cpped.c; fi

popd >/dev/null

echo "$match_file_count matched, $diff_file_count diffs."
