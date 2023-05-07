/* Copyright (c) 2023, ctrip.com * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"
#include "ctrip_lru_cache.h"

#define SWAP_PERSIST_MAX_KEYS_PER_LOOP 256
#define SWAP_PERSIST_MEM_PER_KEY (DEFAULT_KEY_SIZE+sizeof(kvpCacheEntry)+\
        sizeof(dictEntry)+sizeof(listNode))

int submitEvictClientRequest(client *c, robj *key, uint64_t persist_version);

void swapPersistKeyRequestFinished(swapPersistCtx *ctx, int dbid, robj *key,
        uint64_t persist_version) {
    uint64_t current_version;
    redisDb *db = server.db+dbid;
    lruCache *persist_keys = ctx->keys[dbid];
    if (lruCacheLookup(persist_keys,key->ptr,(void**)&current_version)) {
        serverAssert(persist_version <= current_version);
        if (current_version == persist_version) {
            robj *o = lookupKey(db,key,LOOKUP_NOTOUCH);
            if (o == NULL || !objectIsDirty(o)) {
                /* key (with persis_version) persist finished. */
                lruCacheDelete(persist_keys,key->ptr);
            } else {
                /* persist request will later started again */
            }
        } else {
            /* persist started by another attempt */
        }
    }
}

swapPersistCtx *swapPersistCtxNew() {
    int dbnum = server.dbnum;
    swapPersistCtx *ctx = zmalloc(sizeof(swapPersistCtx));
    ctx->keys = zmalloc(dbnum*sizeof(lruCache));
    for (int i = 0; i < dbnum; i++) {
        ctx->keys[i] = lruCacheNew(LRU_CACHE_TYPE_KVP,ULLONG_MAX);
    }
    ctx->version = SWAP_PERSIST_VERSION_INITIAL;
    return ctx;
}

void swapPersistCtxFree(swapPersistCtx *ctx) {
    int dbnum = server.dbnum;
    if (ctx == NULL) return;
    if (ctx->keys != NULL) {
        for (int i = 0; i < dbnum; i++) {
            if (ctx->keys[i] != NULL) {
                lruCacheFree(ctx->keys[i]);
                ctx->keys[i] = NULL;
            }
        }
        zfree(ctx->keys);
        ctx->keys = NULL;
    }
    zfree(ctx);
}

inline size_t swapPersistCtxKeysCount(swapPersistCtx *ctx) {
    if (ctx == NULL) return 0;
    size_t keys_count = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        lruCache *persist_keys = ctx->keys[dbid];
        keys_count += lruCacheCount(persist_keys);
    }
    return keys_count;
}

inline size_t swapPersistCtxUsedMemory(swapPersistCtx *ctx) {
    return swapPersistCtxKeysCount(ctx)*SWAP_PERSIST_MEM_PER_KEY;
}

void swapPersistCtxAddKey(swapPersistCtx *ctx, redisDb *db, robj *key) {
    lruCache *persist_keys = ctx->keys[db->id];
    uint64_t persist_version = ctx->version++;
    lruCachePut(persist_keys,key->ptr,(void*)persist_version,NULL);
}

static void tryPersistKey(redisDb *db, robj *key, uint64_t persist_version) {
    client *evict_client = server.evict_clients[db->id];
    if (lockWouldBlock(server.swap_txid++, db, key)) return;
    submitEvictClientRequest(evict_client,key,persist_version);
}

static inline int reachedPersistInprogressLimit() {
    return server.swap_evict_inprogress_count >= server.swap_evict_inprogress_limit;
}

void swapPersistCtxPersistKeys(swapPersistCtx *ctx) {
    uint64_t count = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        lruCacheIter *iter;
        redisDb *db = server.db+dbid;
        lruCache *persist_keys = ctx->keys[dbid];
        if (lruCacheCount(persist_keys) == 0) continue;
        iter = lruCacheGetIterator(persist_keys, LRU_CAHCHE_ITER_FROM_OLDEST);
        while (lruCacheIterNext(iter) &&
                !reachedPersistInprogressLimit() &&
                count < SWAP_PERSIST_MAX_KEYS_PER_LOOP) {
            sds keyptr = lruCacheIterKey(iter);
            robj *key = createStringObject(keyptr,sdslen(keyptr));
            uint64_t persist_version = (uint64_t)lruCacheIterVal(iter);
            tryPersistKey(db,key,persist_version);
        }
    }
}

sds genSwapPersistInfoString(sds info) {
    if (server.swap_persist_enabled) {
        size_t keys, mem;
        keys = swapPersistCtxKeysCount(server.swap_persist_ctx);
        mem = swapPersistCtxUsedMemory(server.swap_persist_ctx);
        info = sdscatprintf(info,
                "swap_persist_keys_scheduled:%lu\r\n"
                "swap_persist_used_memory:%lu\r\n",
                keys,mem);
    }
    return info;
}

/* scan meta cf to rebuild cold_keys/cold_filter & fix keys */
void loadDataFromRocksdb() {
    struct rocks *rocks = server.rocks;
    rocksdb_iterator_t *meta_iter = rocksdb_create_iterator_cf(
            rocks->db, rocks->ropts,rocks->cf_handles[META_CF]);

    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        sds meta_start_key = rocksEncodeDbRangeStartKey(db->id);
        sds meta_end_key = rocksEncodeDbRangeEndKey(db->id);

        long long start_time = ustime();
        rocksdb_iter_seek(meta_iter,meta_start_key,sdslen(meta_start_key));

        while (rocksdb_iter_valid(meta_iter)) {
            int dbid;
            const char *rawkey, *key;
            size_t rklen, klen, minlen;
            robj *keyobj = NULL;

            rawkey = rocksdb_iter_key(meta_iter,&rklen);

            minlen = rklen < sdslen(meta_end_key) ? rklen : sdslen(meta_end_key);
            if (memcmp(rawkey,meta_end_key,minlen) >= 0) break; /* dbid switched */

            rocksDecodeMetaKey(rawkey,rklen,&dbid,&key,&klen);

            keyobj = createStringObject(key,klen);

            tryLoadKey(db,keyobj,0);

            db->cold_keys++;
            coldFilterAddKey(db->cold_filter,keyobj->ptr);

            rocksdb_iter_next(meta_iter);
        }

        sdsfree(meta_start_key);
        sdsfree(meta_end_key);

        if (db->cold_keys) {
            double elapsed = (double)(ustime() - start_time)/1000000;
            serverLog(LL_NOTICE,
                    "[persist] db-%d loaded %lld keys from rocksdb in %.2f seconds.",
                    i,db->cold_keys,elapsed);
        }
    }
}

static int keyspaceIsEmpty() {
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        if (ctripDbSize(db)) return 0;
    }
    return 1;
}

void loadDataFromDisk(void);
void ctripLoadDataFromDisk(void) {
    if (server.swap_mode != SWAP_MODE_MEMORY &&
            server.swap_persist_enabled) {
        loadDataFromRocksdb();
    }

    setFilterState(FILTER_STATE_OPEN);

    if (keyspaceIsEmpty()) loadDataFromDisk();
}

