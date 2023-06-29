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

#define PERSISTING_KEY_ENTRY_MEM (DEFAULT_KEY_SIZE+sizeof(persistingKeyEntry)+\
        sizeof(dictEntry)+sizeof(listNode))

persistingKeyEntry *persistingKeyEntryNew(listNode *ln, uint64_t version,
        mstime_t mstime) {
    persistingKeyEntry *e = zmalloc(sizeof(persistingKeyEntry));
    e->ln = ln;
    e->version = version;
    e->mstime = mstime;
    return e;
}

void persistingKeyEntryFree(void *privdata, void *val) {
    UNUSED(privdata);
    if (val != NULL) zfree(val);
}

dictType persistingKeysDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    persistingKeyEntryFree,    /* val destructor */
    NULL                       /* allow to expand */
};

persistingKeys *persistingKeysNew() {
    persistingKeys *keys = zmalloc(sizeof(persistingKeys));
    keys->list = listCreate();
    keys->map = dictCreate(&persistingKeysDictType,NULL);
    return keys;
}

void persistingKeysFree(persistingKeys *keys) {
   if (keys == NULL) return;
   dictRelease(keys->map);
   keys->map = NULL;
   listRelease(keys->list);
   keys->list = NULL;
   zfree(keys);
}

int persistingKeysPut(persistingKeys *keys, sds key, uint64_t version,
        mstime_t time) {
    dictEntry *de;
    listNode *ln;
	persistingKeyEntry *entry;

	if ((de = dictFind(keys->map,key))) {
		entry = dictGetVal(de);
		serverAssert(version > entry->version);
		entry->version = version;
        /* old time will be reserved */
        return 0;
    } else {
        sds dup = sdsdup(key);
        listAddNodeTail(keys->list,dup);
        ln = listLast(keys->list);
		entry = persistingKeyEntryNew(ln,version,time);
		dictAdd(keys->map,dup,entry);
        return 1;
    }
}

int persistingKeysLookup(persistingKeys *keys, sds key, uint64_t *version,
		mstime_t *time) {
	dictEntry *de;
	persistingKeyEntry *entry;

	if ((de = dictFind(keys->map,key))) {
		entry = dictGetVal(de);
		if (version) *version = entry->version;
		if (time) *time = entry->mstime;
		return 1;
	} else {
		return 0;
	}
}

int persistingKeysDelete(persistingKeys *keys, sds key) {
	dictEntry *de;
	persistingKeyEntry *entry;

	if ((de = dictUnlink(keys->map,key))) {
		entry = dictGetVal(de);
		listDelNode(keys->list,entry->ln);
		dictFreeUnlinkedEntry(keys->map,de);
		return 1;
	} else {
		return 0;
	}
}

size_t persistingKeysCount(persistingKeys *keys) {
    return listLength(keys->list);
}

size_t persistingKeysUsedMemory(persistingKeys *keys) {
    return persistingKeysCount(keys)*PERSISTING_KEY_ENTRY_MEM;
}

sds persistingKeysEarliest(persistingKeys *keys, uint64_t *version,
        mstime_t *mstime) {
    if (persistingKeysCount(keys) > 0) {
        dictEntry *de;
        persistingKeyEntry *entry;
        listNode *ln = listFirst(keys->list);
        sds key = ln->value;
        de = dictFind(keys->map,key);
        entry = dictGetVal(de);
        if (version) *version = entry->version;
        if (mstime) *mstime = entry->mstime;
        return key;
    } else {
        return NULL;
    }
}

void persistingKeysInitIterator(persistingKeysIter *iter,
        persistingKeys *keys) {
    iter->keys = keys;
    listRewind(keys->list,&iter->li);
}

void persistingKeysDeinitIterator(persistingKeysIter *iter) {
    UNUSED(iter);
}

sds persistingKeysIterNext(persistingKeysIter *iter, uint64_t *version,
		mstime_t *mstime) {
    sds key;
    listNode *ln;
	persistingKeyEntry *entry;

    if ((ln = listNext(&iter->li)) == NULL) return NULL;
    key = listNodeValue(ln);
    entry = dictFetchValue(iter->keys->map,key);
    if (version) *version = entry->version;
    if (mstime) *mstime = entry->mstime;
	return key;
}

static void swapPersistStatInit(swapPersistStat *stat) {
    stat->add_succ = 0;
    stat->add_ignored = 0;
    stat->submit_succ = 0;
    stat->submit_blocked = 0;
}

swapPersistCtx *swapPersistCtxNew() {
    int dbnum = server.dbnum;
    swapPersistCtx *ctx = zmalloc(sizeof(swapPersistCtx));
    ctx->keys = zmalloc(dbnum*sizeof(persistingKeys*));
    for (int i = 0; i < dbnum; i++) {
        ctx->keys[i] = persistingKeysNew();
    }
    ctx->version = SWAP_PERSIST_VERSION_INITIAL;
    swapPersistStatInit(&ctx->stat);
    return ctx;
}

void swapPersistCtxFree(swapPersistCtx *ctx) {
    int dbnum = server.dbnum;
    if (ctx == NULL) return;
    if (ctx->keys != NULL) {
        for (int i = 0; i < dbnum; i++) {
            if (ctx->keys[i] != NULL) {
                persistingKeysFree(ctx->keys[i]);
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
        persistingKeys *keys = ctx->keys[dbid];
        keys_count += persistingKeysCount(keys);
    }
    return keys_count;
}

inline size_t swapPersistCtxUsedMemory(swapPersistCtx *ctx) {
    if (ctx == NULL) return 0;
    size_t used_memory = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        persistingKeys *keys = ctx->keys[dbid];
        used_memory += persistingKeysUsedMemory(keys);
    }
    return used_memory;
}

inline mstime_t swapPersistCtxLag(swapPersistCtx *ctx) {
    mstime_t lag = 0, mstime;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        persistingKeys *keys = ctx->keys[dbid];
        if (persistingKeysEarliest(keys,NULL,&mstime)) {
            lag = MAX(lag,server.mstime-mstime);
        }
    }
    return lag;
}

void swapPersistCtxAddKey(swapPersistCtx *ctx, redisDb *db, robj *key) {
    persistingKeys *keys = ctx->keys[db->id];
    uint64_t persist_version = ctx->version++;
    if (persistingKeysPut(keys,key->ptr,persist_version,server.mstime))
        ctx->stat.add_succ++;
    else
        ctx->stat.add_ignored++;
}

static void tryPersistKey(swapPersistCtx *ctx, redisDb *db, robj *key,
        uint64_t persist_version) {
    client *evict_client = server.evict_clients[db->id];
    if (lockWouldBlock(server.swap_txid++, db, key)) {
        ctx->stat.submit_blocked++;
        return;
    } else {
        ctx->stat.submit_succ++;
        submitEvictClientRequest(evict_client,key,persist_version);
    }
}

static inline int reachedPersistInprogressLimit() {
    return server.swap_eviction_ctx->inprogress_count >=
        server.swap_evict_inprogress_limit;
}

void swapPersistCtxPersistKeys(swapPersistCtx *ctx) {
    uint64_t count = 0, persist_version;
	persistingKeysIter iter;
	redisDb *db;
	persistingKeys *keys;
	sds keyptr;
	mstime_t mstime;

    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        db = server.db+dbid;
        keys = ctx->keys[dbid];
        if (persistingKeysCount(keys) == 0) continue;
        persistingKeysInitIterator(&iter,keys);
        while ((keyptr = persistingKeysIterNext(&iter,&persist_version,&mstime)) &&
                !reachedPersistInprogressLimit() &&
                count++ < SWAP_PERSIST_MAX_KEYS_PER_LOOP) {
            robj *key = createStringObject(keyptr,sdslen(keyptr));
            tryPersistKey(ctx,db,key,persist_version);
            decrRefCount(key);
        }
        persistingKeysDeinitIterator(&iter);
    }
}

void swapPersistKeyRequestFinished(swapPersistCtx *ctx, int dbid, robj *key,
        uint64_t persist_version) {
    uint64_t current_version;
    mstime_t mstime;
    redisDb *db = server.db+dbid;
    persistingKeys *keys = ctx->keys[dbid];
    if (persistingKeysLookup(keys,key->ptr,&current_version,&mstime)) {
        serverAssert(persist_version <= current_version);
        if (current_version == persist_version) {
            robj *o = lookupKey(db,key,LOOKUP_NOTOUCH);
            if (o == NULL || !objectIsDirty(o)) {
                /* key (with persis_version) persist finished. */
                persistingKeysDelete(keys,key->ptr);
            } else {
                /* persist request will later started again */
            }
        } else {
            /* persist started by another attempt */
        }
    }
}

inline int swapRatelimitPersistNeeded(int policy, int *pms) {
    int pause_ms;
    static mstime_t prev_logtime;

    if (pms) *pms = 0;

    if (server.swap_persist_enabled) return 0;

    mstime_t lag = swapPersistCtxLag(server.swap_persist_ctx) / 1000;
    if (lag <= server.swap_ratelimit_persist_lag) return 0;

    if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
        pause_ms = (lag - server.swap_ratelimit_persist_lag)/server.swap_ratelimit_persist_pause_growth_rate;
        pause_ms = pause_ms < SWAP_RATELIMIT_PAUSE_MAX_MS ? pause_ms : SWAP_RATELIMIT_PAUSE_MAX_MS;
        if (pms) *pms = pause_ms;
    }

    if (server.mstime - prev_logtime > 1000) {
        char msg[32] = {0};
        if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
            snprintf(msg,sizeof(msg)-1,"pause (%d)ms", pause_ms);
        } else {
            snprintf(msg,sizeof(msg)-1,"reject");
        }

        serverLog(LL_NOTICE,"[ratelimit] persist lag(%lld) > (%d): %s", lag, server.swap_ratelimit_persist_lag, msg);
        prev_logtime = server.mstime;
    }

    return 1;
}

sds genSwapPersistInfoString(sds info) {
    if (server.swap_persist_enabled) {
        swapPersistStat *stat = &server.swap_persist_ctx->stat;
        size_t keys = swapPersistCtxKeysCount(server.swap_persist_ctx);
        size_t mem = swapPersistCtxUsedMemory(server.swap_persist_ctx);
        mstime_t lag = swapPersistCtxLag(server.swap_persist_ctx);
        info = sdscatprintf(info,
                "swap_persist_stat:add_succ=%lld,add_ignored=%lld,submit_succ=%lld,submit_blocked=%lld\r\n"
                "swap_persist_inprogress:count=%lu,memory=%lu,lag=%lld\r\n",
                stat->add_succ,stat->add_ignored,stat->submit_succ,stat->submit_blocked,keys,mem,lag);
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

            decrRefCount(keyobj);

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

    rocksdb_iter_destroy(meta_iter);
}

static int keyspaceIsEmpty() {
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        if (ctripDbSize(db)) return 0;
    }
    return 1;
}

void ctripLoadDataFromDisk(void) {
    if (server.swap_mode != SWAP_MODE_MEMORY &&
            server.swap_persist_enabled) {
        loadDataFromRocksdb();
    }

    setFilterState(FILTER_STATE_OPEN);

    if (keyspaceIsEmpty()) loadDataFromDisk();
}


#ifdef REDIS_TEST

int swapPersistTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

    redisDb *db;

    TEST("persist: init") {
        server.hz = 10;
        initTestRedisDb();
        db = server.db;
	}

    TEST("persist: persistingKeys") {
        mstime_t mstime;
        uint64_t version;
        persistingKeys *keys;
        persistingKeysIter iter;

        sds key, key1 = sdsnew("key1"), key2 = sdsnew("key2");

        keys = persistingKeysNew();
        test_assert(persistingKeysCount(keys) == 0);
        test_assert(persistingKeysUsedMemory(keys) == 0);
        test_assert(persistingKeysEarliest(keys,NULL,NULL) == 0);

        persistingKeysPut(keys,key1,1,1001);
        test_assert(persistingKeysCount(keys) == 1);
        test_assert(persistingKeysLookup(keys,key1,&version,&mstime));
        test_assert(version == 1 && mstime == 1001);
        test_assert(!persistingKeysLookup(keys,key2,&version,&mstime));
        key = persistingKeysEarliest(keys,&version,&mstime);
        test_assert(sdscmp(key, key1) == 0 && version == 1 && mstime == 1001);

        persistingKeysPut(keys,key2,2,1002);
        test_assert(persistingKeysCount(keys) == 2);
        test_assert(persistingKeysLookup(keys,key1,&version,&mstime));
        test_assert(version == 1 && mstime == 1001);
        test_assert(persistingKeysLookup(keys,key2,&version,&mstime));
        test_assert(version == 2 && mstime == 1002);
        key = persistingKeysEarliest(keys,&version,&mstime);
        test_assert(sdscmp(key, key1) == 0 && version == 1 && mstime == 1001);

        persistingKeysPut(keys,key1,3,1003);
        test_assert(persistingKeysCount(keys) == 2);
        test_assert(persistingKeysLookup(keys,key1,&version,&mstime));
        test_assert(version == 3 && mstime == 1001);
        test_assert(persistingKeysLookup(keys,key2,&version,&mstime));
        test_assert(version == 2 && mstime == 1002);
        key = persistingKeysEarliest(keys,&version,&mstime);
        test_assert(sdscmp(key, key1) == 0 && version == 3 && mstime == 1001);

        persistingKeysInitIterator(&iter,keys);
        key = persistingKeysIterNext(&iter, &version, &mstime);
        test_assert(sdscmp(key, key1) == 0 && version == 3 && mstime == 1001);
        key = persistingKeysIterNext(&iter, &version, &mstime);
        test_assert(sdscmp(key, key2) == 0 && version == 2 && mstime == 1002);
        key = persistingKeysIterNext(&iter, &version, &mstime);
        test_assert(key == NULL);
        persistingKeysDeinitIterator(&iter);

        persistingKeysDelete(keys,key1);
        test_assert(persistingKeysCount(keys) == 1);
        test_assert(!persistingKeysLookup(keys,key1,&version,&mstime));
        test_assert(persistingKeysLookup(keys,key2,&version,&mstime));
        test_assert(version == 2 && mstime == 1002);
        key = persistingKeysEarliest(keys,&version,&mstime);
        test_assert(sdscmp(key, key2) == 0 && version == 2 && mstime == 1002);

        persistingKeysDelete(keys,key2);
        test_assert(persistingKeysCount(keys) == 0);

        persistingKeysFree(keys);
        sdsfree(key1), sdsfree(key2);
	}

    TEST("persist: swapPersistCtx") {
        swapPersistCtx *ctx;
        robj *key1 = createStringObject("key1",4), *key2 = createStringObject("key2",4);

        ctx = swapPersistCtxNew();
        server.mstime = 1001;
        test_assert(swapPersistCtxUsedMemory(ctx) == 0);
        swapPersistCtxAddKey(ctx,db,key1);
        test_assert(swapPersistCtxKeysCount(ctx) == 1);
        test_assert(swapPersistCtxLag(ctx) == 0);
        server.mstime = 1002;
        test_assert(swapPersistCtxLag(ctx) == 1);
        swapPersistCtxAddKey(ctx,db,key2);
        test_assert(swapPersistCtxKeysCount(ctx) == 2);
        test_assert(swapPersistCtxUsedMemory(ctx) > 0);
        server.mstime = 1003;
        swapPersistCtxAddKey(ctx,db,key1);
        test_assert(swapPersistCtxKeysCount(ctx) == 2);
        test_assert(swapPersistCtxLag(ctx) == 2);

        decrRefCount(key1), decrRefCount(key2);
        swapPersistCtxFree(ctx);
    }

    return error;
}


#endif
