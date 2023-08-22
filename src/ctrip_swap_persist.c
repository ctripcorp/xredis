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

#define SWAP_PERSIST_STATE_TODO  0
#define SWAP_PERSIST_STATE_DOING 1

#define PERSISTING_KEY_ENTRY_MEM (DEFAULT_KEY_SIZE+sizeof(persistingKeyEntry)+\
        sizeof(dictEntry)+sizeof(listNode))

persistingKeyEntry *persistingKeyEntryNew(listNode *ln, uint64_t version,
        mstime_t mstime) {
    persistingKeyEntry *e = zmalloc(sizeof(persistingKeyEntry));
    e->ln = ln;
    e->version = version;
    e->mstime = mstime;
    e->state = SWAP_PERSIST_STATE_TODO;
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
    keys->todo = listCreate();
    keys->doing = listCreate();
    keys->map = dictCreate(&persistingKeysDictType,NULL);
    return keys;
}

void persistingKeysFree(persistingKeys *keys) {
   if (keys == NULL) return;
   dictRelease(keys->map);
   keys->map = NULL;
   listRelease(keys->todo);
   keys->todo = NULL;
   listRelease(keys->doing);
   keys->doing = NULL;
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
        listAddNodeTail(keys->todo,dup);
        ln = listLast(keys->todo);
		entry = persistingKeyEntryNew(ln,version,time);
		dictAdd(keys->map,dup,entry);
        return 1;
    }
}

persistingKeyEntry *persistingKeysLookup(persistingKeys *keys, sds key) {
	dictEntry *de;
	if ((de = dictFind(keys->map,key)))
		return dictGetVal(de);
	else
        return NULL;
}

int persistingKeysDelete(persistingKeys *keys, sds key) {
	dictEntry *de;
	persistingKeyEntry *entry;
    list *list;

	if ((de = dictUnlink(keys->map,key))) {
		entry = dictGetVal(de);
        list = entry->state == SWAP_PERSIST_STATE_TODO ? keys->todo : keys->doing;
		listDelNode(list,entry->ln);
		dictFreeUnlinkedEntry(keys->map,de);
		return 1;
	} else {
		return 0;
	}
}

void persistingKeyStart(persistingKeys *keys, persistingKeyEntry *entry) {
    serverAssert(entry->state == SWAP_PERSIST_STATE_TODO);
    listUnlink(keys->todo, entry->ln);
    listLinkTail(keys->doing, entry->ln);
    entry->state = SWAP_PERSIST_STATE_DOING;
}

void persistingKeyRewind(persistingKeys *keys, persistingKeyEntry *entry) {
    serverAssert(entry->state == SWAP_PERSIST_STATE_DOING);
    listUnlink(keys->doing, entry->ln);
    listLinkHead(keys->todo, entry->ln);
    entry->state = SWAP_PERSIST_STATE_TODO;
}

size_t persistingKeysCount(persistingKeys *keys) {
    return listLength(keys->todo) + listLength(keys->doing);
}

size_t persistingKeysUsedMemory(persistingKeys *keys) {
    return persistingKeysCount(keys)*PERSISTING_KEY_ENTRY_MEM;
}

static inline sds persistingKeysEarliest_(persistingKeys *keys, int state,
        persistingKeyEntry **entry) {
    list *list = state == SWAP_PERSIST_STATE_TODO ? keys->todo : keys->doing;
    if (listLength(list) == 0) {
        *entry = NULL;
        return NULL;
    } else {
        listNode *ln = listFirst(list);
        sds key = listNodeValue(ln);
        *entry = dictFetchValue(keys->map,key);
        return key;
    }
}

sds persistingKeysEarliest(persistingKeys *keys, persistingKeyEntry **entry) {
    sds k1, k2;
    persistingKeyEntry *e1, *e2;

    k1 = persistingKeysEarliest_(keys,SWAP_PERSIST_STATE_TODO,&e1);
    k2 = persistingKeysEarliest_(keys,SWAP_PERSIST_STATE_DOING,&e2);
    if (k1 && k2) {
        if (e1->mstime < e2->mstime) {
            *entry = e1;
            return k1;
        } else {
            *entry = e2;
            return k2;
        }
    } else if (k1) {
        *entry = e1;
        return k1;
    } else if (k2) {
        *entry = e2;
        return k2;
    } else {
        *entry = NULL;
        return NULL;
    }
}

void persistingKeysInitTodoIterator(persistingKeysTodoIter *iter,
        persistingKeys *keys) {
    iter->keys = keys;
    listRewind(keys->todo,&iter->li);
}

void persistingKeysDeinitIterator(persistingKeysTodoIter *iter) {
    UNUSED(iter);
}

sds persistingKeysTodoIterNext(persistingKeysTodoIter *iter, persistingKeyEntry **entry) {
    sds key;
    listNode *ln;

    if ((ln = listNext(&iter->li)) == NULL) return NULL;
    key = listNodeValue(ln);
    *entry = dictFetchValue(iter->keys->map,key);
    return key;
}


static void swapPersistStatInit(swapPersistStat *stat) {
    stat->add_succ = 0;
    stat->add_ignored = 0;
    stat->started = 0;
    stat->rewind_dirty = 0;
    stat->rewind_newer = 0;
    stat->ended = 0;
    stat->dont_keep = 0;
    stat->keep_data = 0;
}

swapPersistCtx *swapPersistCtxNew() {
    int dbnum = server.dbnum;
    swapPersistCtx *ctx = zmalloc(sizeof(swapPersistCtx));
    ctx->keys = zmalloc(dbnum*sizeof(persistingKeys*));
    for (int i = 0; i < dbnum; i++) {
        ctx->keys[i] = persistingKeysNew();
    }
    ctx->version = SWAP_PERSIST_VERSION_INITIAL;
    ctx->inprogress_count = 0;
    ctx->inprogress_limit = 0;
    ctx->keep = 1;
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
    mstime_t lag = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        persistingKeys *keys = ctx->keys[dbid];
        persistingKeyEntry *entry;
        if (persistingKeysEarliest(keys,&entry)) {
            lag = MAX(lag, server.mstime - entry->mstime);
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

static inline int reachedPersistInprogressLimit() {
    return server.swap_persist_ctx->inprogress_count >=
        server.swap_persist_ctx->inprogress_limit;
}

/* report memory used whether mem_used over maxmemory, see getMaxmemoryState
 * for more details */
size_t swapPersistGetUsedMemory() {
    size_t mem_used = ctrip_getUsedMemory();
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;
    return mem_used;
}

#define SWAP_PERSIST_CHECK_KEEP_INTERVAL 16
#define SWAP_PERSIST_KEEP_DATA_MEMORY_PERC 80
static inline void swapPersistCtxPersistKeysStart(swapPersistCtx *ctx) {
    static uint64_t counter;
    mstime_t lag_ms = swapPersistCtxLag(ctx);
    if (lag_ms <= server.swap_persist_lag_millis) {
        ctx->inprogress_limit = 0;
    } else {
        ctx->inprogress_limit = 1 + (lag_ms - server.swap_persist_lag_millis) / server.swap_persist_inprogress_growth_rate;
    }
    if (counter++ % SWAP_PERSIST_CHECK_KEEP_INTERVAL == 0) {
        size_t mem_used, mem_keep_data, actual_maxmemory;
        mem_used = swapPersistGetUsedMemory();
        actual_maxmemory = calculateNextMemoryLimit(mem_used,server.maxmemory_scale_from,server.maxmemory);
        mem_keep_data = actual_maxmemory*SWAP_PERSIST_KEEP_DATA_MEMORY_PERC/100;
        if (mem_used <= mem_keep_data || mem_keep_data == 0)
            ctx->keep = 1;
        else
            ctx->keep = 0;
    }
}

void swapPersistCtxPersistKeys(swapPersistCtx *ctx) {
    uint64_t count = 0;
	persistingKeysTodoIter iter;
	redisDb *db;
	persistingKeys *keys;
	sds keyptr;
    persistingKeyEntry *entry;

    swapPersistCtxPersistKeysStart(ctx);

    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        db = server.db+dbid;
        client *evict_client = server.evict_clients[db->id];
        keys = ctx->keys[dbid];
        if (persistingKeysCount(keys) == 0) continue;
        persistingKeysInitTodoIterator(&iter,keys);
        while ((keyptr = persistingKeysTodoIterNext(&iter,&entry)) &&
                !reachedPersistInprogressLimit() &&
                count++ < SWAP_PERSIST_MAX_KEYS_PER_LOOP) {
            robj *key = createStringObject(keyptr,sdslen(keyptr));

            ctx->stat.started++;
            persistingKeyStart(keys,entry);
            submitEvictClientRequest(evict_client,key,ctx->keep,entry->version);

            decrRefCount(key);
        }
        persistingKeysDeinitIterator(&iter);
    }
}

void swapPersistKeyRequestFinished(swapPersistCtx *ctx, int dbid, robj *key,
        uint64_t persist_version) {
    redisDb *db = server.db+dbid;
    persistingKeys *keys = ctx->keys[dbid];
    persistingKeyEntry *entry;

    entry = persistingKeysLookup(keys,key->ptr);
    serverAssert(entry && entry->state == SWAP_PERSIST_STATE_DOING);
    serverAssert(persist_version <= entry->version);

    if (entry->version == persist_version) {
        robj *o = lookupKey(db,key,LOOKUP_NOTOUCH);
        if (o == NULL || !objectIsDirty(o)) {
            ctx->stat.ended++;
            persistingKeysDelete(keys,key->ptr);
        } else {
            /* key still dirty: persist again. */
            ctx->stat.rewind_dirty++;
            persistingKeyRewind(keys, entry);
        }
    } else {
        /* newer persist registered: persist again*/
        ctx->stat.rewind_newer++;
        persistingKeyRewind(keys, entry);
    }
}

inline int swapRatelimitPersistNeeded(swapRatelimitCtx *rlctx, int policy, int *pms) {
    int pause_ms;
    static mstime_t prev_logtime;

    if (pms) *pms = 0;

    if (!server.swap_persist_enabled) return 0;

    mstime_t lag = swapPersistCtxLag(server.swap_persist_ctx) / 1000;
    if (lag <= server.swap_ratelimit_persist_lag) return 0;

    if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
        if (!rlctx->is_write_command) return 0;
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
        long long count = server.swap_persist_ctx->inprogress_count;
        size_t keys = swapPersistCtxKeysCount(server.swap_persist_ctx);
        size_t mem = swapPersistCtxUsedMemory(server.swap_persist_ctx);
        mstime_t lag = swapPersistCtxLag(server.swap_persist_ctx);
        info = sdscatprintf(info,
                "swap_persist_stat:add_succ=%lld,add_ignored=%lld,started=%lld,rewind_dirty=%lld,rewind_newer=%lld,ended=%lld,keep_data=%lld,dont_keep=%lld\r\n"
                "swap_persist_inprogress:count=%lld,keys=%lu,memory=%lu,lag_millis=%lld\r\n",
                stat->add_succ,stat->add_ignored,stat->started,stat->rewind_dirty,stat->rewind_newer,stat->ended,stat->keep_data,stat->dont_keep,
                count,keys,mem,lag);
    }
    return info;
}

#define INIT_FIX_OK 0
#define INIT_FIX_ERR -1
#define INIT_FIX_SKIP -2

struct listMeta *listMetaCreate();

int keyLoadFixDataInit(keyLoadFixData *fix, redisDb *db, decodedResult *dr) {
    serverAssert(db->id == dr->dbid);

    if (dr->cf != META_CF) {
        /* skip orphan (sub)data keys: note that meta key is prefix of data
         * subkey, so rocksIter always start init with meta key, except for
         * orphan (sub)data key. */
        return INIT_FIX_SKIP;
    }

    objectMeta *cold_meta, *rebuild_meta;
    decodedMeta *dm = (decodedMeta*)dr;

    server.swap_persist_load_fix_version = MAX(dm->version,
            server.swap_persist_load_fix_version);

    size_t extlen = dm->extend ? sdslen(dm->extend) : 0;
    if (buildObjectMeta(dm->object_type,dm->version,dm->extend,
                extlen, &cold_meta)) {
        return INIT_FIX_ERR;
    }


    switch (dm->object_type) {
    case OBJ_STRING:
        rebuild_meta = NULL;
        break;
    case OBJ_HASH:
    case OBJ_SET:
    case OBJ_ZSET:
        rebuild_meta = createLenObjectMeta(dm->object_type,dm->version,0);
        break;
    case OBJ_LIST:
        rebuild_meta = createListObjectMeta(dm->version,listMetaCreate());
        break;
    default:
        rebuild_meta = NULL;
        break;
    }

    fix->db = db;
    fix->object_type = dm->object_type;
    fix->key = createStringObject(dr->key, sdslen(dr->key));
    fix->expire = dm->expire;
    fix->version = dm->version;
    fix->cold_meta = cold_meta;
    fix->rebuild_meta = rebuild_meta;
    fix->feed_err = 0;
    fix->feed_ok = 0;
    fix->errstr = NULL;

    return INIT_FIX_OK;
}

void keyLoadFixDataDeinit(keyLoadFixData *fix) {
    if (fix->key) {
        decrRefCount(fix->key);
        fix->key = NULL;
    }

    if (fix->cold_meta) {
        freeObjectMeta(fix->cold_meta);
        fix->cold_meta = NULL;
    }

    if (fix->rebuild_meta) {
        freeObjectMeta(fix->rebuild_meta);
        fix->rebuild_meta = NULL;
    }

    if (fix->errstr) {
        sdsfree(fix->errstr);
        fix->errstr = NULL;
    }
}

static inline int keyLoadFixStart(struct keyLoadFixData *fix) {
    UNUSED(fix);
    return 0;
}

#define PROGRESS_INTERVAL (2*1024*1024)
static void persistLoadFixProgressCallback(decodedData *d) {
    static long long prev_fixed_bytes, fixed_bytes;
    long long progress_bytes = 48; /*overhead*/;

    if (d && d->key) progress_bytes += sdslen(d->key);
    if (d && d->rdbraw) progress_bytes += sdslen(d->rdbraw);
    fixed_bytes += progress_bytes;

    if (fixed_bytes - prev_fixed_bytes > PROGRESS_INTERVAL) {
        prev_fixed_bytes = fixed_bytes;
        loadingProgress(fixed_bytes);
        processEventsWhileBlocked();
    }
}

static inline void keyLoadFixFeed(struct keyLoadFixData *fix, decodedData *d) {
    /* skip obselete subkeys, note that this rule also works for string,
     * subkey version never equal string version (i.e. zero). */
    if (fix->version != d->version) return;

    if (fix->rebuild_meta &&
            objectMetaRebuildFeed(fix->rebuild_meta,d->version,d->subkey,
                sdslen(d->subkey))) {
        fix->feed_err++;
    } else {
        fix->feed_ok++;
    }
}

#define FIX_NONE 0
#define FIX_UPDATE -1
#define FIX_DELETE -2

static int keyLoadFixAna(struct keyLoadFixData *fix) {
    objectMeta *rebuild_meta = fix->rebuild_meta,
               *cold_meta = fix->cold_meta;
    if (fix->feed_err > 0 || fix->feed_ok <= 0)
        return FIX_DELETE;

    if (rebuild_meta == NULL && cold_meta == NULL) {
        if (fix->feed_ok != 1)
            return FIX_DELETE;
        else
            return FIX_NONE;
    }

    if (rebuild_meta == NULL || cold_meta == NULL)
        return FIX_DELETE;

    if (rebuild_meta->object_type != cold_meta->object_type ||
            rebuild_meta->version != cold_meta->version)
        return FIX_DELETE;

    if (objectMetaEqual(rebuild_meta, cold_meta)) {
        return FIX_NONE;
    } else {
#ifdef SWAP_DEBUG
        sds cold_meta_dump = dumpObjectMeta(cold_meta);
        sds rebuild_meta_dump = dumpObjectMeta(cold_meta);
        serverLog(LL_WARNING, "update meta: %s => %s",
                cold_meta_dump, rebuild_meta_dump);
        sdsfree(cold_meta_dump);
        sdsfree(rebuild_meta_dump);
#endif
        return FIX_UPDATE;
    }
}

static inline int keyLoadFixEnd(struct keyLoadFixData *fix,
        loadFixStats *fix_stats) {
    sds extend = NULL;
    int fix_result = 0;
    RIO _rio = {0}, *rio = &_rio;
    int *cfs;
    sds *rawkeys, *rawvals;

    fix_result = keyLoadFixAna(fix);

#ifdef SWAP_DEBUG
    serverLog(LL_WARNING,"keyLoadFixEnd: %s => %d", (sds)fix->key->ptr, fix_result);
#endif

    switch (fix_result) {
    case FIX_NONE:
        fix_stats->fix_none++;
        fix->db->cold_keys++;
        coldFilterAddKey(fix->db->cold_filter,fix->key->ptr);
        break;
    case FIX_UPDATE:
        cfs = zmalloc(sizeof(int));
        rawkeys = zmalloc(sizeof(sds)), rawvals = zmalloc(sizeof(sds));
        cfs[0] = META_CF;
        rawkeys[0] = rocksEncodeMetaKey(fix->db,fix->key->ptr);
        extend = objectMetaEncode(fix->rebuild_meta);
        rawvals[0] = rocksEncodeMetaVal(fix->object_type,fix->expire,
                fix->version,extend);
        RIOInitPut(rio,1,cfs,rawkeys,rawvals);
        RIODo(rio);
        if (!RIOGetError(rio)) {
            fix_stats->fix_update++;
            fix->db->cold_keys++;
            coldFilterAddKey(fix->db->cold_filter,fix->key->ptr);
        } else  {
            fix_stats->fix_err++;
            if (rio->err) fix->errstr = sdsdup(rio->err);
        }
        RIODeinit(rio);
        sdsfree(extend);
        break;
    case FIX_DELETE:
        cfs = zmalloc(sizeof(int));
        rawkeys = zmalloc(sizeof(sds));
        cfs[0] = META_CF;
        rawkeys[0] = rocksEncodeMetaKey(fix->db,fix->key->ptr);
        RIOInitDel(rio,1,cfs,rawkeys);
        RIODo(rio);
        if (!RIOGetError(rio)) {
            fix_stats->fix_delete++;
        } else {
            fix_stats->fix_err++;
            if (rio->err) fix->errstr = sdsdup(rio->err);
        }
        RIODeinit(rio);
        break;
    default:
        serverPanic("unpexected fix result");
        break;
    }

    return C_OK;
}

sds loadFixStatsDump(loadFixStats *stats) {
    return sdscatprintf(sdsempty(),
            "fix.init.ok=%lld,"
            "fix.init.skip=%lld,"
            "fix.init.err=%lld,"
            "fix.do.none=%lld,"
            "fix.do.update=%lld,"
            "fix.do.delete=%lld,"
            "fix.do.err=%lld",
            stats->init_ok,
            stats->init_skip,
            stats->init_err,
            stats->fix_none,
            stats->fix_update,
            stats->fix_delete,
            stats->fix_err);
}

/* scan and fix whole persisted data. */
int persistLoadFixDb(redisDb *db) {
    rocksIter *it = NULL;
    sds errstr = NULL;
    rocksIterDecodeStats _iter_stats = {0}, *iter_stats = &_iter_stats;
    loadFixStats _fix_stats = {0}, *fix_stats = &_fix_stats;
    decodedResult  _cur, *cur = &_cur, _next, *next = &_next;
    decodedResultInit(cur);
    decodedResultInit(next);
    int iter_valid; /* true if current iter value is valid. */

    if (!(it = rocksCreateIter(server.rocks,db))) {
        serverLog(LL_WARNING, "Create rocks iterator failed.");
        return C_ERR;
    }

    iter_valid = rocksIterSeekToFirst(it);

    while (1) {
        int init_result, decode_result;
        keyLoadFixData _fix, *fix = &_fix;
        serverAssert(next->key == NULL);

        if (cur->key == NULL) {
            if (!iter_valid) break;

            decode_result = rocksIterDecode(it,cur,iter_stats);
            iter_valid = rocksIterNext(it);

            if (decode_result) continue;

            serverAssert(cur->key != NULL);
        }

        init_result = keyLoadFixDataInit(fix,db,cur);
        if (init_result == INIT_FIX_SKIP) {
            fix_stats->init_skip++;
            decodedResultDeinit(cur);
            continue;
        } else if (init_result == INIT_FIX_ERR) {
            if (fix_stats->init_err++ < 10) {
                sds repr = sdscatrepr(sdsempty(),cur->key,sdslen(cur->key));
                serverLog(LL_WARNING, "Init fix key failed: %s", repr);
                sdsfree(repr);
            }
            decodedResultDeinit(cur);
            continue;
        } else {
            fix_stats->init_ok++;
        }

        if (keyLoadFixStart(fix) == -1) {
            errstr = sdscatfmt(sdsempty(),"Fix key(%S) start failed: %s",
                    cur->key, strerror(errno));
            keyLoadFixDataDeinit(fix);
            decodedResultDeinit(cur);
            goto err; /* IO error, can't recover. */
        }

        while (1) {
            int key_switch;

            /* Iterate untill next valid rawkey found or eof. */
            while (1) {
                if (!iter_valid) break; /* eof */

                decode_result = rocksIterDecode(it,next,iter_stats);
                iter_valid = rocksIterNext(it);

                if (decode_result) {
                    continue;
                } else { /* next found */
                    serverAssert(next->key != NULL);
                    break;
                }
            }

            /* Can't find next valid rawkey, break to finish saving cur key.*/
            if (next->key == NULL) {
                decodedResultDeinit(cur);
                break;
            }

            serverAssert(cur->key && next->key);
            key_switch = sdslen(cur->key) != sdslen(next->key) ||
                    sdscmp(cur->key,next->key);

            decodedResultDeinit(cur);
            _cur = _next;
            decodedResultInit(next);

            /* key switched, finish current & start another. */
            if (key_switch) break;

            /* key not switched, continue scan current key. */
            keyLoadFixFeed(fix,(decodedData*)cur);

            persistLoadFixProgressCallback((decodedData*)cur);
        }

        /* call save_end if save_start called, no matter error or not. */
        if (keyLoadFixEnd(fix, fix_stats) != C_OK) {
            errstr = sdsdup(fix->errstr);
            keyLoadFixDataDeinit(fix);
            goto err;
        }

        keyLoadFixDataDeinit(fix);
    }

    if (db->cold_keys) {
        sds iter_stats_dump = rocksIterDecodeStatsDump(iter_stats);
        sds fix_stats_dump = loadFixStatsDump(fix_stats);
        serverLog(LL_NOTICE,
                "Fix persist keys finished: db=(%d), iter=(%s), fix=(%s)",
                db->id,iter_stats_dump,fix_stats_dump);
        sdsfree(iter_stats_dump);
        sdsfree(fix_stats_dump);
    }

    if (it) rocksReleaseIter(it);

    return C_OK;

err:
    serverLog(LL_WARNING, "Fix persist data rdb failed: %s", errstr);
    if (it) rocksReleaseIter(it);
    if (errstr) sdsfree(errstr);
    return C_ERR;
}

void startPersistLoadFix() {
    server.loading = 1;
    server.loading_start_time = time(NULL);
    blockingOperationStarts();
    server.swap_persist_load_fix_version = 0;
    serverLog(LL_NOTICE, "persist load fix start");
}

void stopPersistLoadFix() {
    server.loading = 0;
    blockingOperationEnds();
    server.swap_persist_load_fix_version++;
    swapSetVersion(server.swap_persist_load_fix_version);
    serverLog(LL_NOTICE, "persist load fix end (version start from %ld).",
            server.swap_persist_load_fix_version);
}

/* scan meta cf to rebuild cold_keys/cold_filter & fix keys */
void loadDataFromRocksdb() {
    startPersistLoadFix();
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        long long start_time = ustime();
        persistLoadFixDb(db);
        if (db->cold_keys) {
            double elapsed = (double)(ustime() - start_time)/1000000;
            serverLog(LL_NOTICE,
                    "[persist] db-%d loaded %lld keys from rocksdb in %.2f seconds.",
                    i,db->cold_keys,elapsed);
        }
    }
    stopPersistLoadFix();
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

#define PUT_META(redisdb,object_type,key_,version,expire,extend) do {\
    char *err = NULL;                                       \
    sds keysds = rocksEncodeMetaKey(redisdb, key_);         \
    sds valsds = rocksEncodeMetaVal(object_type,expire,version,extend);\
    rocksdb_put_cf(server.rocks->db,server.rocks->wopts,    \
            server.rocks->cf_handles[META_CF],              \
            keysds,sdslen(keysds),valsds,sdslen(valsds),&err);\
    serverAssert(err == NULL);                              \
    sdsfree(keysds), sdsfree(valsds);                       \
} while (0)

#define PUT_DATA(redisdb,key_,version,subkey_,val_) do {    \
    char *err = NULL;                                       \
    sds keysds = rocksEncodeDataKey(redisdb,key_,version,subkey_);\
    sds valsds = rocksEncodeValRdb(val_);                   \
    rocksdb_put_cf(server.rocks->db,server.rocks->wopts,    \
            server.rocks->cf_handles[DATA_CF],              \
            keysds,sdslen(keysds),valsds,sdslen(valsds),&err);\
    serverAssert(err == NULL);                              \
    sdsfree(keysds), sdsfree(valsds);                       \
} while (0)

#define CHECK_NO_META(redisdb,key) do {                     \
    char *err = NULL;                                       \
    size_t meta_rvlen;                                      \
    sds keysds = rocksEncodeMetaKey(redisdb, key);          \
    char *meta_rawval =                                     \
    rocksdb_get_cf(server.rocks->db,server.rocks->ropts,    \
            server.rocks->cf_handles[META_CF],              \
            keysds,sdslen(keysds),&meta_rvlen,&err);        \
    test_assert(meta_rawval == NULL);                       \
    test_assert(err == NULL);                               \
    sdsfree(keysds);                                        \
} while (0)

#define CHECK_META(redisdb,key,   object_type,version,expire,extend) do { \
    char *err = NULL;                                       \
    size_t meta_rvlen;                                      \
    sds keysds = rocksEncodeMetaKey(redisdb, key);          \
    char *meta_rawval =                                     \
    rocksdb_get_cf(server.rocks->db,server.rocks->ropts,    \
            server.rocks->cf_handles[META_CF],              \
            keysds,sdslen(keysds),&meta_rvlen,&err);        \
    test_assert(err == NULL);                               \
    test_assert(meta_rawval != NULL);                       \
    int _obj_type;                                          \
    long long _expire;                                      \
    uint64_t _version;                                      \
    const char *_extend;                                    \
    size_t _extlen;                                         \
    rocksDecodeMetaVal(meta_rawval,meta_rvlen,&_obj_type,   \
            &_expire,&_version,&_extend,&_extlen);          \
    test_assert(object_type == _obj_type);                  \
    test_assert(version == _version);                       \
    if (extend != NULL) {                                   \
        test_assert(sdslen(extend) == _extlen);             \
        test_assert(!memcmp(extend,_extend,_extlen));       \
    } else {                                                \
        test_assert(_extend == NULL);                       \
    }                                                       \
    sdsfree(keysds);                                        \
    zlibc_free(meta_rawval);                                \
} while (0)

int ROCKS_FLUSHDB(int dbid) {
    int retval = 0, i;
    sds startkey = NULL, endkey = NULL;
    char *err = NULL;

    startkey = rocksEncodeDbRangeStartKey(dbid);
    endkey = rocksEncodeDbRangeEndKey(dbid);

    for (i = 0; i < CF_COUNT; i++) {
        rocksdb_delete_range_cf(server.rocks->db,server.rocks->wopts,
                server.rocks->cf_handles[i],startkey,sdslen(startkey),
                endkey,sdslen(endkey), &err);
        if (err != NULL) {
            retval = -1;
            serverLog(LL_WARNING,
                    "[ROCKS] flush db(%d) by delete_range fail:%s",dbid,err);
        }
    }
    if (startkey) sdsfree(startkey);
    if (endkey) sdsfree(endkey);

    return retval;
}

static inline sds listEncodeRidx(long ridx) {
    ridx = htonu64(ridx);
    return sdsnewlen(&ridx,sizeof(ridx));
}

sds encodeListMeta(struct listMeta *lm);
int listMetaAppendSegment(struct listMeta *list_meta, int type, long index, long len);
void listMetaFree(struct listMeta *list_meta);

void initServerConfig(void);
int swapPersistTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

    redisDb *db;

    TEST("persist: init") {
        server.hz = 10;
        initTestRedisDb();
        monotonicInit();
        initServerConfig();
        if (!server.rocks) rocksInit();
        db = server.db;
	}

    TEST("persist: persistingKeys") {
        persistingKeys *keys;
        persistingKeysTodoIter iter;
        persistingKeyEntry *entry;

        sds key, key1 = sdsnew("key1"), key2 = sdsnew("key2");

        keys = persistingKeysNew();
        test_assert(persistingKeysCount(keys) == 0);
        test_assert(persistingKeysUsedMemory(keys) == 0);
        test_assert(persistingKeysEarliest(keys,&entry) == NULL);

        persistingKeysPut(keys,key1,1,1001);
        test_assert(persistingKeysCount(keys) == 1);
        test_assert((entry = persistingKeysLookup(keys,key1)));
        test_assert(entry->version == 1 && entry->mstime == 1001);
        test_assert(!persistingKeysLookup(keys,key2));
        key = persistingKeysEarliest(keys,&entry);
        test_assert(sdscmp(key, key1) == 0 && entry->version == 1 && entry->mstime == 1001);

        persistingKeysPut(keys,key2,2,1002);
        test_assert(persistingKeysCount(keys) == 2);
        test_assert((entry = persistingKeysLookup(keys,key1)));
        test_assert(entry->version == 1 && entry->mstime == 1001);
        test_assert((entry = persistingKeysLookup(keys,key2)));
        test_assert(entry->version == 2 && entry->mstime == 1002);
        key = persistingKeysEarliest(keys,&entry);
        test_assert(sdscmp(key, key1) == 0 && entry->version == 1 && entry->mstime == 1001);

        persistingKeysPut(keys,key1,3,1003);
        test_assert(persistingKeysCount(keys) == 2);
        test_assert((entry = persistingKeysLookup(keys,key1)));
        test_assert(entry->version == 3 && entry->mstime == 1001);
        test_assert((entry = persistingKeysLookup(keys,key2)));
        test_assert(entry->version == 2 && entry->mstime == 1002);
        key = persistingKeysEarliest(keys,&entry);
        test_assert(sdscmp(key, key1) == 0 && entry->version == 3 && entry->mstime == 1001);

        persistingKeysInitTodoIterator(&iter,keys);
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(sdscmp(key, key1) == 0 && entry->version == 3 && entry->mstime == 1001);
        persistingKeyStart(keys, entry);
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(sdscmp(key, key2) == 0 && entry->version == 2 && entry->mstime == 1002);
        persistingKeyStart(keys, entry);
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(key == NULL);
        persistingKeysDeinitIterator(&iter);

        /* oerwrite doing key */
        persistingKeysPut(keys, key1, 4, 1004);
        entry = persistingKeysLookup(keys, key1);
        test_assert(entry->state == SWAP_PERSIST_STATE_DOING && entry->version == 4 && entry->mstime == 1001);

        /* lookup doing key */
        entry = persistingKeysLookup(keys, key2);
        test_assert(entry->state == SWAP_PERSIST_STATE_DOING && entry->version == 2 && entry->mstime == 1002);

        /* iter skips doing key */
        persistingKeysInitTodoIterator(&iter,keys);
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(key == NULL);
        persistingKeysDeinitIterator(&iter);

        /* rewind doing key */
        entry = persistingKeysLookup(keys, key1);
        persistingKeyRewind(keys, entry);
        entry = persistingKeysLookup(keys, key2);
        persistingKeyRewind(keys, entry);

        /* iter and start again */
        persistingKeysInitTodoIterator(&iter,keys);
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(sdscmp(key, key2) == 0 && entry->version == 2 && entry->mstime == 1002);
        persistingKeyStart(keys, entry); /* simulate rewind dirty */
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(sdscmp(key, key1) == 0 && entry->version == 4 && entry->mstime == 1001);
        persistingKeyStart(keys, entry); /* simulate rewind newer */
        key = persistingKeysTodoIterNext(&iter, &entry);
        test_assert(key == NULL);
        persistingKeysDeinitIterator(&iter);

        persistingKeysDelete(keys,key1);
        test_assert(persistingKeysCount(keys) == 1);
        test_assert(!persistingKeysLookup(keys,key1));
        test_assert((entry = persistingKeysLookup(keys,key2)));
        test_assert(entry->version == 2 && entry->mstime == 1002);
        key = persistingKeysEarliest(keys,&entry);
        test_assert(sdscmp(key, key2) == 0 && entry->version == 2 && entry->mstime == 1002);

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

    ROCKS_FLUSHDB(db->id);

    TEST("persist: load fix (string)") {
        sds k1 = sdsnew("k1"), k2 = sdsnew("k2"), k3 = sdsnew("k3"), k4 = sdsnew("k4");
        robj *v1 = createStringObject("v1",2), *v2 = createStringObject("v2",2),
             *v3 = createStringObject("v3",2), *v4 = createStringObject("v4",2);
        sds s3 = sdsnew("s3"), s4 = sdsnew("s4");
        uint64_t version = 2;

        /* meta => deleted */
        PUT_META(db,OBJ_STRING,k1,0,0,NULL);
        /* meta + data => none */
        PUT_META(db,OBJ_STRING,k2,0,0,NULL);
        PUT_DATA(db,k2,0,NULL,v2);
        PUT_DATA(db,k2,0,NULL,v1);
        /* meta + subkey => deleted */
        PUT_META(db,OBJ_STRING,k3,0,0,NULL);
        PUT_DATA(db,k3,version,s3,v3);
        /* meta + data + subkey => none */
        PUT_META(db,OBJ_STRING,k4,0,0,NULL);
        PUT_DATA(db,k4,0,NULL,v4);
        PUT_DATA(db,k4,version,s4,v4);
        PUT_DATA(db,k4,version,s3,v3);

        db->cold_keys = 0;
        persistLoadFixDb(db);

        test_assert(db->cold_keys == 2);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnonnull"
        CHECK_NO_META(db,k1);
        CHECK_META(db,k2, OBJ_STRING,0,0,NULL);
        CHECK_NO_META(db,k3);
        CHECK_META(db,k4, OBJ_STRING,0,0,NULL);
#pragma GCC diagnostic pop

        sdsfree(k1), sdsfree(k2), sdsfree(k3), sdsfree(k4);
        decrRefCount(v1), decrRefCount(v2), decrRefCount(v3), decrRefCount(v4);
        sdsfree(s3), sdsfree(s4);

        ROCKS_FLUSHDB(db->id);
    }

    TEST("persist: load fix (hash/set/zset)") {
        sds k1 = sdsnew("k1"), k2 = sdsnew("k2"), k3 = sdsnew("k3"), k4 = sdsnew("k4");
        robj *v1 = createStringObject("v1",2), *v2 = createStringObject("v2",2),
             *v3 = createStringObject("v3",2), *v4 = createStringObject("v4",2);
        sds s1 = sdsnew("s1"), s2 = sdsnew("s2"), s3 = sdsnew("s3"), s4 = sdsnew("s4");
        sds ext_one = rocksEncodeObjectMetaLen(1), ext_two = rocksEncodeObjectMetaLen(2),
            ext_three = rocksEncodeObjectMetaLen(3);
        uint64_t version = 2, oversion = 1;

        /* meta => deleted */
        PUT_META(db,OBJ_HASH,k1,version,0,ext_one);
        /* meta + strig + subkey... => none */
        PUT_META(db,OBJ_HASH,k2,version,0,ext_two);
        PUT_DATA(db,k2,version,s1,v1);
        PUT_DATA(db,k2,version,s2,v2);
        PUT_DATA(db,k2,oversion,s1,v1);
        PUT_DATA(db,k2,oversion,s2,v2);
        PUT_DATA(db,k2,0,NULL,v2);
        /* meta + string + obselete => deleted */
        PUT_META(db,OBJ_HASH,k3,version,0,ext_one);
        PUT_DATA(db,k3,0,NULL,v3);
        PUT_DATA(db,k3,oversion,s1,v1);
        /* meta + subkey(len not match) => update */
        PUT_META(db,OBJ_HASH,k4,version,0,ext_two);
        PUT_DATA(db,k4,0,NULL,v1);
        PUT_DATA(db,k4,version,s1,v1);
        PUT_DATA(db,k4,version,s2,v2);
        PUT_DATA(db,k4,version,s3,v3);
        PUT_DATA(db,k4,oversion,s1,v1);

        db->cold_keys = 0;
        persistLoadFixDb(db);

        test_assert(db->cold_keys == 2);

        CHECK_NO_META(db,k1);
        CHECK_META(db,k2, OBJ_HASH,version,0,ext_two);
        CHECK_NO_META(db,k3);
        CHECK_META(db,k4, OBJ_HASH,version,0,ext_three);

        sdsfree(k1), sdsfree(k2), sdsfree(k3), sdsfree(k4);
        decrRefCount(v1), decrRefCount(v2), decrRefCount(v3), decrRefCount(v4);
        sdsfree(s1), sdsfree(s2), sdsfree(s3), sdsfree(s4);
        sdsfree(ext_one), sdsfree(ext_two), sdsfree(ext_three);

        ROCKS_FLUSHDB(db->id);
    }

    TEST("persist: load fix (list)") {
        sds k1 = sdsnew("k1"), k2 = sdsnew("k2"), k3 = sdsnew("k3"), k4 = sdsnew("k4"), k5 = sdsnew("k5");
        robj *v1 = createStringObject("v1",2), *v2 = createStringObject("v2",2),
             *v3 = createStringObject("v3",2), *v4 = createStringObject("v4",2);
        sds s1 = listEncodeRidx(1), s2 = listEncodeRidx(2), s3 = listEncodeRidx(3), s4 = listEncodeRidx(4);
        sds f1 = sdsnew("f1"), f2 = sdsnew("f2");
        struct listMeta *lm = listMetaCreate();
        listMetaAppendSegment(lm,SEGMENT_TYPE_COLD,1,2);
        sds ext2 = encodeListMeta(lm);
        listMetaAppendSegment(lm,SEGMENT_TYPE_COLD,3,1);
        sds ext3 = encodeListMeta(lm);

        uint64_t version = 2, oversion = 1;

        /* meta => deleted */
        PUT_META(db,OBJ_LIST,k1,version,0,ext3);
        /* meta + strig + subkey... => none */
        PUT_META(db,OBJ_LIST,k2,version,0,ext3);
        PUT_DATA(db,k2,version,s1,v1);
        PUT_DATA(db,k2,version,s2,v2);
        PUT_DATA(db,k2,version,s3,v3);
        PUT_DATA(db,k2,oversion,f1,v1);
        PUT_DATA(db,k2,oversion,f2,v2);
        PUT_DATA(db,k2,0,NULL,v2);
        /* meta + string + obselete => deleted */
        PUT_META(db,OBJ_LIST,k3,version,0,ext3);
        PUT_DATA(db,k3,0,NULL,v3);
        PUT_DATA(db,k3,oversion,s1,v1);
        /* meta + subkey(len not match) => update */
        PUT_META(db,OBJ_LIST,k4,version,0,ext3);
        PUT_DATA(db,k4,0,NULL,v1);
        PUT_DATA(db,k4,oversion,s1,v1);
        PUT_DATA(db,k4,version,s1,v1);
        PUT_DATA(db,k4,version,s2,v2);
        PUT_DATA(db,k4,oversion,f2,v2);
        /* meta + subkey(invalid) => deleted */
        PUT_META(db,OBJ_LIST,k5,version,0,ext2);
        PUT_DATA(db,k5,version,s1,v2);
        PUT_DATA(db,k5,version,s2,v2);
        PUT_DATA(db,k5,version,f1,v1);

        db->cold_keys = 0;
        persistLoadFixDb(db);

        test_assert(db->cold_keys == 2);

        CHECK_NO_META(db,k1);
        CHECK_META(db,k2, OBJ_LIST,version,0,ext3);
        CHECK_NO_META(db,k3);
        CHECK_META(db,k4, OBJ_LIST,version,0,ext2);
        CHECK_NO_META(db,k5);

        sdsfree(k1), sdsfree(k2), sdsfree(k3), sdsfree(k4), sdsfree(k5);
        decrRefCount(v1), decrRefCount(v2), decrRefCount(v3), decrRefCount(v4);
        sdsfree(s1), sdsfree(s2), sdsfree(s3), sdsfree(s4);
        sdsfree(f1), sdsfree(f2);
        listMetaFree(lm);
        sdsfree(ext2), sdsfree(ext3);

        ROCKS_FLUSHDB(db->id);
    }

    return error;
}


#endif
