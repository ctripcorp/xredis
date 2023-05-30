/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
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

void evictClientKeyRequestFinished(client *c, swapCtx *ctx) {
    UNUSED(ctx);
    robj *key = ctx->key_request->key;
    if (ctx->errcode) clientSwapError(c,ctx->errcode);
    incrRefCount(key);
    c->keyrequests_count--;
    serverAssert(c->client_hold_mode == CLIENT_HOLD_MODE_EVICT);
    clientReleaseLocks(c,ctx);
    decrRefCount(key);

    server.swap_eviction_ctx->inprogress_count--;
}

int submitEvictClientRequest(client *c, robj *key) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequestsPrepareResult(&result,1);
    incrRefCount(key);
    getKeyRequestsAppendSubkeyResult(&result,REQUEST_LEVEL_KEY,key,0,NULL,
            c->cmd->intention,c->cmd->intention_flags,c->cmd->flags,c->db->id);
    c->keyrequests_count++;
    submitDeferredClientKeyRequests(c,&result,evictClientKeyRequestFinished,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);

    server.swap_eviction_ctx->inprogress_count++;
    return 1;
}

int tryEvictKey(redisDb *db, robj *key, int *evict_result) {
    int dirty, old_keyrequests_count;
    robj *o;
    client *evict_client = server.evict_clients[db->id];

    if (lockWouldBlock(server.swap_txid++, db, key)) {
        if (evict_result) *evict_result = EVICT_FAIL_SWAPPING;
        return 0;
    }

    if ((o = lookupKey(db, key, LOOKUP_NOTOUCH)) == NULL) {
        if (evict_result) *evict_result = EVICT_FAIL_ABSENT;
        return 0;
    }

    dirty = o->dirty;
    old_keyrequests_count = evict_client->keyrequests_count;
    submitEvictClientRequest(evict_client,key);
    /* Evit request finished right away, no swap triggered. */
    if (evict_client->keyrequests_count == old_keyrequests_count) {
        if (dirty) {
            if (evict_result) *evict_result = EVICT_FAIL_UNSUPPORTED;
        } else {
            if (evict_result) *evict_result = EVICT_SUCC_FREED;
        }
        return 0;
    } else {
        if (evict_result) *evict_result = EVICT_SUCC_SWAPPED;
        return 1;
    }
}

/* EVICT is a special command that getswaps returns nothing ('cause we don't
 * need to swap anything before command executes) but does swap out(PUT)
 * inside command func. Note that EVICT is the command of fake evict clients */
void swapEvictCommand(client *c) {
    int i, nevict = 0;

    for (i = 1; i < c->argc; i++) {
        nevict += tryEvictKey(c->db,c->argv[i],NULL);
    }

    addReplyLongLong(c, nevict);
}

void tryEvictKeyAsapLater(redisDb *db, robj *key) {
    incrRefCount(key);
    listAddNodeTail(db->evict_asap, key);
}

void swapDebugEvictKeys() {
    int i = 0, j, swap_debug_evict_keys = server.swap_debug_evict_keys;
    if (swap_debug_evict_keys < 0) swap_debug_evict_keys = INT_MAX;
    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db + j;
        dictEntry *de;
        dictIterator *di = dictGetSafeIterator(db->dict);
        while ((de = dictNext(di)) && i++ < swap_debug_evict_keys) {
            sds key = dictGetKey(de);
            robj *keyobj = createStringObject(key,sdslen(key));
            tryEvictKey(db, keyobj, NULL);
            decrRefCount(keyobj);
        }
        dictReleaseIterator(di);
        if (i >= swap_debug_evict_keys) return;
    }
}

void debugSwapOutCommand(client *c) {
    int i, nevict = 0, evict_result;
    if (c->argc == 2) {
        dictEntry* de;
        dictIterator* di = dictGetSafeIterator(c->db->dict);
        while((de = dictNext(di)) != NULL) {
            sds key = dictGetKey(de);
            evict_result = 0;
            robj* k = createRawStringObject(key, sdslen(key));
            nevict += tryEvictKey(c->db, k, &evict_result);
            decrRefCount(k);
        }
        dictReleaseIterator(di);
    } else {
        for (i = 1; i < c->argc; i++) {
            evict_result = 0;
            nevict += tryEvictKey(c->db, c->argv[i], &evict_result);
            serverLog(LL_NOTICE, "debug swapout %s: %s.", (sds)c->argv[i]->ptr, evictResultName(evict_result));
        }
    }
    addReplyLongLong(c, nevict);
}

/* ----------------------------- swap evict ------------------------------ */
unsigned long long calculateNextMemoryLimit(size_t mem_used, unsigned long long from, unsigned long long to) {
    if (from <= to || to == 0) return to;
    /* scale down maxmemory step by step for low evict concurrent */
    unsigned long long safe_mem_limit = mem_used - server.maxmemory_scaledown_rate;
    if (safe_mem_limit < from) {
        return safe_mem_limit > to ? safe_mem_limit : to;
    }
    return from;
}

void updateMaxMemoryScaleFrom() {
    size_t mem_used = ctrip_getUsedMemory();
    server.maxmemory_scale_from = calculateNextMemoryLimit(mem_used, server.maxmemory_scale_from, server.maxmemory);
}

inline int swapEvictGetInprogressLimit(size_t mem_tofree) {
    /* Base inprogress limit is threads num(deffer thread), increase one every n MB */
    int inprogress_limit = 1 + mem_tofree/(server.swap_evict_inprogress_growth_rate);

    if (inprogress_limit > server.swap_evict_inprogress_limit)
        inprogress_limit = server.swap_evict_inprogress_limit;
    return inprogress_limit;
}

inline size_t ctrip_getMemoryToFree(size_t mem_used) {
    size_t mem_tofree;
    if (server.swap_mode != SWAP_MODE_MEMORY && server.maxmemory_scale_from > server.maxmemory) {
        mem_tofree = mem_used - server.maxmemory_scale_from;
    } else {
        mem_tofree = mem_used - server.maxmemory;
    }
    return mem_tofree;
}

inline int swapEvictionReachedInprogressLimit() {
    return server.swap_eviction_ctx->inprogress_count >=
        server.swap_eviction_ctx->inprogress_limit;
}

inline void ctrip_performEvictionStart(swapEvictKeysCtx *sectx) {
    if (sectx->swap_mode == SWAP_MODE_MEMORY) return;
    /* Evict keys registered to be evicted ASAP even if not over maxmemory,
     * because evict asap could reduce cow. */
    server.swap_eviction_ctx->inprogress_limit = swapEvictGetInprogressLimit(sectx->mem_tofree);
    if (swapEvictAsap() == EVICT_ASAP_AGAIN) {
        ctrip_startEvictionTimeProc();
    }
}

inline int ctrip_performEvictionLoopStartShouldBreak(swapEvictKeysCtx *sectx) {
    if (sectx->swap_mode == SWAP_MODE_MEMORY) return 0;
    if (swapEvictionReachedInprogressLimit()) {
        ctrip_startEvictionTimeProc();
        return 1;
    } else {
        return 0;
    }
}

swapEvictionCtx *swapEvictionCtxCreate() {
    swapEvictionCtx *ctx = zcalloc(sizeof(swapEvictionCtx));
    return ctx;
}

void swapEvictionCtxFree(swapEvictionCtx *ctx) {
    if (ctx == NULL) return;
    zfree(ctx);
}

static inline void swapEvictionCtxUpdateStat(swapEvictionCtx *ctx, int evict_result) {
    ctx->stat.evict_result[evict_result]++;
}

inline size_t performEvictionSwapSelectedKey(swapEvictKeysCtx *sectx, redisDb *db,
        robj *keyobj) {
    int evict_result;
    size_t mem_freed;
    mstime_t eviction_latency;
    swapEvictionCtx *ctx = server.swap_eviction_ctx;

    serverAssert(sectx->swap_mode != SWAP_MODE_MEMORY);

    latencyStartMonitor(eviction_latency);

    /* Key might be directly freed if not dirty, so we need to compute key
     * size beforehand. */
    mem_freed = keyEstimateSize(db, keyobj);
    sectx->swap_trigged += tryEvictKey(db, keyobj, &evict_result);
    if (evictResultIsSucc(evict_result)) {
        ctx->failed_inrow = 0;
        notifyKeyspaceEvent(NOTIFY_EVICTED, "swap-evicted", keyobj, db->id);
    } else {
        ctx->failed_inrow++;
    }
    swapEvictionCtxUpdateStat(ctx,evict_result);

    latencyEndMonitor(eviction_latency);
    latencyAddSampleIfNeeded("swap-eviction",eviction_latency);

    sectx->keys_scanned++;
    return mem_freed;
}

inline int ctrip_performEvictionLoopCheckShouldBreak(swapEvictKeysCtx *sectx) {
    swapEvictionCtx *ctx = server.swap_eviction_ctx;
    if (sectx->swap_mode == SWAP_MODE_MEMORY) return 0;
    /* evict failed too much continously, continue evict are most
     * likely to fail again. */
    if (ctx->failed_inrow > 16) return 1;
    return 0;
}

inline void ctrip_performEvictionEnd(swapEvictKeysCtx *sectx) {
    static long long nscaned, nloop, nswap;
    static mstime_t prev_logtime;

    if (sectx->swap_mode == SWAP_MODE_MEMORY || sectx->ended) return;

    nloop++;
    nscaned += sectx->keys_scanned;
    nswap += sectx->swap_trigged;

    if (server.mstime - prev_logtime > 1000) {
        serverLog(LL_VERBOSE,
                "Eviction loop=%lld,scaned=%lld,swapped=%lld,mem_used=%ld,mem_inprogress=%ld",
                nloop, nscaned, nswap, sectx->mem_used, server.swap_inprogress_memory);
        prev_logtime = server.mstime;
        nscaned = 0, nloop = 0, nswap = 0;
    }

    sectx->ended = 1;
}

sds genSwapEvictionInfoString(sds info) {
    swapEvictionCtx *ctx = server.swap_eviction_ctx;

    info = sdscatprintf(info,"swap_inprogress_evict_count:%lld\r\n",
            ctx->inprogress_count);

    info = sdscatprintf(info,"swap_evict_stat:");
    for (int i = 0; i < EVICT_RESULT_TYPES; i++) {
        long long count = ctx->stat.evict_result[i];
        if (i == 0) {
            info = sdscatprintf(info,"%s=%lld",evictResultName(i),count);
        } else {
            info = sdscatprintf(info,",%s=%lld",evictResultName(i),count);
        }
    }
    info = sdscatprintf(info,"\r\n");
    return info;
}

/* ----------------------------- evict asap ------------------------------ */
#define EVICT_ASAP_KEYS_LIMIT 256

int swapEvictAsap() {
    static mstime_t stat_mstime;
    static long stat_evict, stat_scan, stat_loop;

    int evicted = 0, scanned = 0, result = EVICT_ASAP_OK;

    for (int i = 0; i < server.dbnum; i++) {
        listIter li;
        listNode *ln;
        redisDb *db = server.db+i;

        if (listLength(db->evict_asap) == 0) continue;

        listRewind(db->evict_asap, &li);
        while ((ln = listNext(&li))) {
            int evict_result;
            robj *key = listNodeValue(ln);

            if (swapEvictionReachedInprogressLimit() ||
                    scanned >= EVICT_ASAP_KEYS_LIMIT) {
                result = EVICT_ASAP_AGAIN;
                goto end;
            }

            tryEvictKey(db, key, &evict_result);

            scanned++;
            if (evict_result == EVICT_FAIL_SWAPPING) {
                /* Try evict again if key is holded or swapping */
                listAddNodeHead(db->evict_asap, key);
            } else {
                decrRefCount(key);
                evicted++;
            }
            listDelNode(db->evict_asap, ln);
        }
    }

end:
    stat_loop++;
    stat_evict += evicted;
    stat_scan += scanned;

    if (server.mstime - stat_mstime > 1000) {
        if (stat_scan > 0) {
            serverLog(LL_VERBOSE, "SwapEvictAsap loop=%ld,scaned=%ld,swapped=%ld",
                    stat_loop, stat_scan, stat_evict);
        }
        stat_mstime = server.mstime;
        stat_loop = 0, stat_evict = 0, stat_scan = 0;
    }

    return result;
}

/* ----------------------------- ratelimit ------------------------------ */
#define SWAP_RATELIMIT_PAUSE_MAX_MS 100

static inline int swapRatelimitIsNeccessary(int policy, int *pms) {
    int pause_ms;
    static mstime_t prev_logtime;
    size_t mem_reported, mem_used, mem_ratelimit;

    if (pms) *pms = 0;

    /* mem_used are not returned if not overmaxmemory. */
    if (!getMaxmemoryState(&mem_reported,&mem_used,NULL,NULL)) return 0;

    mem_ratelimit = server.maxmemory*server.swap_ratelimit_maxmemory_percentage/100;
    if (mem_used <= mem_ratelimit)  return 0;

    if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
        pause_ms = (mem_used - mem_ratelimit)/server.swap_ratelimit_pause_growth_rate;
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

        serverLog(LL_NOTICE,"[ratelimit] mem_used(%ld) > (%ld): %s", mem_used, mem_ratelimit, msg);
        prev_logtime = server.mstime;
    }

    return 1;
}

void rejectCommand(client *c, robj *reply);
/* return 1 if command rejected */
int swapRateLimitReject(client *c) {
    serverAssert(server.swap_mode != SWAP_MODE_MEMORY);

    if (server.swap_ratelimit_policy != SWAP_RATELIMIT_POLICY_REJECT_OOM &&
        server.swap_ratelimit_policy != SWAP_RATELIMIT_POLICY_REJECT_ALL) {
        return 0;
    }

    /* Never reject replicated commands from master */
    if (c->flags & CLIENT_MASTER) return 0;

    int is_read_command = (c->cmd->flags & CMD_READONLY) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_READONLY));
    int is_write_command = (c->cmd->flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    int is_denyoom_command = (c->cmd->flags & CMD_DENYOOM) ||
                             (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_DENYOOM));

    if ((server.swap_ratelimit_policy == SWAP_RATELIMIT_POLICY_REJECT_OOM &&
                is_denyoom_command) ||
        (server.swap_ratelimit_policy == SWAP_RATELIMIT_POLICY_REJECT_ALL &&
               (is_read_command || is_write_command))) {
        if (swapRatelimitIsNeccessary(server.swap_ratelimit_policy,NULL)) {
            rejectCommand(c, shared.oomerr);
            server.stat_swap_ratelimit_rejected_cmd_count++;
            return 1;
        }
    }

    return 0;
}

static int unprotectClientdProc(
        struct aeEventLoop *el, long long id, void *clientData) {
    client *c = clientData;
    UNUSED(el), UNUSED(id);
    unprotectClient(c);
    return AE_NOMORE;
}

void swapRateLimitPause(client *c) {
    int pause_ms;
    serverAssert(server.swap_mode != SWAP_MODE_MEMORY);

    if (server.swap_ratelimit_policy != SWAP_RATELIMIT_POLICY_PAUSE) return;

    if (swapRatelimitIsNeccessary(server.swap_ratelimit_policy,&pause_ms) &&
            pause_ms > 0) {
        protectClient(c);
        aeCreateTimeEvent(server.el,pause_ms,unprotectClientdProc,c,NULL);
        server.stat_swap_ratelimit_client_pause_count++;
        server.stat_swap_ratelimit_client_pause_ms += pause_ms;
    }
}

void trackSwapRateLimitInstantaneousMetrics() {
    int count_metric_idx =
        SWAP_RATELIMIT_STATS_METRIC_OFFSET+SWAP_RATELIMIT_STATS_METRIC_PAUSE_COUNT;
    int ms_metric_idx =
        SWAP_RATELIMIT_STATS_METRIC_OFFSET+SWAP_RATELIMIT_STATS_METRIC_PAUSE_MS;

    trackInstantaneousMetric(count_metric_idx,server.stat_swap_ratelimit_client_pause_count);
    trackInstantaneousMetric(ms_metric_idx,server.stat_swap_ratelimit_client_pause_ms);
}

void resetSwapRateLimitInstantaneousMetrics() {
    server.stat_swap_ratelimit_client_pause_count = 0;
    server.stat_swap_ratelimit_client_pause_ms = 0;
}

sds genSwapRateLimitInfoString(sds info) {
    int count_metric_idx =
        SWAP_RATELIMIT_STATS_METRIC_OFFSET+SWAP_RATELIMIT_STATS_METRIC_PAUSE_COUNT;
    int ms_metric_idx =
        SWAP_RATELIMIT_STATS_METRIC_OFFSET+SWAP_RATELIMIT_STATS_METRIC_PAUSE_MS;
    long long count_ps = getInstantaneousMetric(count_metric_idx),
         ms_ps = getInstantaneousMetric(ms_metric_idx);;
    float avg_pause_ms = count_ps > 0 ? (double)ms_ps/count_ps : 0;

    info = sdscatprintf(info,
            "swap_ratelimit_client_pause_instantaneous_ms:%.2f\r\n"
            "swap_ratelimit_client_pause_count:%lld\r\n"
            "swap_ratelimit_client_pause_ms:%lld\r\n"
            "swap_ratelimit_rejected_cmd_count:%lld\r\n",
            avg_pause_ms,
            server.stat_swap_ratelimit_client_pause_count,
            server.stat_swap_ratelimit_client_pause_ms,
            server.stat_swap_ratelimit_rejected_cmd_count);
    return info;
}

