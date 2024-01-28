/* Copyright (c) 2023, ctrip.com
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
#include "dict.h"

static void coldFilterDisableCuckooFilters() {
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        if (db->cold_filter->filter == NULL) continue;
        cuckooFilterFree(db->cold_filter->filter);
        db->cold_filter->filter = NULL;
    }
    server.swap_cuckoo_filter_enabled = 0;
}

static inline
void coldFilterInitAbsentCache(coldFilter *filter) {
    if (server.swap_absent_cache_enabled) {
        filter->absents = absentCacheNew(server.swap_absent_cache_capacity);
    }
}

static inline
void coldFilterInitCuckooFilter(coldFilter *filter) {
    if (server.swap_cuckoo_filter_enabled && filter->filter == NULL) {
        filter->filter = cuckooFilterNew(
                cuckooGenHashFunction,
                server.swap_cuckoo_filter_bit_type,
                server.swap_cuckoo_filter_estimated_keys);
    }
}

void coldFilterDeinit(coldFilter *filter) {
    if (filter->absents) {
        absentCacheFree(filter->absents);
        filter->absents = NULL;
    }
    if (filter->filter) {
        cuckooFilterFree(filter->filter);
        filter->filter = NULL;
    }
}

coldFilter *coldFilterCreate() {
    coldFilter *filter = zcalloc(sizeof(coldFilter));
    coldFilterInitAbsentCache(filter);
    return filter;
}

void coldFilterDestroy(coldFilter *filter) {
    if (filter == NULL) return;
    coldFilterDeinit(filter);
    zfree(filter);
}

void coldFilterReset(coldFilter *filter) {
    coldFilterDeinit(filter);
    coldFilterInitAbsentCache(filter);
}

void coldFilterAddKey(coldFilter *filter, sds key) {
    /* cuckoo filter are lazily created to save memory */
    coldFilterInitCuckooFilter(filter);

    if (filter->filter) {
        if (cuckooFilterInsert(filter->filter,key,sdslen(key)) == CUCKOO_ERR) {
            cuckooFilterStat stat;
            cuckooFilterGetStat(filter->filter,&stat);

            coldFilterDisableCuckooFilters();

            serverLog(LL_WARNING,
                    "Insert key(%s) to cuckoo filter(ntables=%ld,ntags=%ld,used_memory=%ld,load_factor=%.2f) failed, cuckoo filter turned off.",
                    key,stat.ntables,stat.ntags,stat.used_memory,stat.load_factor);
        }
    }

    if (filter->absents) absentCacheDelete(filter->absents,key);
}

void coldFilterDeleteKey(coldFilter *filter, sds key) {
    if (filter->filter) {
        serverAssert(cuckooFilterDelete(filter->filter,key,sdslen(key)) == CUCKOO_OK);
    }
}

void coldFilterKeyNotFound(coldFilter *filter, sds key) {
    if (filter->absents) absentCachePutKey(filter->absents,key);
    if (filter->filter) filter->filter_stat.false_positive_count++;
}

int coldFilterMayContainKey(coldFilter *filter, sds key, int *filt_by) {
    /* cuckoo filter are lazily created to save memory */
    coldFilterInitCuckooFilter(filter);

    if (filter->filter) {
        filter->filter_stat.lookup_count++;
        if (cuckooFilterContains(filter->filter,key,sdslen(key)) == CUCKOO_ERR) {
            *filt_by = COLDFILTER_FILT_BY_CUCKOO_FILTER;
            return 0;
        }
    }

    if (filter->absents && absentCacheGetKey(filter->absents,key)) {
        *filt_by = COLDFILTER_FILT_BY_ABSENT_CACHE;
        return 0;
    }

    return 1;
}

/* Invalidate key from absent cache when subkey added to rocksdb so that
 * we don't need to save evicted subkeys (cant maintain absent cache
 * from swap thread) and delete every subkey in main thread.  */
void coldFilterSubkeyAdded(coldFilter *filter, sds key) {
    if (server.swap_absent_cache_include_subkey && filter->absents)
        absentCacheDelete(filter->absents,key);
}

void coldFilterSubkeyNotFound(coldFilter *filter, sds key, sds subkey) {
    if (server.swap_absent_cache_include_subkey && filter->absents)
        absentCachePutSubkey(filter->absents,key,subkey);
}

int coldFilterMayContainSubkey(coldFilter *filter, sds key, sds subkey) {
    if (server.swap_absent_cache_include_subkey && filter->absents) {
        atomicIncr(server.swap_hit_stats->stat_absent_subkey_query_count,1);
        if (absentCacheGetSubkey(filter->absents,key,subkey)) {
            atomicIncr(server.swap_hit_stats->stat_absent_subkey_filt_count,1);
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }
}

/* cuckoo filter not counted in maxmemory */
size_t coldFiltersUsedMemory() {
    size_t used_memory = 0;
    for (int i = 0; i < server.dbnum; i++) {
        coldFilter *cold_filter = (server.db+i)->cold_filter;
        if (cold_filter->filter) {
            used_memory += cuckooFilterUsedMemory(cold_filter->filter);
        }
    }
    return used_memory;
}

void swapCuckooFilterStatInit(swapCuckooFilterStat *stat) {
    stat->lookup_count = 0;
    stat->false_positive_count = 0;
}

void swapCuckooFilterStatDeinit(swapCuckooFilterStat *stat) {
    UNUSED(stat);
}

void trackSwapCuckooFilterInstantaneousMetrics() {
    long long lookup = 0, false_positive = 0;
    int lookup_metric_idx =
        SWAP_FILTER_STATS_METRIC_OFFSET+SWAP_FILTER_STATS_METRIC_LOOKUP;
    int fp_metric_idx =
        SWAP_FILTER_STATS_METRIC_OFFSET+SWAP_FILTER_STATS_METRIC_FALSE_POSITIVE;

    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        swapCuckooFilterStat *stat;

        if (db->cold_filter == NULL) continue;

        stat = &db->cold_filter->filter_stat;
        lookup += stat->lookup_count;
        false_positive += stat->false_positive_count;
    }

    trackInstantaneousMetric(lookup_metric_idx,lookup);
    trackInstantaneousMetric(fp_metric_idx,false_positive);
}

void resetSwapCukooFilterInstantaneousMetrics() {
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        swapCuckooFilterStat *stat;
        if (db->cold_filter == NULL) continue;
        stat = &db->cold_filter->filter_stat;
        swapCuckooFilterStatInit(stat);
    }
}

sds genSwapCuckooFilterInfoString(sds info) {
    double fpr;
    long long lookup_ps, false_positive_ps;
    cuckooFilterStat cuckoo_stat_, *cuckoo_stat = &cuckoo_stat_;
    int lookup_metric_idx =
        SWAP_FILTER_STATS_METRIC_OFFSET+SWAP_FILTER_STATS_METRIC_LOOKUP;
    int fp_metric_idx =
        SWAP_FILTER_STATS_METRIC_OFFSET+SWAP_FILTER_STATS_METRIC_FALSE_POSITIVE;

    lookup_ps = getInstantaneousMetric(lookup_metric_idx);
    false_positive_ps = getInstantaneousMetric(fp_metric_idx);
    fpr = lookup_ps > 0 ? (double)false_positive_ps/lookup_ps : 0;
    info = sdscatprintf(info,"swap_cuckoo_filter_instantaneous_fpr:%.2f%%\r\n",fpr*100);

    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        if (db->cold_filter->filter == NULL) continue;
        cuckooFilterGetStat(db->cold_filter->filter,cuckoo_stat);
        info = sdscatprintf(info,
                "swap_cuckoo_filter%d:used_memory=%ld,tags=%ld,load_factor=%.2f\r\n",
                i,cuckoo_stat->used_memory,cuckoo_stat->ntags,cuckoo_stat->load_factor);
    }

    return info;
}

