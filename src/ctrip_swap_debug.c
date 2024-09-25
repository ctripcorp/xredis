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

static sds debugRioGet(int cf, sds rawkey) {
    sds rawval;
    RIO _rio, *rio = &_rio;
    int *cfs = zmalloc(1*sizeof(int));
    sds *rawkeys = zmalloc(1*sizeof(sds));
    cfs[0] = cf;
    rawkeys[0] = sdsdup(rawkey);
    RIOInitGet(rio,1,cfs,rawkeys);
    RIODo(rio);
    rawval = rio->get.rawvals[0] ? sdsdup(rio->get.rawvals[0]) : NULL;
    RIODeinit(rio);
    return rawval;
}

static sds getSwapObjectInfo(robj *o) {
    if (o) {
        return sdscatprintf(sdsempty(),
                "at=%p,refcount=%d,type=%s,encoding=%s,dirty_meta=%d,dirty_data=%d,"
                "persistent=%d,persist_keep=%d,lru=%d,lru_seconds_idle=%llu",
                (void*)o,o->refcount,strObjectType(o->type),
                strEncoding(o->encoding),o->dirty_meta,o->dirty_data,
                o->persistent,o->persist_keep,o->lru,estimateObjectIdleTime(o)/1000);
    } else {
        return sdsnew("<nil>");
    }
}

static sds getSwapMetaInfo(int swap_type, long long expire,objectMeta *m) {
    if (swap_type == -1) return sdsnew("<nil>");
    sds info = sdscatprintf(sdsempty(),"swap_type=%d,expire=%lld",
            swap_type,expire);
    if (m) {
        sds omdump = dumpObjectMeta(m);
        info = sdscatprintf(info, ",at=%p,%s",(void*)m,omdump);
        sdsfree(omdump);
    } else {
        info = sdscatprintf(info, ",at=<nil>");
    }
    return info;
}

static int getCfOrReply(client *c, robj *cf) {
    if (!strcasecmp(cf->ptr,"meta")) {
        return META_CF;
    } else if (!strcasecmp(cf->ptr, "data")) {
        return DATA_CF;
    } else if (!strcasecmp(cf->ptr, "score")) {
        return SCORE_CF;
    } else {
        addReplyError(c,"invalid cf");
        return -1;
    }
}

static sds calculateNextPrefix(sds current) {
    sds next = NULL;
    size_t nextlen = sdslen(current);

    do {
        if (current[nextlen - 1] != (char)0xff) break;
        nextlen--;
    } while(nextlen > 0);

    if (0 == nextlen) return NULL;

    next = sdsnewlen(current, nextlen);
    next[nextlen - 1]++;

    return next;
}

void swapCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"OBJECT <key>",
"    Show info about `key` and assosiated value.",
"ENCODE-META-KEY <key>",
"    Encode meta key.",
"DECODE-META-KEY <rawkey>",
"    Decode meta key.",
"ENCODE-DATA-KEY <key> <version> <subkey>",
"    Encode data key.",
"DECODE-DATA-KEY <rawkey>",
"    Decode data key.",
"RIO-GET meta|data <rawkey> <rawkey> ...",
"    Get raw value from rocksdb.",
"RIO-SCAN meta|data <prefix>",
"    Scan rocksdb with prefix.",
"RIO-ERROR <count> [ACTION name]",
"    Make next count rio return error.",
"RESET-STATS",
"    Reset swap stats.",
"COMPACT",
"   COMPACT rocksdb",
"FLUSH [<cfname,cfname...>]",
"   Flush rocksdb",
"ROCKSDB-PROPERTY-INT <rocksdb-prop-name> [<cfname,cfname...>]",
"    Get rocksdb property value (int type)",
"ROCKSDB-PROPERTY-VALUE <rocksdb-prop-name> [<cfname,cfname...>]",
"    Get rocksdb property value (string type)",
"SCAN-SESSION [<cursor>]",
"    List assigned scan sesions",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"object") && c->argc == 3) {
        redisDb *db = c->db;
        robj *key = c->argv[2];
        robj *value = lookupKey(db,key,LOOKUP_NOTOUCH);
        objectMeta *hot_meta = lookupMeta(db,key), *cold_meta = NULL;
        long long hot_expire = getExpire(db,key), cold_expire = -1;
        sds meta_rawkey = NULL, meta_rawval = NULL;
        int hot_swap_type = hot_meta ? hot_meta->swap_type : -1;
        uint64_t cold_version;
        int cold_swap_type = -1;

        meta_rawkey = rocksEncodeMetaKey(db,key->ptr);
        meta_rawval = debugRioGet(META_CF,meta_rawkey);
        if (meta_rawval) {
            const char *extend;
            size_t extlen;
            rocksDecodeMetaVal(meta_rawval,sdslen(meta_rawval),
                    &cold_swap_type,&cold_expire,&cold_version,&extend,&extlen);
            if (extend) {
                buildObjectMeta(cold_swap_type,cold_version,extend,extlen,&cold_meta);
            }
        }

        if (!value && !hot_meta && !meta_rawval) {
            addReplyErrorObject(c,shared.nokeyerr);
            if (meta_rawkey) sdsfree(meta_rawkey);
            if (meta_rawval) sdsfree(meta_rawval);
            return;
        }

        sds value_info = getSwapObjectInfo(value);
        sds hot_meta_info = getSwapMetaInfo(hot_swap_type,hot_expire,hot_meta);
        sds cold_meta_info = getSwapMetaInfo(cold_swap_type,cold_expire,cold_meta);
        sds info = sdscatprintf(sdsempty(),
                "value: %s\nhot_meta: %s\ncold_meta: %s\n",
                value_info,hot_meta_info,cold_meta_info);
        addReplyVerbatim(c,info,sdslen(info),"txt");
        sdsfree(value_info);
        sdsfree(hot_meta_info);
        sdsfree(cold_meta_info);
        sdsfree(info);
        if (meta_rawkey) sdsfree(meta_rawkey);
        if (meta_rawval) sdsfree(meta_rawval);
        if (cold_meta) freeObjectMeta(cold_meta);
    } else if (!strcasecmp(c->argv[1]->ptr,"encode-meta-key") && c->argc == 3) {
        addReplyBulkSds(c,rocksEncodeMetaKey(c->db,c->argv[2]->ptr));
    } else if (!strcasecmp(c->argv[1]->ptr,"decode-meta-key") && c->argc == 3) {
        int dbid, retval;
        sds rawkey = c->argv[2]->ptr;
        const char *key;
        size_t keylen;

        retval = rocksDecodeMetaKey(rawkey,sdslen(rawkey),&dbid,&key,&keylen);
        if (retval) {
            addReplyError(c,"invalid meta key");
        } else {
            addReplyArrayLen(c,2);
            addReplyBulkLongLong(c,dbid);
            addReplyBulkCBuffer(c,key,keylen);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"encode-data-key") && c->argc == 5) {
        long long version;
        if (getLongLongFromObject(c->argv[3],&version)) {
            addReplyError(c,"invalid version");
        } else {
            addReplyBulkSds(c,rocksEncodeDataKey(c->db,c->argv[2]->ptr,version,
                        c->argv[4]->ptr));
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"decode-data-key") && c->argc == 3) {
        int dbid, retval;
        sds rawkey = c->argv[2]->ptr;
        const char *key, *subkey;
        size_t keylen, sublen;
        uint64_t version;

        retval = rocksDecodeDataKey(rawkey,sdslen(rawkey),
                &dbid,&key,&keylen,&version,&subkey,&sublen);
        if (retval) {
            addReplyError(c,"invalid data key");
        } else {
            addReplyArrayLen(c,4);
            addReplyBulkLongLong(c,dbid);
            addReplyBulkCBuffer(c,key,keylen);
            addReplyBulkLongLong(c,version);
            addReplyBulkCBuffer(c,subkey,sublen);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"rio-get") && c->argc >= 4) {
        int cf;
        addReplyArrayLen(c, c->argc-3);
        if ((cf = getCfOrReply(c,c->argv[2])) < 0) return;
        for (int i = 3; i < c->argc; i++) {
            sds rawval = debugRioGet(cf,c->argv[i]->ptr);
            if (rawval == NULL) {
                addReplyNull(c);
            } else {
                addReplyBulkSds(c,sdsdup(rawval));
            }
            sdsfree(rawval);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"rio-scan") && c->argc == 4) {
        int cf;
        RIO _rio, *rio = &_rio;
        if ((cf = getCfOrReply(c,c->argv[2])) < 0) return;
        sds prefix = sdsdup(c->argv[3]->ptr);
        sds end = NULL;
        if (0 != sdslen(prefix)) end = calculateNextPrefix(prefix);
        RIOInitIterate(rio, cf, 0, prefix, end, ROCKS_ITERATE_NO_LIMIT);
        RIODo(rio);
        addReplyArrayLen(c,rio->iterate.numkeys);
        for (int i = 0; i < rio->iterate.numkeys; i++) {
            sds repr = sdsempty();
            repr = sdscatsds(repr, rio->iterate.rawkeys[i]);
            repr = sdscat(repr, "=>");
            repr = sdscatsds(repr, rio->iterate.rawvals[i]);
            addReplyBulkSds(c,repr);
        }
        RIODeinit(rio);
    } else if (!strcasecmp(c->argv[1]->ptr,"rio-error") && c->argc >= 3) {
        long long count;
        int action = 0;
        if (getLongLongFromObjectOrReply(c,c->argv[2],&count,NULL))
            return;
        if (c->argc > 3) {
            if (c->argc == 5 && !strcasecmp(c->argv[3]->ptr,"action")) {
                action = rocksActionFromName(c->argv[4]->ptr);
                if (action == -1) {
                    addReplyError(c,"rio-error invalid action");
                    return;
                }
            } else {
                addReplyError(c,"rio-error invalid arg");
                return;
            }
        }
        if (count > INT_MAX || count < 0) {
            addReplyError(c,"rio-error count invalid");
        } else {
            server.swap_debug_rio_error = (int)count;
            server.swap_debug_rio_error_action = action;
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"reset-stats") && c->argc == 2) {
        resetStatsSwap();
        resetSwapHitStat();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"compact") && c->argc == 2) {
        sds error = NULL;
        /* no specified compact range, to launch a full compact. */
        compactTask *task = compactTaskNew();
        task->compact_type = TYPE_FULL_COMPACT;

        compactKeyRange *meta_key_range = zcalloc(sizeof(compactKeyRange));
        meta_key_range->cf_index = META_CF;

        compactKeyRange *data_key_range = zcalloc(sizeof(compactKeyRange));
        data_key_range->cf_index = DATA_CF;

        compactKeyRange *score_key_range = zcalloc(sizeof(compactKeyRange));
        score_key_range->cf_index = SCORE_CF;
        
        compactTaskAppend(task,meta_key_range);
        compactTaskAppend(task,data_key_range);
        compactTaskAppend(task,score_key_range);

        if (submitUtilTask(ROCKSDB_COMPACT_RANGE_TASK, task, rocksdbCompactRangeTaskDone, task, &error)) {
            addReply(c,shared.ok);
        } else {
            addReplyErrorSds(c,error);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"flush") && c->argc >= 2) {
        sds error = NULL;
        const char *cfnames = c->argc > 2 ? c->argv[2]->ptr : NULL;
        rocks *rocks = serverRocksGetReadLock();
        swapData4RocksdbFlush *data = rocksdbFlushTaskArgCreate(rocks,cfnames);
        serverRocksUnlock(rocks);
        if (submitUtilTask(ROCKSDB_FLUSH_TASK, data, rocksdbFlushTaskDone, data, &error)) {
            addReply(c,shared.ok);
        } else {
            addReplyErrorSds(c,error);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"rocksdb-property-int") && c->argc >= 3) {
        uint64_t property_int = 0;
        const char *cfnames = c->argc > 3 ? c->argv[3]->ptr : NULL;
        rocks *rocks = serverRocksGetReadLock();
        rocksPropertyInt(rocks,cfnames,c->argv[2]->ptr,&property_int);
        serverRocksUnlock(rocks);
        addReplyLongLong(c, property_int);
    } else if (!strcasecmp(c->argv[1]->ptr,"rocksdb-property-value") && c->argc >= 3) {
        const char *cfnames = c->argc > 3 ? c->argv[3]->ptr : NULL;
        rocks *rocks = serverRocksGetReadLock();
        sds property_value = rocksPropertyValue(rocks,cfnames,c->argv[2]->ptr);
        serverRocksUnlock(rocks);
        addReplyBulkCString(c, property_value);
        if (property_value) sdsfree(property_value);
    } else if (!strcasecmp(c->argv[1]->ptr,"scan-session") &&
            (c->argc == 2 || c->argc == 3)) {
        long long outer_cursor;
        if (c->argc == 2) {
            outer_cursor = -1;
        } else {
            if (getLongLongFromObjectOrReply(c,c->argv[2],&outer_cursor,
                        "Invalid cursor")) {
                return;
            }
            if (outer_cursor < 0) {
                addReplyError(c,"Invalid cursor");
                return;
            }
        }
        sds o = getAllSwapScanSessionsInfoString(outer_cursor);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* swap.debug will requires global lock. */
void swapDebugCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"RORDB BGSAVE|RELOAD",
"    Background save or reload with rordb format.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"rordb") && c->argc == 3) {
        if (!strcasecmp(c->argv[2]->ptr,"bgsave")) {
            rdbSaveInfo rsi, *rsiptr;
            swapForkRocksdbCtx *sfrctx = NULL;

            rsiptr = rdbPopulateSaveInfo(&rsi);
            if (server.child_type == CHILD_TYPE_RDB || hasActiveChildProcess()) {
                addReplyError(c,"Background child already in progress");
                return;
            }
            sfrctx = swapForkRocksdbCtxCreate(SWAP_FORK_ROCKSDB_TYPE_CHECKPOINT);
            if (rdbSaveBackground(server.rdb_filename,rsiptr,sfrctx,1) == C_OK) {
                addReplyStatus(c,"Background saving started(rordb mode)");
            } else {
                addReplyErrorObject(c,shared.err);
            }
        } else if (!strcasecmp(c->argv[2]->ptr,"reload")) {
            rdbSaveInfo rsi, *rsiptr;
            rsiptr = rdbPopulateSaveInfo(&rsi);
            sds checkpoint_dir = sdscatprintf(sdsempty(),"%s/tmp_%lld",ROCKS_DATA,ustime());
            rocks *rocks = serverRocksGetReadLock();
            int ret2 = rocksCreateCheckpoint(rocks, checkpoint_dir);
            if (ret2 != C_OK) {
                addReplyError(c,"Error creating checkpoint");
                return;
            }
            if (rdbSave(server.rdb_filename,rsiptr,1) != C_OK) {
                addReplyError(c,"Error saving rordb");
                return;
            }
            rocksReleaseCheckpoint(rocks);
            serverRocksUnlock(rocks);
            emptyDb(-1,EMPTYDB_NO_FLAGS,NULL);
            protectClient(c);
            int ret = rdbLoad(server.rdb_filename,NULL,RDBFLAGS_NONE);
            unprotectClient(c);
            if (ret != C_OK) {
                addReplyError(c,"Error trying to load RORDB");
                return;
            }
            serverLog(LL_WARNING,"DB reloaded by SWAP.DEBUG RORDB RELOAD");
            addReply(c,shared.ok);
        } else {
            addReplySubcommandSyntaxError(c);
        }
    } else {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

#ifdef SWAP_DEBUG

void swapDebugMsgsInit(swapDebugMsgs *msgs, char *identity) {
    snprintf(msgs->identity,MAX_MSG,"[%s]",identity);
}

void swapDebugMsgsAppendV(swapDebugMsgs *msgs, char *step, char *fmt, va_list ap) {
    char *name = msgs->steps[msgs->index].name;
    char *info = msgs->steps[msgs->index].info;
    strncpy(name,step,MAX_MSG-1);
    vsnprintf(info,MAX_MSG,fmt,ap);
    msgs->index++;
}

void swapDebugMsgsAppend(swapDebugMsgs *msgs, char *step, char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    swapDebugMsgsAppendV(msgs, step, fmt, ap);
    va_end(ap);
}

void swapDebugBatchMsgsAppend(swapExecBatch *batch, char *step, char *fmt, ...) {
    for (size_t i = 0; i < batch->count; i++) {
        va_list ap;
        va_start(ap,fmt);
        swapDebugMsgsAppend(batch->reqs[i]->msgs, step, fmt, ap);
        va_end(ap);
    }
}

void swapDebugMsgsDump(swapDebugMsgs *msgs) {
    serverLog(LL_NOTICE,"=== %s ===", msgs->identity);
    for (int i = 0; i < msgs->index; i++) {
        char *name = msgs->steps[i].name;
        char *info = msgs->steps[i].info;
        serverLog(LL_NOTICE,"%2d %25s : %s",i,name,info);
    }
}

#endif

