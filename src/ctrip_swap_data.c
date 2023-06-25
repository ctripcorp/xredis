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

swapData *createSwapData(redisDb *db, robj *key, robj *value, robj *dirty_subkeys) {
    swapData *data = zcalloc(sizeof(swapData));
    data->db = db;
    if (key) incrRefCount(key);
    data->key = key;
    if (value) incrRefCount(value);
    data->value = value;
    if (dirty_subkeys) incrRefCount(dirty_subkeys);
    data->dirty_subkeys = dirty_subkeys;
    return data;
}

int swapDataAlreadySetup(swapData *data) {
    return data->type != NULL;
}

int swapDataMayContainSubkey(swapData *data, int thd, robj *subkey) {
    /* To avoid lock, only main thread access absent cache. */
    if (thd != SWAP_ANA_THD_MAIN) return 1;
    return coldFilterMayContainSubkey(data->db->cold_filter,data->key->ptr,subkey->ptr);
}

void swapDataMarkPropagateExpire(swapData *data) {
    data->propagate_expire = 1;
}

int swapDataMarkedPropagateExpire(swapData *data) {
    return data->propagate_expire;
}

static int swapDataExpiredAndShouldDelete(swapData *data) {
    if (!timestampIsExpired(data->expire)) return 0;
    if (server.masterhost != NULL) return 0;
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;
    return 1;
}

int swapDataKeyRequestFinished(swapData *data) {
    if (data->propagate_expire) {
        deleteExpiredKeyAndPropagate(data->db,data->key);
    }

    if (data->set_dirty) {
        dbSetDirty(data->db,data->key);
    }

    if (data->set_dirty_meta) {
        dbSetMetaDirty(data->db,data->key);
    }

    if (data->persistence_deleted) {
        dbDeleteMeta(data->db, data->key);
    }
    return 0;
}

/* Main/swap-thread: analyze data and command intention & request to decide
 * final swap intention. e.g. command might want SWAP_IN but data not
 * evicted, then intention is decided as NOP. */
int swapDataAna(swapData *d, int thd, struct keyRequest *key_request,
        int *intention, uint32_t *intention_flags, void *datactx) {
    int retval = 0;

    serverAssert(swapDataAlreadySetup(d));

    if (swapDataMarkedPropagateExpire(d)) {
        key_request->cmd_intention = SWAP_DEL;
        key_request->cmd_intention_flags = 0;
    }

    if (key_request->cmd_intention == SWAP_DEL && d->value && !d->value->persistent) {
        // no persistent data, skip del
        key_request->cmd_intention = SWAP_NOP;
        key_request->cmd_intention_flags = 0;
    }

    if (d->type->swapAna) {
        if (!(key_request->cmd_flags & CMD_SWAP_DATATYPE_KEYSPACE
            || key_request->cmd_flags & d->type->cmd_swap_flags))  {
            return SWAP_ERR_DATA_WRONG_TYPE_ERROR;
        }
        retval = d->type->swapAna(d,thd,key_request,intention,
                intention_flags,datactx);

        if ((*intention_flags & SWAP_EXEC_IN_DEL) &&
                key_request->type == KEYREQUEST_TYPE_SUBKEY &&
                key_request->b.num_subkeys > 0 &&
                server.swap_dirty_subkeys_enabled) {
            /* commands with EXEC_IN_DEL flags for subkeys:
             *  - HDEL/SREM: meta will be set dirty when swap finished and
             *    persisted later, but subkeys in rocksdb and memory will
             *    not diverge.
             *  - ZADD/ZINCRBY/GEOADD...: meta will be set dirty when swap
             *    finished, subkeys will be flag dirty when call() and then
             *    persisted later.
             * set meta dirty is sufficient for both kind.
             */
            d->set_dirty_meta = 1;
        } else if ((*intention_flags & SWAP_FIN_DEL_SKIP) ||
                (*intention_flags & SWAP_EXEC_IN_DEL)) {
            /* rocksdb and memory will diverge when swap finish. */
            d->set_dirty = 1;
        }
    }

    return retval;
}

inline int swapDataSwapAnaAction(swapData *d, int intention, void *datactx, int *action) {
    if (d->type->swapAnaAction)
        return d->type->swapAnaAction(d, intention, datactx, action);
    else
        return 0;
}

/* Swap-thread: decide how to encode keys by data and intention. */
inline int swapDataEncodeKeys(swapData *d, int intention, void *datactx,
        int *numkeys, int **cfs, sds **rawkeys) {
    if (d->type->encodeKeys)
        return d->type->encodeKeys(d,intention,datactx,numkeys,cfs,rawkeys);
    else
        return 0;
}

/* Swap-thread: decode how to encode val/subval by data and intention.
 * dataactx can be used store context of which subvals are encoded. */
inline int swapDataEncodeData(swapData *d, int intention, void *datactx,
        int *numkeys, int **cfs, sds **rawkeys, sds **rawvals) {
    if (d->type->encodeData)
        return d->type->encodeData(d,intention,datactx,numkeys,cfs,rawkeys,rawvals);
    else
        return 0;
}

inline int swapDataEncodeRange(struct swapData *d, int intention, void *datactx,
        int *limit, uint32_t *flags, int *pcf, sds *start, sds *end) {
    if (d->type->encodeRange)
        return d->type->encodeRange(d,intention,datactx,limit,flags,pcf,start,end);
    else
        return 0;
}

/* Swap-thread: decode val/subval from rawvalss returned by rocksdb. */
inline int swapDataDecodeData(swapData *d, int num, int *cfs, sds *rawkeys,
        sds *rawvals, void **decoded) {
    if (d->type->decodeData)
        return d->type->decodeData(d,num,cfs,rawkeys,rawvals,decoded);
    else
        return 0;
}

/* Main-thread: swap in created or merged result into keyspace. */
inline int swapDataSwapIn(swapData *d, void *result, void *datactx) {
    if (d->type->swapIn)
        return d->type->swapIn(d,result,datactx);
    else
        return 0;
}

/* Main-thread: swap out data out of keyspace. */
inline int swapDataSwapOut(swapData *d, void *datactx, int *totally_out) {
    if (d->type->swapOut)
        return d->type->swapOut(d, datactx, totally_out);
    else
        return 0;
}

/* Main-thread: swap del data out of keyspace. */
inline int swapDataSwapDel(swapData *d, void *datactx, int async) {
    if (d->type->swapDel)
        return d->type->swapDel(d, datactx, async);
    else
        return 0;
}

/* Swap-thread: prepare robj to be merged.
 * - create new object: return newly created object.
 * - merge fields into robj: subvals merged into db.value, returns NULL */
inline void *swapDataCreateOrMergeObject(swapData *d, void *decoded,
        void *datactx) {
    if (d->type->createOrMergeObject)
        return d->type->createOrMergeObject(d,decoded,datactx);
    else
        return NULL;
}

/* Swap-thread: clean data.value. */
inline int swapDataCleanObject(swapData *d, void *datactx) {
    if (d->type->cleanObject)
        return d->type->cleanObject(d,datactx);
    else
        return 0;
}

inline int swapDataBeforeCall(swapData *d, client *c, void *datactx) {
    if (d->type->beforeCall)
        return d->type->beforeCall(d,c,datactx);
    else
        return 0;
}

inline int swapDataMergedIsHot(swapData *d, void *result, void *datactx) {
    return d->type->mergedIsHot(d,result,datactx);
}

int swapDataObjectMergedIsHot(swapData *d, void *result, void *datactx) {
    objectMeta *object_meta = swapDataObjectMeta(d);
    robj *value = swapDataIsCold(d) ? result : d->value;
    UNUSED(datactx);
    return keyIsHot(object_meta,value);
}

swapDataAbsentSubkey *swapDataAbsentSubkeyNew() {
    swapDataAbsentSubkey *absent = zmalloc(sizeof(swapDataAbsentSubkey));
    absent->subkeys = zmalloc(sizeof(sds)*SWAP_DATA_ABSENT_SUBKEYS_INIT);
    absent->count = 0;
    absent->capacity = SWAP_DATA_ABSENT_SUBKEYS_INIT;
    return absent;
}

void swapDataAbsentSubkeyPush(swapDataAbsentSubkey *absent, sds subkey) {
    if (absent->count == absent->capacity) {
        if (absent->capacity >= SWAP_DATA_ABSENT_SUBKEYS_LINEAR)
            absent->capacity += SWAP_DATA_ABSENT_SUBKEYS_LINEAR;
        else
            absent->capacity *= 2;

        absent->subkeys = zrealloc(absent->subkeys,absent->capacity*sizeof(sds));
    }

    absent->subkeys[absent->count++] = subkey;
}

void swapDataAbsentSubkeyFree(swapDataAbsentSubkey *absent) {
    if (absent == NULL) return;
    if (absent->subkeys) {
        for (size_t i = 0; i < absent->count; i++) {
            sdsfree(absent->subkeys[i]);
        }
        zfree(absent->subkeys);
        absent->subkeys = NULL;
    }
    zfree(absent);
}

inline void swapDataFree(swapData *d, void *datactx) {
    /* free extend */
    if (d->type && d->type->free) d->type->free(d,datactx);
    /* free base */
    if (d->cold_meta) freeObjectMeta(d->cold_meta);
    if (d->new_meta) freeObjectMeta(d->new_meta);
    if (d->key) decrRefCount(d->key);
    if (d->value) decrRefCount(d->value);
    if (d->dirty_subkeys) decrRefCount(d->dirty_subkeys);
    if (d->absent) swapDataAbsentSubkeyFree(d->absent);
    zfree(d);
}

inline void *swapDataGetObjectMetaAux(swapData *data, void *datactx) {
    if (data->type->getObjectMetaAux)
        return data->type->getObjectMetaAux(data,datactx);
    else
        return NULL;
}

sds swapDataEncodeMetaVal(swapData *d, void *datactx) {
    sds extend = NULL, encoded;
    objectMeta *object_meta = swapDataObjectMeta(d);
    uint64_t version = object_meta ? object_meta->version : SWAP_VERSION_ZERO;
    if (d->omtype->encodeObjectMeta) {
        void *omaux = swapDataGetObjectMetaAux(d,datactx);
        extend = d->omtype->encodeObjectMeta(object_meta,omaux);
    }
    encoded = rocksEncodeMetaVal(d->object_type,d->expire,version,extend);
    sdsfree(extend);
    return encoded;
}

sds swapDataEncodeMetaKey(swapData *d) {
    return rocksEncodeMetaKey(d->db,(sds)d->key->ptr);
}

int swapDataSetupMeta(swapData *d, int object_type, long long expire,
        void **datactx) {
    int retval;
    serverAssert(d->type == NULL);

    d->expire = expire;
    d->object_type = object_type;

    if (!swapDataMarkedPropagateExpire(d) &&
            swapDataExpiredAndShouldDelete(d)) {
        swapDataMarkPropagateExpire(d);
    }

    if (datactx) *datactx = NULL;

    switch (d->object_type) {
    case OBJ_STRING:
        retval = swapDataSetupWholeKey(d,datactx);
        break;
    case OBJ_HASH:
        retval = swapDataSetupHash(d,datactx);
        break;
    case OBJ_SET:
        retval = swapDataSetupSet(d,datactx);
        break;
    case OBJ_ZSET:
        retval = swapDataSetupZSet(d, datactx);
        break;
    case OBJ_LIST:
        retval = swapDataSetupList(d, datactx);
        break;
    case OBJ_STREAM:
        retval = SWAP_ERR_SETUP_UNSUPPORTED;
        break;
    default:
        retval = SWAP_ERR_SETUP_FAIL;
        break;
    }
    return retval;
}

int swapDataDecodeAndSetupMeta(swapData *d, sds rawval, void **datactx) {
    const char *extend;
    size_t extend_len;
    int retval = 0, object_type;
    long long expire;
    uint64_t version;
    objectMeta *object_meta = NULL;

    retval = rocksDecodeMetaVal(rawval,sdslen(rawval),&object_type,&expire,
            &version,&extend,&extend_len);
    if (retval) return retval;

    retval = swapDataSetupMeta(d,object_type,expire,datactx);
    if (retval) return retval;

    retval = buildObjectMeta(object_type,version,extend,extend_len,&object_meta);
    if (retval) return SWAP_ERR_DATA_DECODE_META_FAILED;

    swapDataSetColdObjectMeta(d,object_meta/*moved*/);
    return retval;
}

void swapDataTurnWarmOrHot(swapData *data) {
    if (data->expire != -1) {
        setExpire(NULL,data->db,data->key,data->expire);
    }
    data->db->cold_keys--;
    coldFilterDeleteKey(data->db->cold_filter,data->key->ptr);
}

void swapDataTurnCold(swapData *data) {
    coldFilterAddKey(data->db->cold_filter,data->key->ptr);
    data->db->cold_keys++;
}

void swapDataTurnDeleted(swapData *data, int del_skip) {
    if (swapDataIsCold(data)) {
        data->db->cold_keys--;
        coldFilterDeleteKey(data->db->cold_filter,data->key->ptr);
    } else {
        /* rocks-meta already deleted, only need to delete object_meta
         * from keyspace. */
        if (!del_skip && data->expire != -1) {
            removeExpire(data->db,data->key);
        }
    }
}

/* Save absent subkeys when in swap thread, which will be merged into cold
 * filter when callback in main thread. */
void swapDataRetainAbsentSubkeys(swapData *data, int num, int *cfs,
        sds *rawkeys, sds *rawvals) {
    uint64_t version = swapDataObjectVersion(data);

    /* string dont have subkey */
    if (version <= 0) return;

    for (int i = 0; i < num; i++) {
        int dbid;
        const char *keystr, *subkeystr;
        size_t klen, slen;
        uint64_t subkey_version;
        sds absent_subkey;

        if (cfs[i] != DATA_CF) continue;
        if (rawvals[i] != NULL) continue;
        if (rocksDecodeDataKey(rawkeys[i],sdslen(rawkeys[i]),
                    &dbid,&keystr,&klen,&subkey_version,&subkeystr,&slen) < 0) {
            continue;
        }
        if (subkey_version != version) continue;

        absent_subkey = sdsnewlen(subkeystr,slen);

        if (data->absent == NULL)
            data->absent = swapDataAbsentSubkeyNew();

        swapDataAbsentSubkeyPush(data->absent,absent_subkey);
    }
}

void swapDataMergeAbsentSubkey(swapData *data) {
    swapDataAbsentSubkey *absent = data->absent;
    if (absent == NULL) return;
    for (size_t i = 0; i < absent->count; i++) {
        sds key = data->key->ptr;
        sds subkey = absent->subkeys[i];
        coldFilterSubkeyNotFound(data->db->cold_filter,key,subkey);
    }
}

#ifdef REDIS_TEST
int swapDataTest(int argc, char *argv[], int accurate) {
    int error = 0, intention;
    uint32_t intention_flags;
    swapData *data;
    void *datactx;
    redisDb *db;
    robj *key1, *val1;

    UNUSED(argc), UNUSED(argv), UNUSED(accurate);

    TEST("swapdata - init") {
        initTestRedisServer();
        db = server.db + 0;
        key1 = createRawStringObject("key1",4);
        val1 = createRawStringObject("val1",4);
    }

    TEST("swapdata - propagate_expire") {
        keyRequest key_request_, *key_request = &key_request_;
        key_request->level = REQUEST_LEVEL_KEY;
        key_request->key = key1;
        key_request->b.subkeys = NULL;
        key_request->cmd_intention = SWAP_IN;
        key_request->cmd_intention_flags = 0;
        key_request->dbid = 0;

        data = createSwapData(db,key1,NULL,NULL);
        test_assert(!swapDataAlreadySetup(data));
        swapDataSetupMeta(data,OBJ_STRING,0/*expired*/,&datactx);
        test_assert(swapDataAlreadySetup(data));
        swapDataAna(data,0,key_request,&intention,&intention_flags,datactx);
        test_assert(intention == SWAP_DEL);
        test_assert(intention_flags == 0);
        test_assert(data->propagate_expire == 1);

        swapDataFree(data,datactx);
    }

    TEST("swapdata - set_dirty") {
        keyRequest key_request_, *key_request = &key_request_;
        key_request->level = REQUEST_LEVEL_KEY;
        key_request->key = key1;
        key_request->b.subkeys = NULL;
        key_request->cmd_intention = SWAP_IN;
        key_request->cmd_intention_flags = SWAP_IN_DEL;
        key_request->dbid = 0;

        data = createSwapData(db,key1,val1,NULL);
        swapDataSetupMeta(data,OBJ_STRING,-1,&datactx);
        swapDataAna(data,0,key_request,&intention,&intention_flags,datactx);
        test_assert(intention == SWAP_DEL);
        test_assert(intention_flags == SWAP_FIN_DEL_SKIP);
        test_assert(data->set_dirty == 1);

        swapDataFree(data,datactx);
    }

    TEST("swapdata - deinit") {
        decrRefCount(key1), decrRefCount(val1);
    }

    return error;
}

#endif
