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
#include "server.h"

static void createFakeWholeKeyForDeleteIfCold(swapData *data) {
	if (swapDataIsCold(data)) {
        /* empty whole key allowed */
        dbAdd(data->db,data->key,createStringObject("", 0));
	}
}

/* ------------------- whole key object meta ----------------------------- */
int wholeKeyIsHot(objectMeta *om, robj *value) {
    UNUSED(om);
    return value != NULL;
}

objectMetaType wholekeyObjectMetaType = {
    .encodeObjectMeta = NULL,
    .decodeObjectMeta = NULL,
    .objectIsHot = wholeKeyIsHot,
};

/* ------------------- whole key swap data ----------------------------- */
int wholeKeySwapAna(swapData *data, int thd, struct keyRequest *req,
        int *intention, uint32_t *intention_flags, void *datactx_) {
    /* for string type, store ctx_flag in struct swapData's `void *extends[2];` */
    long *datactx = datactx_;
    int cmd_intention = req->cmd_intention;
    uint32_t cmd_intention_flags = req->cmd_intention_flags;

    switch(cmd_intention) {
    case SWAP_NOP:
        *intention = SWAP_NOP;
        *intention_flags = 0;
        break;
    case SWAP_IN:
        if (!data->value) {
            if (cmd_intention_flags & SWAP_IN_DEL) {
                *intention = SWAP_IN;
                *intention_flags = SWAP_EXEC_IN_DEL;
            } else if (cmd_intention_flags & SWAP_IN_DEL_MOCK_VALUE) {
                /* DEL/UNLINK: Lazy delete current key. */
                *datactx |= BIG_DATA_CTX_FLAG_MOCK_VALUE;
                *intention = SWAP_DEL;
                *intention_flags = SWAP_FIN_DEL_SKIP;
            } else {
                *intention = SWAP_IN;
                *intention_flags = 0;
            }
        } else if (data->value) {
            if ((cmd_intention_flags & SWAP_IN_DEL) ||
                    (cmd_intention_flags & SWAP_IN_DEL_MOCK_VALUE) ||
                    cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                *intention = SWAP_DEL;
                *intention_flags = SWAP_FIN_DEL_SKIP;
            } else {
                *intention = SWAP_NOP;
                *intention_flags = 0;
            }
        } else {
            *intention = SWAP_NOP;
            *intention_flags = 0;
        }
        break;
    case SWAP_OUT:
        if (data->value) {
            /* we can always keep data and clear dirty after persist string */
            int keep_data = swapDataPersistKeepData(data,cmd_intention_flags,1);

            if (objectIsDirty(data->value)) {
                *intention = SWAP_OUT;
                *intention_flags = keep_data ? SWAP_EXEC_OUT_KEEP_DATA : 0;
            } else {
                serverAssert(thd == SWAP_ANA_THD_MAIN);
                /* Not dirty: swapout right away without swap. */
                if (!keep_data) swapDataTurnCold(data);
                swapDataSwapOut(data,NULL,keep_data,NULL);
                *intention = SWAP_NOP;
                *intention_flags = 0;
            }
        } else {
            *intention = SWAP_NOP;
            *intention_flags = 0;
        }
        break;
    case SWAP_DEL:
        *intention = SWAP_DEL;
        *intention_flags = 0;
        break;
    default:
        break;
    }

    return 0;
}

int wholeKeySwapAnaAction(swapData *data, int intention, void *datactx_, int *action) {
    UNUSED(data), UNUSED(datactx_);
    switch (intention) {
        case SWAP_IN:
            *action = ROCKS_GET;
            break;
        case SWAP_DEL:
            *action = ROCKS_DEL;
            break;
        case SWAP_OUT:
            *action = ROCKS_PUT;
            break;
        default:
            *action = ROCKS_NOP;
            return SWAP_ERR_DATA_FAIL;
    }
    return 0;
}

int wholeKeyEncodeKeys(swapData *data, int intention, void *datactx,
        int *numkeys, int **pcfs, sds **prawkeys) {
    sds *rawkeys = zmalloc(sizeof(sds));
    int *cfs = zmalloc(sizeof(int));

    UNUSED(datactx);
    serverAssert(intention == SWAP_IN || intention == SWAP_DEL);
    rawkeys[0] = rocksEncodeDataKey(data->db,data->key->ptr,SWAP_VERSION_ZERO,NULL);
    cfs[0] = DATA_CF;
    *numkeys = 1;
    *prawkeys = rawkeys;
    *pcfs = cfs;

    return 0;
}

static sds wholeKeyEncodeDataKey(swapData *data) {
    return data->key ? rocksEncodeDataKey(data->db,data->key->ptr,SWAP_VERSION_ZERO,NULL) : NULL;
}

static sds wholeKeyEncodeDataVal(swapData *data) {
    return data->value ? rocksEncodeValRdb(data->value) : NULL;
}

int wholeKeyEncodeData(swapData *data, int intention, void *datactx,
        int *numkeys, int **pcfs, sds **prawkeys, sds **prawvals) {
    UNUSED(datactx);
    serverAssert(intention == SWAP_OUT);
    sds *rawkeys = zmalloc(sizeof(sds));
    sds *rawvals = zmalloc(sizeof(sds));
    int *cfs = zmalloc(sizeof(int));
    rawkeys[0] = wholeKeyEncodeDataKey(data);
    rawvals[0] = wholeKeyEncodeDataVal(data);
    cfs[0] = DATA_CF;
    *numkeys = 1;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    *pcfs = cfs;
    return 0;
}

/* decoded move to exec module */
int wholeKeyDecodeData(swapData *data, int num, int *cfs, sds *rawkeys,
        sds *rawvals, void **pdecoded) {
    serverAssert(num == 1);
    UNUSED(data);
    UNUSED(rawkeys);
    UNUSED(cfs);
    sds rawval = rawvals[0];
    *pdecoded = rocksDecodeValRdb(rawval);
    return 0;
}

/* If maxmemory policy is not LRU/LFU, rdbLoadObject might return shared
 * object, but swap needs individual object to track dirty/evict flags. */
robj *dupSharedObject(robj *o) {
    switch(o->type) {
    case OBJ_STRING:
        return dupStringObject(o);
    case OBJ_HASH:
    case OBJ_LIST:
    case OBJ_SET:
    case OBJ_ZSET:
    default:
        return NULL;
    }
}

static robj *createSwapInObject(MOVE robj *newval) {
    robj *swapin = newval;
    serverAssert(newval);
    /* Copy swapin object before modifing If newval is shared object. */
    if (newval->refcount == OBJ_SHARED_REFCOUNT)
        swapin = dupSharedObject(newval);
    clearObjectDirty(swapin);
    clearObjectPersistKeep(swapin);
    return swapin;
}

int wholeKeySwapIn(swapData *data, MOVE void *result, void *datactx) {
    UNUSED(datactx);
    robj *swapin;
    serverAssert(data->value == NULL);
    swapin = createSwapInObject(result);
    /* mark persistent after data swap in without
     * persistence deleted, or mark non-persistent else */
    overwriteObjectPersistent(swapin,!data->persistence_deleted);
    dbAdd(data->db,data->key,swapin);
    return 0;
}

int wholeKeySwapOut(swapData *data, void *datactx, int keep_data, int *totally_out) {
    UNUSED(datactx);
    redisDb *db = data->db;
    robj *key = data->key;
    if (keep_data) {
        setObjectPersistent(data->value);
        clearObjectDataDirty(data->value);
        if (totally_out) *totally_out = 0;
    } else {
        if (dictSize(db->dict) > 0) dbDelete(db, key);
        if (totally_out) *totally_out = 1;
    }
    return 0;
}

int wholeKeySwapDel(swapData *data, void *datactx_, int async) {
    redisDb *db = data->db;
    robj *key = data->key;

    long *datactx = datactx_;
    if (*datactx & BIG_DATA_CTX_FLAG_MOCK_VALUE) {
        createFakeWholeKeyForDeleteIfCold(data);
    }

    if (async) return 0;
    if (data->value) dictDelete(db->dict,key->ptr);
    return 0;
}

/* decoded moved back by exec to wholekey then moved to exec again. */
void *wholeKeyCreateOrMergeObject(swapData *data, void *decoded, void *datactx) {
    UNUSED(data);
    UNUSED(datactx);
    serverAssert(decoded);
    return decoded;
}

static void tryTransStringToBitmap(redisDb *db, robj *key) {
    if (!server.swap_bitmap_subkeys_enabled) return;
    if (bitmapSetObjectMarkerIfNotExist(db,key) == 1) {
        atomicIncr(server.swap_string_switched_to_bitmap_count, 1);
    }
}

int wholeKeyBeforeCall(swapData *data, keyRequest *key_request,
        client *c, void *datactx)  {
    UNUSED(data), UNUSED(c), UNUSED(datactx);
    robj *o = lookupKey(data->db, data->key, LOOKUP_NOTOUCH);
    if ((key_request->cmd_flags & CMD_SWAP_DATATYPE_BITMAP) && o) {
        tryTransStringToBitmap(data->db,data->key);
    }
    return 0;
}

swapDataType wholeKeySwapDataType = {
    .name = "wholekey",
    .cmd_swap_flags = CMD_SWAP_DATATYPE_STRING,
    .swapAna = wholeKeySwapAna,
    .swapAnaAction = wholeKeySwapAnaAction,
    .encodeKeys = wholeKeyEncodeKeys,
    .encodeData = wholeKeyEncodeData,
    .decodeData = wholeKeyDecodeData,
    .encodeRange = NULL,
    .swapIn = wholeKeySwapIn,
    .swapOut = wholeKeySwapOut,
    .swapDel = wholeKeySwapDel,
    .createOrMergeObject = wholeKeyCreateOrMergeObject,
    .cleanObject = NULL,
    .beforeCall = wholeKeyBeforeCall,
    .free = NULL,
    .rocksDel = NULL,
    .mergedIsHot = wholeKeyMergedIsHot,
};

int swapDataSetupWholeKey(swapData *d, OUT void **pdatactx) {
    d->type = &wholeKeySwapDataType;
    d->omtype = &wholekeyObjectMetaType;
    /* for string type, store ctx_flag in struct swapData's `void *extends[2];` */
    long *datactx = (long*)d->extends;
    *datactx = BIG_DATA_CTX_FLAG_NONE;
    *pdatactx = d->extends;
    return 0;
}

/* ------------------- whole key rdb save -------------------------------- */
int wholekeySave(rdbKeySaveData *keydata, rio *rdb, decodedData *decoded) {
    robj keyobj = {0};

    serverAssert(decoded->cf == DATA_CF);
    serverAssert((NULL == decoded->subkey));
    initStaticStringObject(keyobj,decoded->key);

    if (rdbSaveKeyHeader(rdb,&keyobj,&keyobj,
                RDB_TYPE_STRING,
                keydata->expire) == -1) {
        return -1;
    }

    if (rdbWriteRaw(rdb,decoded->rdbraw,
                sdslen(decoded->rdbraw)) == -1) {
        return -1;
    }

    return 0;
}

rdbKeySaveType wholekeyRdbSaveType = {
    .save_start = NULL,
    .save_hot_ext = NULL,
    .save = wholekeySave,
    .save_end = NULL,
    .save_deinit = NULL,
};

void wholeKeySaveInit(rdbKeySaveData *keydata) {
    keydata->type = &wholekeyRdbSaveType;
    keydata->omtype = &wholekeyObjectMetaType;
}

/* ------------------- whole key rdb load -------------------------------- */
void wholekeyLoadStart(struct rdbKeyLoadData *keydata, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    UNUSED(rdb);
    *cf = META_CF;
    *rawkey = rocksEncodeMetaKey(keydata->db,keydata->key);
    *rawval = rocksEncodeMetaVal(keydata->swap_type,keydata->expire,SWAP_VERSION_ZERO,NULL);
    *error = 0;
}

int wholekeyLoad(struct rdbKeyLoadData *keydata, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    *error = RDB_LOAD_ERR_OTHER;
    int rdbtype = keydata->rdbtype;
    sds verbatim = NULL, key = keydata->key;
    redisDb *db = keydata->db;

    verbatim = rdbVerbatimNew((unsigned char)rdbtype);
    if (rdbLoadStringVerbatim(rdb,&verbatim)) goto err;

    *error = 0;
    *cf = DATA_CF;
    *rawkey = rocksEncodeDataKey(db,key,SWAP_VERSION_ZERO,NULL);
    *rawval = verbatim;
    return 0;

err:
    if (verbatim) sdsfree(verbatim);
    return 0;
}

rdbKeyLoadType wholekeyLoadType = {
    .load_start = wholekeyLoadStart,
    .load = wholekeyLoad,
    .load_end = NULL,
    .load_deinit = NULL,
};

void wholeKeyLoadInit(rdbKeyLoadData *keydata) {
    keydata->type = &wholekeyLoadType;
    keydata->omtype = &wholekeyObjectMetaType;
    keydata->swap_type = SWAP_TYPE_STRING;
}


#ifdef REDIS_TEST
#include <stdio.h>
#include <limits.h>
#include <assert.h>

#define FREE_SDSARRAY(sdss,n) do {    \
    for (int i = 0; i < n; i++) sdsfree(sdss[i]);    \
    zfree(sdss), sdss = NULL; \
} while (0)

swapData *createWholeKeySwapDataWithExpire(redisDb *db, robj *key, robj *value,
        long long expire, void **datactx) {
    swapData *data = createSwapData(db,key,value,NULL);
    swapDataSetupMeta(data,OBJ_STRING,expire,datactx);
    return data;
}

swapData *createWholeKeySwapData(redisDb *db, robj *key, robj *value, void **datactx) {
    return createWholeKeySwapDataWithExpire(db,key,value,-1,datactx);
}

int wholeKeySwapAna_(swapData *data_,
        int cmd_intention, uint32_t cmd_intention_flags,
        int *intention, uint32_t *intention_flags, void *datactx) {
    int retval;
    struct keyRequest req_, *req = &req_;
    req->level = REQUEST_LEVEL_KEY;
    req->b.num_subkeys = 0;
    req->key = createStringObject("key1",4);
    req->b.subkeys = NULL;
    req->cmd_flags = CMD_SWAP_DATATYPE_STRING;
    req->cmd_intention = cmd_intention;
    req->cmd_intention_flags = cmd_intention_flags;
    retval = wholeKeySwapAna(data_,0,req,intention,intention_flags,datactx);
    decrRefCount(req->key);
    return retval;
}

int swapDataWholeKeyTest(int argc, char **argv, int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    initTestRedisServer();
    redisDb* db = server.db + 0;
    int error = 0;

    TEST("wholeKey - SwapAna hot key") {
        void* ctx = NULL;
        robj* value  = createRawStringObject("value", 5);
        swapData* data = createWholeKeySwapData(db, NULL, value, &ctx);
        int intention;
        uint32_t intention_flags;
        wholeKeySwapAna_(data, SWAP_NOP, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_NOP);
        wholeKeySwapAna_(data, SWAP_IN, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_NOP);
        wholeKeySwapAna_(data, SWAP_IN, SWAP_IN_DEL, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_DEL);
        test_assert(intention_flags == SWAP_FIN_DEL_SKIP);
        wholeKeySwapAna_(data, SWAP_OUT, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_OUT);
        test_assert(intention_flags == 0);
        wholeKeySwapAna_(data, SWAP_DEL, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_DEL);
        swapDataFree(data, ctx);
    }

    TEST("wholeKey - SwapAna cold key") {
        void* ctx = NULL;
        swapData* data = createWholeKeySwapData(db, NULL, NULL, &ctx);
        int intention;
        uint32_t intention_flags;
        wholeKeySwapAna_(data, SWAP_NOP, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_NOP);
        wholeKeySwapAna_(data, SWAP_IN, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_IN);
        wholeKeySwapAna_(data, SWAP_IN, SWAP_IN_DEL, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_IN);
        test_assert(intention_flags == SWAP_EXEC_IN_DEL);
        wholeKeySwapAna_(data, SWAP_OUT, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_NOP);
        wholeKeySwapAna_(data, SWAP_DEL, 0, &intention, &intention_flags, ctx);
        test_assert(intention == SWAP_DEL);
        swapDataFree(data, ctx);
    }

    TEST("wholeKey - EncodeKeys (hot)") {
        void* ctx = NULL;
        robj* key = createRawStringObject("key", 3);
        robj* value  = createRawStringObject("value", 5);
        swapData* data = createWholeKeySwapData(db, key, value, &ctx);
        int numkeys, action;
        int *cfs;
        sds *rawkeys = NULL;
        wholeKeySwapAnaAction(data, SWAP_IN, ctx, &action);
        int result = wholeKeyEncodeKeys(data, SWAP_IN, ctx, &numkeys, &cfs, &rawkeys);
        test_assert(ROCKS_GET == action);
        test_assert(numkeys == 1);
        test_assert(cfs[0] == DATA_CF);
#if (BYTE_ORDER == LITTLE_ENDIAN)
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x03\x00\x00\x00key", 11));
#else
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x00\x00\x00\x03key", 11));
#endif
        test_assert(result == C_OK);
        FREE_SDSARRAY(rawkeys,1);
        wholeKeySwapAnaAction(data, SWAP_DEL, ctx, &action);
        result = wholeKeyEncodeKeys(data, SWAP_DEL, ctx, &numkeys, &cfs, &rawkeys);
        test_assert(ROCKS_DEL == action);
        test_assert(numkeys == 1);
        test_assert(cfs[0] == DATA_CF);
#if (BYTE_ORDER == LITTLE_ENDIAN)
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x03\x00\x00\x00key", 11));
#else
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x00\x00\x00\x03key", 11));
#endif
        test_assert(result == C_OK);
        FREE_SDSARRAY(rawkeys,1);
        swapDataFree(data, ctx);
    }

    TEST("wholeKey - EncodeKeys (cold)") {
        void* ctx = NULL;
        robj* key = createRawStringObject("key", 3);
        swapData* data = createWholeKeySwapData(db, key, NULL, &ctx);
        int numkeys, action;
        sds *rawkeys = NULL;
        int *cfs;
        wholeKeySwapAnaAction(data, SWAP_IN, ctx, &action);
        int result = wholeKeyEncodeKeys(data, SWAP_IN, ctx, &numkeys, &cfs, &rawkeys);
        test_assert(ROCKS_GET == action);
        test_assert(numkeys == 1);
        test_assert(cfs[0] == DATA_CF);
#if (BYTE_ORDER == LITTLE_ENDIAN)
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x03\x00\x00\x00key", 11));
#else
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x00\x00\x00\x03key", 11));
#endif
        test_assert(result == C_OK);
        FREE_SDSARRAY(rawkeys,1);
        wholeKeySwapAnaAction(data, SWAP_DEL, ctx, &action);
        result = wholeKeyEncodeKeys(data, SWAP_DEL, ctx, &numkeys, &cfs, &rawkeys);
        test_assert(ROCKS_DEL == action);
        test_assert(numkeys == 1);
        test_assert(cfs[0] == DATA_CF);
#if (BYTE_ORDER == LITTLE_ENDIAN)
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x03\x00\x00\x00key", 11));
#else
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x00\x00\x00\x03key", 11));
#endif
        test_assert(result == C_OK);
        FREE_SDSARRAY(rawkeys,1);
        swapDataFree(data, ctx);
    }

    TEST("wholeKey - EncodeData + DecodeData") {
        void* wholekey_ctx = NULL;
        robj* key = createRawStringObject("key", 3);
        robj* value  = createRawStringObject("value", 5);
        swapData* data = createWholeKeySwapData(db, key, value, &wholekey_ctx);
        int numkeys, action;
        sds *rawkeys = NULL, *rawvals = NULL;
        int *cfs;
        wholeKeySwapAnaAction(data, SWAP_OUT, wholekey_ctx, &action);
        int result = wholeKeyEncodeData(data, SWAP_OUT, wholekey_ctx, &numkeys, &cfs, &rawkeys, &rawvals);
        test_assert(result == C_OK);
        test_assert(ROCKS_PUT == action);
        test_assert(numkeys == 1);
        test_assert(cfs[0] == DATA_CF);
#if (BYTE_ORDER == LITTLE_ENDIAN)
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x03\x00\x00\x00key", 11));
#else
        test_assert(!memcmp(rawkeys[0], "\x00\x00\x00\x00\x00\x00\x00\x03key", 11));
#endif

        void* decoded;
        result = wholeKeyDecodeData(data, numkeys, cfs, rawkeys, rawvals, &decoded);
        test_assert(result == C_OK);
        test_assert(strcmp(((robj*)decoded)->ptr ,"value") == 0);
        swapDataFree(data, wholekey_ctx);
    }

    TEST("wholeKey - swapIn cold non-volatie key") {
        void* wholekey_ctx = NULL;
        robj *decoded;
        robj* key = createRawStringObject("key", 3);
        robj* val;
        swapData* data = createWholeKeySwapData(db, key, NULL, &wholekey_ctx);
        decoded = createRawStringObject("value", 5);
        test_assert(wholeKeySwapIn(data, decoded, NULL) == 0);
        test_assert(dictFind(db->dict, key->ptr) != NULL);
        val = dictGetVal(dictFind(db->dict, key->ptr));
        test_assert(val->persistent);
        decoded = NULL;
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();
    }

    TEST("wholekey - swapIn cold volatile key") {
        robj *key = createRawStringObject("key", 3);
        robj* val;
        robj *decoded = NULL;
        void* wholekey_ctx = NULL;
        test_assert(dictFind(db->dict, key->ptr) == NULL);

        swapData* data = createWholeKeySwapDataWithExpire(db, key, NULL, 1000000, &wholekey_ctx);
        decoded = createRawStringObject("value", 5);
        test_assert(wholeKeySwapIn(data,decoded,NULL) == 0);
        test_assert(dictFind(db->dict, key->ptr) != NULL);
        val = dictGetVal(dictFind(db->dict, key->ptr));
        test_assert(val->persistent);
        decoded = NULL;
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();

    }

    TEST("wholeKey - swapout hot non-volatile key") {
        robj* key = createRawStringObject("key", 3);
        robj* value  = createRawStringObject("value", 5);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        dbAdd(db, key, value);
        test_assert(dictFind(db->dict, key->ptr) != NULL);
        void* wholekey_ctx = NULL;

        swapData* data = createWholeKeySwapData(db, key, value, &wholekey_ctx);
        test_assert(wholeKeySwapOut(data, NULL, 0, NULL) == 0);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();
    }

    TEST("wholeKey - swapout hot volatile key") {
        robj* key = createRawStringObject("key", 3);
        robj* value  = createRawStringObject("value", 5);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        dbAdd(db, key, value);
        setExpire(NULL, db, key, 1000000);
        test_assert(dictFind(db->dict, key->ptr) != NULL);
        void* wholekey_ctx = NULL;

        swapData* data = createWholeKeySwapDataWithExpire(db, key, value, 1000000, &wholekey_ctx);
        test_assert(wholeKeySwapOut(data, NULL, 0, NULL) == 0);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();
    }

    TEST("wholeKey - swapdelete hot non-volatile key") {
        robj* key = createRawStringObject("key", 3);
        robj* value  = createRawStringObject("value", 5);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        dbAdd(db, key, value);
        test_assert(dictFind(db->dict, key->ptr) != NULL);
        void* wholekey_ctx = NULL;

        swapData* data = createWholeKeySwapData(db, key, value, &wholekey_ctx);
        test_assert(wholeKeySwapDel(data, &wholekey_ctx, 0) == 0);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();
    }

    TEST("wholeKey - swapdelete hot volatile key") {
        robj* key = createRawStringObject("key", 3);
        robj* value  = createRawStringObject("value", 5);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        dbAdd(db, key, value);
        setExpire(NULL, db, key, 1000000);
        test_assert(dictFind(db->dict, key->ptr) != NULL);
        void* wholekey_ctx = NULL;

        swapData* data = createWholeKeySwapDataWithExpire(db, key, value, 1000000, &wholekey_ctx);
        test_assert(wholeKeySwapDel(data, &wholekey_ctx, 0) == 0);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();
    }

    TEST("wholeKey - swapdelete cold key") {
        robj* key = createRawStringObject("key", 3);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        void* wholekey_ctx = NULL;
        swapData* data = createWholeKeySwapData(db, key, NULL, &wholekey_ctx);
        test_assert(wholeKeySwapDel(data, &wholekey_ctx, 0) == 0);
        test_assert(dictFind(db->dict, key->ptr) == NULL);
        swapDataFree(data, wholekey_ctx);
        clearTestRedisDb();
    }

    int rocksDecodeMetaCF(sds rawkey, sds rawval, decodedMeta *decoded);
    int rocksDecodeDataCF(sds rawkey, unsigned char rdbtype, sds rdbraw, decodedData *decoded);

    TEST("wholeKey rdb save & load") {
        int err, feed_cf;
        rio sdsrdb;
        rdbKeySaveData _savedata, *savedata = &_savedata;
        rdbKeyLoadData _loaddata, *loaddata = &_loaddata;
        decodedMeta _dm, *dm = &_dm;
        decodedData _dd, *dd = &_dd;
        sds feed_rawkey, feed_rawval, rdb_key, rdbraw;

        sds key = sdsnew("key");
        robj *val = createStringObject("val",3);
        sds meta_rawkey = rocksEncodeMetaKey(db,key);
        sds meta_rawval = rocksEncodeMetaVal(OBJ_STRING,-1,0,NULL);
        sds data_rawkey = rocksEncodeDataKey(db,key,0,NULL);
        sds data_rawval = rocksEncodeValRdb(val);

        test_assert(!rocksDecodeMetaCF(sdsdup(meta_rawkey),sdsdup(meta_rawval),dm));
        test_assert(dm->expire == -1);
        test_assert(dm->extend == NULL);
        test_assert(!sdscmp(dm->key,key));

        rdbraw = sdsnewlen(data_rawval+1,sdslen(data_rawval)-1);
        test_assert(!rocksDecodeDataCF(sdsdup(data_rawkey),data_rawval[0],rdbraw,dd));
        test_assert(!sdscmp(dd->key,key));
        test_assert(dd->subkey == NULL);
        test_assert(!memcmp(dd->rdbraw,data_rawval+1,sdslen(dd->rdbraw)));

        rioInitWithBuffer(&sdsrdb, sdsempty());
        test_assert(!rdbKeySaveWarmColdInit(savedata,db,(decodedResult*)dm));
        test_assert(!wholekeySave(savedata,&sdsrdb,dd));

        rioInitWithBuffer(&sdsrdb,sdsrdb.io.buffer.ptr);
        /* LFU */
        uint8_t byte;
        test_assert(rdbLoadType(&sdsrdb) == RDB_OPCODE_FREQ);
        test_assert(rioRead(&sdsrdb,&byte,1));
        /* rdbtype */
        test_assert(rdbLoadType(&sdsrdb) == RDB_TYPE_STRING);
        /* key */
        rdb_key = rdbGenericLoadStringObject(&sdsrdb,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(rdb_key, key));

        rdbKeyLoadDataInit(loaddata,RDB_TYPE_STRING,db,rdb_key,-1,1600000000);

        wholekeyLoadStart(loaddata,&sdsrdb,&feed_cf,&feed_rawkey,&feed_rawval,&err);
        test_assert(err == 0);
        test_assert(feed_cf == META_CF);
        test_assert(!sdscmp(feed_rawkey,meta_rawkey));
        test_assert(!sdscmp(feed_rawval,meta_rawval));

        wholekeyLoad(loaddata,&sdsrdb,&feed_cf,&feed_rawkey,&feed_rawval,&err);
        test_assert(err == 0);
        test_assert(feed_cf == DATA_CF);
        test_assert(!sdscmp(feed_rawkey,data_rawkey));
        test_assert(!sdscmp(feed_rawval,data_rawval));
    }

    return error;
}

#endif

