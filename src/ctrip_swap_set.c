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

static sds bigSetEncodeSubkey(sds key, sds subkey) {
    return rocksEncodeSubkey(rocksGetEncType(OBJ_SET,1),key,subkey);
}

static void bigSetEncodeDeleteRange(bigSetSwapData *data, sds *start, sds *end) {
    *start = rocksEncodeSubkey(rocksGetEncType(OBJ_SET,1),data->key->ptr,NULL);
    *end = rocksCalculateNextKey(*start);
    serverAssert(NULL != *end);
}

int bigSetEncodeKeys(swapData *data_, int intention, void *datactx_,
                      int *action, int *numkeys, sds **prawkeys) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    sds *rawkeys = NULL;

    switch (intention) {
        case SWAP_IN:
            if (datactx->num) { /* Swap in specific fields */
                int i;
                rawkeys = zmalloc(sizeof(sds)*datactx->num);
                for (i = 0; i < datactx->num; i++) {
                    rawkeys[i] = bigSetEncodeSubkey(data->key->ptr,datactx->subkeys[i]->ptr);
                }
                *numkeys = datactx->num;
                *prawkeys = rawkeys;
                *action = ROCKS_MULTIGET;
            } else { /* Swap in entire set. */
                rawkeys = zmalloc(sizeof(sds));
                rawkeys[0] = bigSetEncodeSubkey(data->key->ptr,NULL);
                *numkeys = 1;
                *prawkeys = rawkeys;
                *action = ROCKS_SCAN;
            }
            return C_OK;
        case SWAP_DEL:
            if (data->meta) {
                rawkeys = zmalloc(sizeof(sds)*2);
                bigSetEncodeDeleteRange(data, &rawkeys[0], &rawkeys[1]);
                *numkeys = 2;
                *prawkeys = rawkeys;
                *action = ROCKS_DELETERANGE;
            } else {
                *action = 0;
                *numkeys = 0;
                *prawkeys = NULL;
            }
            return C_OK;
        case SWAP_OUT:
        default:
            /* Should not happen .*/
            *action = 0;
            *numkeys = 0;
            *prawkeys = NULL;
            return C_OK;
    }

    return C_OK;
}

int bigSetEncodeData(swapData *data_, int intention, void *datactx_,
                      int *action, int *numkeys, sds **prawkeys, sds **prawvals) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    if (datactx->num == 0) {
        *action = 0;
        *numkeys = 0;
        *prawkeys = NULL;
        *prawvals = NULL;
        return C_OK;
    }
    sds *rawkeys = zmalloc(datactx->num*sizeof(sds));
    sds *rawvals = zmalloc(datactx->num*sizeof(sds));
    serverAssert(intention == SWAP_OUT);
    for (int i = 0; i < datactx->num; i++) {
        rawkeys[i] = bigSetEncodeSubkey(data->key->ptr,datactx->subkeys[i]->ptr);
        rawvals[i] = sdsempty();
    }
    *action = ROCKS_WRITE;
    *numkeys = datactx->num;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    return C_OK;
}

int bigSetDecodeData(swapData *data_, int num, sds *rawkeys,
                      sds *rawvals, robj **pdecoded) {
    UNUSED(rawvals);
    int i;
    robj *decoded;
    bigSetSwapData *data = (bigSetSwapData*)data_;
    serverAssert(num >= 0);

    if (num == 0) {
        *pdecoded = NULL;
        return C_OK;
    }

    decoded = NULL;
    for (i = 0; i < num; i++) {
        sds subkey;
        const char *keystr, *subkeystr;
        size_t klen, slen;

        if (rocksDecodeSubkey(rawkeys[i],sdslen(rawkeys[i]),
                              &keystr,&klen,&subkeystr,&slen) < 0)
            continue;
        /* Decode do not hold obselete data.*/
        if (data->meta == NULL)
            continue;
        subkey = sdsnewlen(subkeystr,slen);
        serverAssert(memcmp(data->key->ptr,keystr,klen) == 0); //TODO remove

        if (NULL == decoded)
            decoded = setTypeCreate(subkey);
        setTypeAdd(decoded,subkey);
        sdsfree(subkey);
    }
    *pdecoded = decoded;
    return C_OK;
}

robj *bigSetCreateOrMergeObject(swapData *data_, robj *decoded, void *datactx_) {
    robj *result;
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    serverAssert(decoded == NULL || decoded->type == OBJ_SET);

    if (!data->value || !decoded) {
        /* decoded moved to exec again. */
        result = decoded;
        if (decoded) datactx->meta_len_delta -= setTypeSize(decoded);
    } else {
        setTypeIterator *si;
        sds subkey;
        si = setTypeInitIterator(decoded);
        while (NULL != (subkey = setTypeNextObject(si))) {
            int updated = setTypeAdd(data->value, subkey);
            if (!updated) datactx->meta_len_delta--;
            sdsfree(subkey);
        }

        setTypeReleaseIterator(si);
        /* decoded merged, we can release it now. */
        decrRefCount(decoded);
        result = NULL;
    }
    return result;
}

int bigSetCleanObject(swapData *data_, void *datactx_) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    if (!data->value) return C_OK;
    for (int i = 0; i < datactx->num; i++) {
        if (setTypeRemove(data->value, datactx->subkeys[i]->ptr)) {
            datactx->meta_len_delta++;
        }
    }
    return C_OK;
}

void freeBigSetSwapData(swapData *data_, void *datactx_) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    if (data->key) decrRefCount(data->key);
    if (data->value) decrRefCount(data->value);
    if (data->evict) decrRefCount(data->evict);
    /* db.meta is a ref, no need to free. */
    zfree(data);
    bigSetDataCtx *datactx = datactx_;
    for (int i = 0; i < datactx->num; i++) {
        decrRefCount(datactx->subkeys[i]);
    }
    zfree(datactx->subkeys);
    zfree(datactx);
}

static inline void doSwapIn(redisDb *db, robj* key, robj* val, long long expire, objectMeta *meta) {
    if (-1 != expire) dictDelete(db->expires,key->ptr);
    dbDeleteMeta(db, key);
    serverAssert(DICT_OK == dictDelete(db->evict,key->ptr));

    dbAdd(db,key,val);
    if (NULL != meta) dbAddMeta(db,key,meta);
    if (expire >= 0) setExpire(NULL,db,key,expire);
}

static inline void doSwapOut(redisDb *db, robj* key, robj* val, long long expire, objectMeta *meta) {
    if (-1 != expire) dictDelete(db->expires,key->ptr);
    dbDeleteMeta(db, key);
    serverAssert(DICT_OK == dictDelete(db->dict, key->ptr));

    dbAddEvict(db,key,val);
    if (NULL != meta) dbAddMeta(db,key,meta);
    if (expire >= 0) setExpire(NULL,db,key,expire);
}

static void createFakeSetForDeleteIfNeeded(bigSetSwapData *data) {
    if (data->evict) {
        redisDb *db = data->db;
        objectMeta *meta_dup = NULL != data->meta ? dupObjectMeta(data->meta) : NULL;
        // -2 for delete expire and no assign again
        doSwapIn(db, data->key, createSetObject(), -2, meta_dup);
        data->meta = meta_dup;
    }
}

int bigSetSwapAna(swapData *data_, int cmd_intention,
                   uint32_t cmd_intention_flags, struct keyRequest *req,
                   int *intention, uint32_t *intention_flags, void *datactx_) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    serverAssert(req->num_subkeys >= 0);

    if (intention_flags) *intention_flags = cmd_intention_flags;

    switch(cmd_intention) {
        case SWAP_NOP:
            *intention = SWAP_NOP;
            *intention_flags = 0;
            break;
        case SWAP_IN:
            if (data->meta == NULL || data->meta->len == 0) {
                /* No need to swap in for pure hot key */
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else if (req->num_subkeys == 0) {
                if (cmd_intention_flags == INTENTION_IN_DEL) {
                    /* DEL/GETDEL: Lazy delete current key. */
                    createFakeSetForDeleteIfNeeded(data);
                    *intention = SWAP_DEL;
                    *intention_flags = INTENTION_DEL_ASYNC;
                } else if (cmd_intention_flags == INTENTION_IN_META) {
                    *intention = SWAP_NOP;
                    *intention_flags = 0;
                } else {
                    *intention = SWAP_IN;
                    datactx->num = 0;
                    datactx->subkeys = NULL;
                }
            } else { /* keyrequests with subkeys */
                datactx->num = 0;
                datactx->subkeys = zmalloc(req->num_subkeys * sizeof(robj*));
                for (int i = 0; i < req->num_subkeys; i++) {
                    robj *subkey = req->subkeys[i];
                    /* even if field is hot (exists in value), we still
                     * need to do ROCKS_DEL on those fields. */
                    if (cmd_intention_flags == INTENTION_IN_DEL ||
                        data->value == NULL ||
                        !setTypeIsMember(data->value,subkey->ptr)) {
                        incrRefCount(subkey);
                        datactx->subkeys[datactx->num++] = subkey;
                    }
                }

                *intention = datactx->num > 0 ? SWAP_IN : SWAP_NOP;
            }
            break;
        case SWAP_OUT:
            if (data->value == NULL) {
                *intention = SWAP_NOP;
            } else {
                unsigned long long evict_memory = 0;
                sds vstr;
                datactx->subkeys = zmalloc(
                        server.swap_evict_step_max_subkeys*sizeof(robj*));
                setTypeIterator *si;
                si = setTypeInitIterator(data->value);
                while (NULL != (vstr = setTypeNextObject(si))) {
                    size_t vlen = sdslen(vstr);
                    // 需要算encode之后的长度吗？
                    robj* subkey = createObject(OBJ_STRING, vstr);

                    evict_memory += vlen;
                    datactx->subkeys[datactx->num++] = subkey;

                    if (datactx->num >= server.swap_evict_step_max_subkeys ||
                        evict_memory >= server.swap_evict_step_max_memory) {
                        /* Evict in small steps. */
                        break;
                    }
                }
                setTypeReleaseIterator(si);

                /* create new meta if needed */
                if (data->meta == NULL)
                    datactx->new_meta = createObjectMeta(0);

                if (!data->value->dirty) {
                    swapData *data_ = (swapData*)data;
                    /* directly evict value db.dict if not dirty. */
                    swapDataCleanObject(data_, datactx);
                    swapDataSwapOut(data_,datactx);
                    *intention = SWAP_NOP;
                } else {
                    *intention = SWAP_OUT;
                }
            }
            break;
        case SWAP_DEL:
            *intention = SWAP_DEL;
            break;
        default:
            break;
    }

    return 0;
}

static robj *createSwapInObject(robj *newval, robj *evict) {
    robj *swapin = newval;
    serverAssert(newval && newval->type == OBJ_SET);
    serverAssert(evict && evict->type == OBJ_SET);
    incrRefCount(newval);
    swapin->lru = evict->lru;
    swapin->big = evict->big;
    swapin->dirty = 0;
    return swapin;
}

/* Note: meta are kept as long as there are data in rocksdb. */
int bigSetSwapIn(swapData *data_, robj *result, void *datactx_) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    objectMeta *meta = data->meta;

    /* hot key no need to swap in, this must be a warm or cold key. */
    serverAssert(meta);
    if (!data->value && result == NULL) {
        /* cold key swapped in nothing: nop. */
    } else if (!data->value && result != NULL) {
        /* cold key swapped in fields */
        /* dup expire/meta satellites before evict deleted. */
        meta = dupObjectMeta(data->meta);
        meta->len += datactx->meta_len_delta;
        serverAssert(meta->len >= 0);
        long long expire = getExpire(data->db,data->key);
        robj *swapin = createSwapInObject(result,data->evict);
        doSwapIn(data->db, data->key, swapin, expire, meta);
    } else {
        /* if data.value exists, then we expect all fields merged already
         * and nothing need to be swapped in. */
        serverAssert(result == NULL);
        data->meta->len += datactx->meta_len_delta;
    }

    return C_OK;
}

static robj *createSwapOutObject(robj *value, robj *evict) {
    serverAssert(value && !evict);
    robj *swapout = createObject(value->type,NULL);
    swapout->lru = value->lru;
    swapout->big = value->big;
    return swapout;
}

/* subkeys already cleaned by cleanObject(to save cpu usage of main thread),
 * swapout updates keyspace (db.meta/db.evict/db.dict/db.expire). */
int bigSetSwapOut(swapData *data_, void *datactx_) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    bigSetDataCtx *datactx = datactx_;
    serverAssert(data->value && !data->evict);
    serverAssert(datactx->meta_len_delta >= 0);
    if (!setTypeSize(data->value)) {
        /* all fields swapped out, key turns into cold. */
        robj *swapout;
        long long expire;
        /* dup satellite (expire/meta) before value delete */
        if (data->meta) {
            datactx->new_meta = dupObjectMeta(data->meta);
            datactx->new_meta->len += datactx->meta_len_delta;
        } else {
            /* must have swapped out some fields, otherwise value should
             * not be empty. */
            serverAssert(datactx->meta_len_delta);
            datactx->new_meta->len += datactx->meta_len_delta;
        }
        expire = getExpire(data->db,data->key);
        swapout = createSwapOutObject(data->value,data->evict);
        doSwapOut(data->db, data->key, swapout, expire, datactx->new_meta);
        datactx->new_meta = NULL; /* moved */
    } else {
        /* not all fields swapped out. */
        if (!data->meta) {
            if (datactx->meta_len_delta == 0) {
                /* hot set stays hot: nop */
            } else {
                /* hot key turns warm: add meta. */
                datactx->new_meta->len += datactx->meta_len_delta;
                dbAddMeta(data->db,data->key,datactx->new_meta);
                datactx->new_meta = NULL; /* moved */
            }
        } else {
            /* swap out some. */
            data->meta->len += datactx->meta_len_delta;
        }
    }

    return C_OK;
}

int bigSetSwapDel(swapData *data_, void *datactx, int async) {
    bigSetSwapData *data = (bigSetSwapData*)data_;
    UNUSED(datactx);
    if (async) {
        if (data->meta) dbDeleteMeta(data->db,data->key);
        return C_OK;
    } else {
        if (data->value) dbDelete(data->db,data->key);
        if (data->evict) dbDeleteEvict(data->db,data->key);
        return C_OK;
    }
}

swapDataType bigSetSwapDataType = {
    .name = "bigset",
    .swapAna = bigSetSwapAna,
    .encodeKeys = bigSetEncodeKeys,
    .encodeData = bigSetEncodeData,
    .decodeData = bigSetDecodeData,
    .swapIn = bigSetSwapIn,
    .swapOut = bigSetSwapOut,
    .swapDel = bigSetSwapDel,
    .createOrMergeObject = bigSetCreateOrMergeObject,
    .cleanObject = bigSetCleanObject,
    .free = freeBigSetSwapData,
};

rdbKeyType bigSetRdbType = {
    .save_start = NULL,
    .save = NULL,
    .save_end = NULL,
    .save_deinit = NULL,
    .load = NULL,
    .load_end = NULL,
    .load_dbadd = NULL,
    .load_deinit = NULL,
};

swapData *createBigSetSwapData(redisDb *db, robj *key, robj *value, robj *evict, objectMeta *meta, void **pdatactx) {
    bigSetSwapData *data = zmalloc(sizeof(bigSetSwapData));
    data->d.type = &bigSetSwapDataType;
    data->db = db;
    if (key) incrRefCount(key);
    data->key = key;
    if (value) incrRefCount(value);
    data->value = value;
    if (evict) incrRefCount(evict);
    data->evict = evict;
    data->meta = meta;
    bigSetDataCtx *datactx = zmalloc(sizeof(bigSetDataCtx));
    datactx->meta_len_delta = 0;
    datactx->num = 0;
    datactx->subkeys = NULL;
    if (pdatactx) *pdatactx = datactx;

    return (swapData*)data;
}

#ifdef REDIS_TEST

int swapDataBigSetTest(int argc, char **argv, int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    initTestRedisServer();
    redisDb* db = server.db + 0;
    int error = 0;
    swapData *set1_data, *cold1_data;
    bigSetDataCtx *set1_ctx, *cold1_ctx;
    robj *key1, *set1, *cold1, *cold1_evict, *decoded;
    keyRequest _kr1, *kr1 = &_kr1, _cold_kr1, *cold_kr1 = &_cold_kr1;
    objectMeta *cold1_meta;
    robj *subkeys1[4];
    sds f1, f2, f3, f4;
    int action, numkeys;
    int oldEvictStep = server.swap_evict_step_max_subkeys;

    TEST("bigSet - init") {
        server.swap_evict_step_max_subkeys = 2;
        key1 = createStringObject("key1",4);
        f1 = sdsnew("f1"), f2 = sdsnew("f2"), f3 = sdsnew("f3"), f4 = sdsnew("f4");
        set1 = createSetObject();
        set1->big = 1;
        setTypeAdd(set1, f1);
        setTypeAdd(set1, f2);
        setTypeAdd(set1, f3);
        setTypeAdd(set1, f4);
        dbAdd(db,key1,set1);

        cold1 = createStringObject("cold1",5);
        cold1_evict = createObject(OBJ_SET,NULL);
        cold1_evict->big = 1;
        cold1_meta = createObjectMeta(4);
    }

    TEST("bigSet - encodeData/DecodeData") {
        robj *origin = setTypeDup(set1);
        int originsize = setTypeSize(origin);
        set1_data = createBigSetSwapData(db, key1,set1,NULL,NULL,(void**)&set1_ctx);
        bigSetSwapData *set1_data_ = (bigSetSwapData*)set1_data;
        sds *rawkeys, *rawvals;

        set1_ctx->num = 2;
        set1_ctx->subkeys = zmalloc(set1_ctx->num*sizeof(robj*));
        setTypeIterator *si= setTypeInitIterator(set1);
        set1_ctx->subkeys[0] = createObject(OBJ_STRING, setTypeNextObject(si));
        set1_ctx->subkeys[1] = createObject(OBJ_STRING, setTypeNextObject(si));
        setTypeReleaseIterator(si);
        set1_data_->meta = createObjectMeta(0);

        bigSetEncodeData(set1_data, SWAP_OUT, set1_ctx, &action, &numkeys, &rawkeys, &rawvals);
        test_assert(action == ROCKS_WRITE);
        test_assert(numkeys == set1_ctx->num);

        bigSetCleanObject(set1_data, set1_ctx);
        test_assert(originsize - 2 == setTypeSize(set1));

        bigSetDecodeData(set1_data, set1_ctx->num, rawkeys, rawvals, &decoded);
        test_assert(NULL != decoded);
        test_assert(2 == setTypeSize(decoded));

        bigSetCreateOrMergeObject(set1_data, decoded, set1_ctx);
        test_assert(originsize == setTypeSize(set1));
        test_assert(origin->encoding == set1->encoding);
        si = setTypeInitIterator(origin);
        sds ele;
        while (NULL != (ele = setTypeNextObject(si))) {
            test_assert(setTypeIsMember(set1, ele));
        }
        setTypeReleaseIterator(si);

        freeObjectMeta(set1_data_->meta);
        set1_data_->meta = NULL;
        decrRefCount(origin);

        for (int i = 0; i < set1_ctx->num; i++) {
            sdsfree(rawkeys[i]);
            sdsfree(rawvals[i]);
        }
        zfree(rawkeys);
        zfree(rawvals);
        freeBigSetSwapData(set1_data, set1_ctx);
    }

    TEST("bigset - swapAna") {
        int intention;
        uint32_t intention_flags;
        objectMeta *set1_meta = createObjectMeta(0);
        set1_data = createBigSetSwapData(db, key1,set1,NULL,NULL,(void**)&set1_ctx);
        cold1_data = createBigSetSwapData(db,cold1,NULL,cold1_evict,cold1_meta,(void**)&cold1_ctx);
        bigSetSwapData *set1_data_ = (bigSetSwapData*)set1_data;
        kr1->key = key1;
        kr1->level = REQUEST_LEVEL_KEY;
        kr1->num_subkeys = 0;
        kr1->subkeys = NULL;
        cold_kr1->key = key1;
        cold_kr1->level = REQUEST_LEVEL_KEY;
        cold_kr1->num_subkeys = 0;
        cold_kr1->subkeys = NULL;

        // swap nop
        /* nop: NOP/IN_META/IN_DEL/IN hot/OUT cold/DEL_ASYNC... */
        swapDataAna(set1_data,SWAP_NOP,0,kr1,&intention,&intention_flags,set1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(set1_data,SWAP_IN,INTENTION_IN_META,kr1,&intention,&intention_flags,set1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(set1_data,SWAP_IN,INTENTION_IN_DEL,kr1,&intention,&intention_flags,set1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(set1_data,SWAP_IN,0,kr1,&intention,&intention_flags,set1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(set1_data,SWAP_IN,0,kr1,&intention,&intention_flags,set1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(cold1_data,SWAP_OUT,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(cold1_data,SWAP_DEL,INTENTION_DEL_ASYNC,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_DEL && intention_flags == INTENTION_DEL_ASYNC);
        /* in: entire or with subkeys */
        swapDataAna(cold1_data,SWAP_IN,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->num == 0 && cold1_ctx->subkeys == NULL);
        subkeys1[0] = createStringObject(f1,sdslen(f1));
        subkeys1[1] = createStringObject(f2,sdslen(f2));
        cold_kr1->num_subkeys = 2;
        cold_kr1->subkeys = subkeys1;
        swapDataAna(cold1_data,SWAP_IN,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->num == 2 && cold1_ctx->subkeys != NULL);
        /* out: evict by small steps */
        kr1->num_subkeys = 0;
        kr1->subkeys = NULL;
        swapDataAna(set1_data,SWAP_OUT,0,kr1,&intention,&intention_flags,set1_ctx);
        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(cold1_ctx->num == 2 && cold1_ctx->subkeys != NULL);
        freeBigSetSwapData(set1_data, set1_ctx);
        freeBigSetSwapData(cold1_data, cold1_ctx);
    }

    TEST("bigset - swapIn/swapOut") {
        robj *s, *e;
        objectMeta *m;
        set1_data = createBigSetSwapData(db, key1,set1,NULL,NULL,(void**)&set1_ctx);
        bigSetSwapData _data = *(bigSetSwapData*)set1_data, *data = &_data;
        test_assert(lookupMeta(db,key1) == NULL);

        /* hot => warm => cold */
        setTypeRemove(set1,f1);
        setTypeRemove(set1,f2);
        set1_ctx->meta_len_delta = 2;
        set1_ctx->new_meta = createObjectMeta(0);
        bigSetSwapOut((swapData*)data, set1_ctx);
        test_assert((m =lookupMeta(db,key1)) != NULL && m->len == 2);
        test_assert(lookupEvictKey(db,key1) == NULL);

        setTypeRemove(set1, f3);
        setTypeRemove(set1, f4);
        set1_ctx->meta_len_delta = 2;
        data->meta = lookupMeta(data->db,data->key);
        bigSetSwapOut((swapData*)data, set1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 4);
        test_assert((e = lookupEvictKey(db,key1)) != NULL);
        test_assert((s = lookupKey(db,key1,LOOKUP_NOTOUCH)) == NULL);

        /* cold => warm => hot */
        set1 = createSetObject();
        setTypeAdd(set1, f1);
        setTypeAdd(set1, f2);
        set1_ctx->meta_len_delta = -2;
        data->value = s;
        data->evict = e;
        data->meta = m;
        bigSetSwapIn((swapData*)data,set1,set1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 2);
        test_assert((e = lookupEvictKey(db,key1)) == NULL);
        test_assert((s = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(s->big && setTypeSize(s) == 2);

        decoded = setTypeDup(set1);
        setTypeAdd(decoded,f3);
        setTypeAdd(decoded,f4);
        data->value = s;
        data->evict = e;
        data->meta = m;
        bigSetCreateOrMergeObject((swapData*)data,decoded,set1_ctx);
        bigSetSwapIn((swapData*)data,NULL,set1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL);
        test_assert((e = lookupEvictKey(db,key1)) == NULL);
        test_assert((s = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(s->big && setTypeSize(s) == 4);

        /* hot => cold */
        setTypeRemove(set1,f1);
        setTypeRemove(set1,f2);
        setTypeRemove(set1,f3);
        setTypeRemove(set1,f4);
        set1_ctx->new_meta = createObjectMeta(0);
        set1_ctx->meta_len_delta = 4;
        *data = *(bigSetSwapData*)set1_data;
        bigSetSwapOut((swapData*)data, set1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 4);
        test_assert((e = lookupEvictKey(db,key1)) != NULL);
        test_assert((s = lookupKey(db,key1,LOOKUP_NOTOUCH)) == NULL);

        /* cold => hot */
        set1 = createSetObject();
        setTypeAdd(set1, f1);
        setTypeAdd(set1, f2);
        setTypeAdd(set1, f3);
        setTypeAdd(set1, f4);
        data->value = s;
        data->meta = m;
        data->evict = e;
        set1_ctx->meta_len_delta = -4;
        bigSetSwapIn((swapData*)data,set1,set1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL);
        test_assert((e = lookupEvictKey(db,key1)) == NULL);
        test_assert((s = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(s->big && setTypeSize(s) == 4);

        freeBigSetSwapData(set1_data, set1_ctx);
    }

    TEST("bigset - free") {
        decrRefCount(set1);
        server.swap_evict_step_max_subkeys = oldEvictStep;
    }

    return error;
}

#endif
