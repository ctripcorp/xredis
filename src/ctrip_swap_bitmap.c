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

#define BITMAP_SUBKEY_SIZE 4096  /* default 4KB */
#define BITS_NUM_IN_SUBKEY 32768

typedef struct bitmapMeta {
    int bitmapSize;
    int subkeysSum;
    int subkeysNumOnlyRocks;
    roaringBitmap *subkeysInRedis;
} bitmapMeta;

static inline bitmapMeta *bitmapMetaCreate(void) {
    bitmapMeta *bitmap_meta = zmalloc(sizeof(bitmapMeta));
    bitmap_meta->bitmapSize = 0;
    bitmap_meta->subkeysSum = 0;
    bitmap_meta->subkeysNumOnlyRocks = 0;
    bitmap_meta->subkeysInRedis = rbmCreate();
    return bitmap_meta;
}

void bitmapMetaFree(bitmapMeta *bitmap_meta) {
    if (bitmap_meta == NULL) return;
    rbmDestory(bitmap_meta->subkeysInRedis);
    zfree(bitmap_meta);
}

typedef struct bitmapDataCtx {
    int ctx_flag;
    int subkeysSize;  /* only used in swap out */
    int subkeysNum;
    int *subkeysLogicIdx;
    argRewriteRequest arg_reqs[2];
} bitmapDataCtx;

/*
* todo 将在Meta机制修改中添加，首次创建bitmap 对象时， data->new_meta = createBitmapObjectMeta(swapGetAndIncrVersion(),bitmap_meta)，bitmap_meta暂时为NULL，在后续首次 swap out 时 set。
*/

objectMeta *createBitmapObjectMeta(uint64_t version, bitmapMeta *bitmap_meta) {
    objectMeta *object_meta = createObjectMeta(OBJ_BITMAP, version);
    objectMetaSetPtr(object_meta, bitmap_meta);
    return object_meta;
}

static inline sds encodeBitmapMeta(bitmapMeta *bm) {
    if (bm == NULL) return NULL;
    return sdsnewlen(&bm->bitmapSize, sizeof(int));
}

static inline bitmapMeta *decodeBitmapMeta(const char *extend, size_t extendLen) {
    if (extendLen != sizeof(int)) return NULL;
    bitmapMeta *bitmap_meta = bitmapMetaCreate();
    bitmap_meta->bitmapSize = *(int *)extend;
    bitmap_meta->subkeysSum = (bitmap_meta->bitmapSize - 1) / BITMAP_SUBKEY_SIZE + 1;
    bitmap_meta->subkeysNumOnlyRocks = bitmap_meta->subkeysSum;
    return bitmap_meta;
}

sds encodeBitmapObjectMeta(struct objectMeta *object_meta, void *aux) {
    UNUSED(aux);
    if (object_meta == NULL) return NULL;
    serverAssert(object_meta->object_type == OBJ_BITMAP);
    return encodeBitmapMeta(objectMetaGetPtr(object_meta));
}

int decodeBitmapObjectMeta(struct objectMeta *object_meta, const char *extend, size_t extlen) {
    serverAssert(object_meta->object_type == OBJ_BITMAP);
    serverAssert(objectMetaGetPtr(object_meta) == NULL);
    objectMetaSetPtr(object_meta, decodeBitmapMeta(extend, extlen));
    return 0;
}

int bitmapObjectMetaIsHot(struct objectMeta *object_meta, robj *value)
{
    UNUSED(value);
    serverAssert(object_meta && object_meta->object_type == OBJ_BITMAP);
    bitmapMeta *meta = objectMetaGetPtr(object_meta);
    if (meta == NULL) {
        /* ptr not set, bitmap is purely hot, never swapped out */
        return 1;
    } else {
        return meta->subkeysNumOnlyRocks == 0;
    }
}

void bitmapObjectMetaFree(objectMeta *object_meta) {
    if (object_meta == NULL) return;
    bitmapMetaFree(objectMetaGetPtr(object_meta));
    objectMetaSetPtr(object_meta, NULL);
}

static inline bitmapMeta *bitmapMetaDup(bitmapMeta *bitmap_meta) {
    bitmapMeta *meta = zmalloc(sizeof(bitmapMeta));
    meta->subkeysSum = bitmap_meta->subkeysSum;
    meta->bitmapSize = bitmap_meta->bitmapSize;
    meta->subkeysNumOnlyRocks = bitmap_meta->subkeysNumOnlyRocks;
    meta->subkeysInRedis = zmalloc(sizeof(roaringBitmap));
    rbmdup(meta->subkeysInRedis, bitmap_meta->subkeysInRedis);
    return meta;
}

void bitmapObjectMetaDup(struct objectMeta *dup_meta, struct objectMeta *object_meta) {
    if (object_meta == NULL) return;
    serverAssert(dup_meta->object_type == OBJ_BITMAP);
    serverAssert(objectMetaGetPtr(dup_meta) == NULL);
    if (objectMetaGetPtr(object_meta) == NULL) return;
    objectMetaSetPtr(dup_meta, bitmapMetaDup(objectMetaGetPtr(object_meta)));
}

int bitmapObjectMetaEqual(struct objectMeta *dest_om, struct objectMeta *src_om) {
    bitmapMeta *dest_meta = objectMetaGetPtr(dest_om);
    bitmapMeta *src_meta = objectMetaGetPtr(src_om);
    if (dest_meta->subkeysSum != src_meta->subkeysSum || !rbmIsEqual(dest_meta->subkeysInRedis, src_meta->subkeysInRedis)) {
        return 0;
    }
    return 1;
}

static inline int bitmapMetaGetHotSubkeysNum(bitmapMeta *bitmap_meta, int startSubkeyIdx, int endSubkeyIdx)
{
    if (bitmap_meta == NULL) {
        return 0;
    }
    serverAssert(startSubkeyIdx <= endSubkeyIdx && startSubkeyIdx >= 0);
    return rbmGetBitRange(bitmap_meta->subkeysInRedis, startSubkeyIdx, endSubkeyIdx);
}

/* status = 1, set to hot
 * status = 0, set to cold
 * */
static inline void bitmapMetaSetSubkeyStatus(bitmapMeta *bitmap_meta, int startSubkeyIdx, int endSubkeyIdx, int status)
{
    if (bitmap_meta == NULL) {
        return;
    }
    serverAssert(startSubkeyIdx <= endSubkeyIdx && startSubkeyIdx >= 0);
    if (status) {
        rbmSetBitRange(bitmap_meta->subkeysInRedis, startSubkeyIdx, endSubkeyIdx);
    } else {
        rbmClearBitRange(bitmap_meta->subkeysInRedis, startSubkeyIdx, endSubkeyIdx);
    }
}

objectMetaType bitmapObjectMetaType = {
        .encodeObjectMeta = encodeBitmapObjectMeta,
        .decodeObjectMeta = decodeBitmapObjectMeta,
        .objectIsHot = bitmapObjectMetaIsHot,
        .free = bitmapObjectMetaFree,
        .duplicate = bitmapObjectMetaDup,
        .equal = bitmapObjectMetaEqual
};

static inline bitmapMeta *swapDataGetBitmapMeta(swapData *data) {
    objectMeta *object_meta = swapDataObjectMeta(data);
    return object_meta ? objectMetaGetPtr(object_meta) : NULL;
}

static inline void getSubKeysRequired(bitmapDataCtx *datactx, bitmapMeta *bm, bool isWriteReq, long long startOffset, long long endOffset)
{
    int requiredSubkeyStartIdx = startOffset / BITS_NUM_IN_SUBKEY;
    int requiredSubkeyEndIdx = endOffset / BITS_NUM_IN_SUBKEY;

    /* when subkeys range required exceed the max subkey idx,
     * if it's a write operation, then redis will extend for a bigger new bitmap.
     * so some operations here will be a little tricky, we need ensure the last subkey will be swap in.
     * if it's a read operation, there is no extension operation in redis,
     * we don't need to the last subkey swaped in. */

    /*
     * 1、 required range exceed subkeysSum - 1:
     *    1) we need to swap in the subkeys in the range of [requiredSubkeyStartIdx, subkeysSum - 1]
     *      PS: (if requiredSubkeyStartIdx > subkeysSum - 1) ,  swap in the last subkey (idx = subkeysSum - 1);
     *
     * 2、  required range not exceed the max subkey idx:
     *    1) swap in the subkeys required;
     * */

    int subkeysSwapInStartIdx = requiredSubkeyStartIdx;
    int subkeysSwapInEndIdx = requiredSubkeyEndIdx;

    if (isWriteReq) {
        if (requiredSubkeyStartIdx > bm->subkeysSum - 1) {
            subkeysSwapInStartIdx = bm->subkeysSum - 1;
        }

        if (requiredSubkeyEndIdx > bm->subkeysSum - 1) {
            subkeysSwapInEndIdx = bm->subkeysSum - 1;
        }
    } else {
        if (requiredSubkeyStartIdx > bm->subkeysSum - 1) {
            datactx->subkeysNum = 0;
            return;
        }
        if (requiredSubkeyEndIdx > bm->subkeysSum - 1) {
            subkeysSwapInEndIdx = bm->subkeysSum - 1;
        }
    }

    int subkeyNumNeedSwapin = subkeysSwapInEndIdx - subkeysSwapInStartIdx + 1 - bitmapMetaGetHotSubkeysNum(bm, subkeysSwapInStartIdx, subkeysSwapInEndIdx);

    /* subkeys required are all in redis */
    if (subkeyNumNeedSwapin == 0) {
        /* subkeys required have been in redis.*/
        datactx->subkeysNum = 0;
        return;
    }

    /* subkeys required are not all in redis */
    if (subkeyNumNeedSwapin == bm->subkeysSum) {
        /* all subKey of bitmap need to swap in */
        datactx->subkeysNum = -1;
        return;
    }

    datactx->subkeysLogicIdx = zmalloc(sizeof(robj*) * subkeyNumNeedSwapin);
    int cursor = 0;
    /* record idx of subkey to swap in. */
    for (int i = subkeysSwapInStartIdx; i <= subkeysSwapInEndIdx; i++) {
        if (bitmapMetaGetHotSubkeysNum(bm, i, i) == 0) {
            datactx->subkeysLogicIdx[cursor++] = i;
        }
    }
    datactx->subkeysNum = cursor;
}

/* todo 注释添加, 接口， 变量命名 */

static inline int decodeSubkeyIdx(const char *str, size_t len) {
    if (len != sizeof(int)) {
        return -1;
    }
    int idx = *(int*)str;
    return idx;
}

#define SELECT_MAIN 0
#define SELECT_DSS  1

static int bitmapSwapAnaOutSelectSubkeys(swapData *data, bitmapDataCtx *datactx, int *may_keep_data)
{
    int noswap;
    bitmapMeta *meta = swapDataGetBitmapMeta(data);

    int hotSubkeysNum = bitmapMetaGetHotSubkeysNum(meta, 0, meta->subkeysSum - 1);
    int subkeysNumMaySwapout = 0;

    /* only SELECT_MAIN */
    if (objectIsDataDirty(data->value)) { /* all subkeys might be dirty */
        subkeysNumMaySwapout = hotSubkeysNum;
        noswap = 0;
    } else {
        /* If data dirty, meta will be persisted as an side effect.
         * If just meta dirty, we still persists meta.
         * If data & meta clean, we persists nothing (just free). */
        if (objectIsMetaDirty(data->value)) { /* meta dirty */
            /* meta dirty */
            subkeysNumMaySwapout = 0;
            noswap = 0;
        } else { /* clean */
            subkeysNumMaySwapout = hotSubkeysNum;
            noswap = 1;
        }
    }

    subkeysNumMaySwapout = MIN(server.swap_evict_step_max_subkeys, subkeysNumMaySwapout);
    if (!noswap) *may_keep_data = 0;

    int cursor = 0;
    long long swapoutSubkeysBufSize = 0;

    /* from left to right to select subkeys */
    for (int i = 0; i < subkeysNumMaySwapout; i++) {

        int subValSize = 0;
        int leftSize = stringObjectLen(data->value) - BITMAP_SUBKEY_SIZE * i;
        if (leftSize < BITMAP_SUBKEY_SIZE) {
            subValSize = leftSize;
        } else {
            subValSize = BITMAP_SUBKEY_SIZE;
        }

        if (swapoutSubkeysBufSize + subValSize > server.swap_evict_step_max_memory) {
            break;
        }
        swapoutSubkeysBufSize += subValSize;

        datactx->subkeysSize += subValSize;
        cursor = i + 1;
    }
    datactx->subkeysNum = cursor;
}

static inline void bitmapInitMeta(bitmapMeta *meta, robj *value)
{
    int actualBitmapSize = stringObjectLen(value);
    meta->bitmapSize = actualBitmapSize;
    meta->subkeysSum = (actualBitmapSize - 1) / BITMAP_SUBKEY_SIZE + 1;
    meta->subkeysNumOnlyRocks = 0;
    rbmSetBitRange(meta->subkeysInRedis, 0, meta->subkeysSum - 1);
}

int bitmapSwapAna(swapData *data, int thd, struct keyRequest *req,
                int *intention, uint32_t *intention_flags, void *datactx_) {
    bitmapDataCtx *datactx = datactx_;

    int cmd_intention = req->cmd_intention;
    uint32_t cmd_intention_flags = req->cmd_intention_flags;
    UNUSED(thd);

    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    switch (cmd_intention) {
        case SWAP_NOP:
            *intention = SWAP_NOP;
            *intention_flags = 0;
            break;
        case SWAP_IN:
            if (!swapDataPersisted(data)) {
                /* No need to swap for pure hot key */
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else if (req->l.num_ranges == 0) {
                if (cmd_intention_flags == SWAP_IN_DEL_MOCK_VALUE) {
                    /* DEL, unlink */
                    datactx->ctx_flag = BIG_DATA_CTX_FLAG_MOCK_VALUE;
                    *intention = SWAP_DEL;
                    *intention_flags = SWAP_FIN_DEL_SKIP;
                } else if (cmd_intention_flags & SWAP_IN_DEL  /* cmd rename... */
                    || cmd_intention_flags & SWAP_IN_OVERWRITE
                    || cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                    if (meta->subkeysNumOnlyRocks == 0) {
                        /* the same subkeys in redis get dirty, delete the subkeys in rocksDb.*/
                        *intention = SWAP_DEL;
                        *intention_flags = SWAP_FIN_DEL_SKIP;
                    } else {
                        *intention = SWAP_IN;
                        *intention_flags = SWAP_EXEC_IN_DEL;
                    }
                    if (cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                        *intention_flags |= SWAP_EXEC_FORCE_HOT;
                    }
                } else if (swapDataIsHot(data)) {
                    /* No need to do swap for hot key(execept for SWAP_IN_DEl). */
                    *intention = SWAP_NOP;
                    *intention_flags = 0;
                } else if (cmd_intention_flags == SWAP_IN_META) {
                    if (!swapDataIsCold(data)) {
                        *intention = SWAP_NOP;
                        *intention_flags = 0;
                    } else {
                        datactx->subkeysNum = 0;
                        datactx->subkeysLogicIdx = zmalloc(sizeof(int));
                        datactx->subkeysLogicIdx[datactx->subkeysNum++] = -1;
                        *intention = SWAP_IN;
                        *intention_flags = 0;
                    }
                } else {
                    /* string, keyspace ... operation */
                    /* swap in all subkeys, and keep them in rocks. */
                    datactx->subkeysNum = -1;
                    *intention = SWAP_IN;
                    *intention_flags = 0;
                }
            } else { /* range requests */
                getSubKeysRequired(datactx, meta, cmd_intention_flags == SWAP_IN_DEL,  req->l.ranges->start, req->l.ranges->end);

                *intention = datactx->subkeysNum == 0 ? SWAP_NOP : SWAP_IN;
                if (cmd_intention_flags == SWAP_IN_DEL)
                    *intention_flags = SWAP_EXEC_IN_DEL;
                else
                    *intention_flags = 0;

            }
            if (cmd_intention_flags & SWAP_OOM_CHECK) {
                *intention_flags |= SWAP_EXEC_OOM_CHECK;
            }
            break;
        case SWAP_OUT:
            if (swapDataIsCold(data)) {
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else {
                /* purely hot bitmap to swap out, need to set meta */
                if (!swapDataPersisted(data)) {
                    /* todo bitmap meta 机制， 如果是 hot， object meta 中ptr not set. */
                    objectMeta *object_meta = swapDataObjectMeta(data);
                    serverAssert(object_meta != NULL);
                    bitmapMeta *meta = bitmapMetaCreate();
                    bitmapInitMeta(meta, data->value);
                    objectMetaSetPtr(object_meta, meta);
                }

                int may_keep_data;

                int noswap = bitmapSwapAnaOutSelectSubkeys(data, datactx, &may_keep_data);
                int keep_data = swapDataPersistKeepData(data,cmd_intention_flags,may_keep_data);

                if (noswap) {
                    /* directly evict value from db.dict if not dirty. */
                    swapDataCleanObject(data, datactx, keep_data);
                    if (stringObjectLen(data->value) == 0) {
                        swapDataTurnCold(data);
                    }
                    swapDataSwapOut(data,datactx,keep_data,NULL);

                    *intention = SWAP_NOP;
                    *intention_flags = 0;
                } else {
                    *intention = SWAP_OUT;
                    *intention_flags = keep_data ? SWAP_EXEC_OUT_KEEP_DATA : 0;
                }
            }
            break;
        case SWAP_DEL:
            if (!swapDataPersisted(data)) {
                /* If key is hot, swapAna must be executing in main-thread,
                 * we can safely delete meta. */
                dbDeleteMeta(data->db, data->key);
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else {
                *intention = SWAP_DEL;
                *intention_flags = 0;
            }
            break;
        default:
            break;
    }

    datactx->arg_reqs[0] = req->arg_rewrite[0];
    datactx->arg_reqs[1] = req->arg_rewrite[1];

    return 0;
}

int bitmapSwapAnaAction(swapData *data, int intention, void *datactx_, int *action) {
    UNUSED(data);
    bitmapDataCtx *datactx = datactx_;
    switch (intention) {
        case SWAP_IN:
            if (datactx->subkeysNum > 0) *action = ROCKS_GET; /* Swap in specific fields */
            else *action = ROCKS_ITERATE; /* Swap in entire bitmap */
            break;
        case SWAP_DEL:
            /* No need to del data (meta will be deleted by exec) */
            *action = ROCKS_NOP;
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

static inline sds bitmapEncodeSubkey(redisDb *db, sds key, uint64_t version,
                                   sds subkey) {
    return rocksEncodeDataKey(db,key,version,subkey);
}

int bitmapEncodeKeys(swapData *data, int intention, void *datactx_,
                   int *numkeys, int **pcfs, sds **prawkeys) {
    bitmapDataCtx *datactx = datactx_;
    sds *rawkeys = NULL;
    int *cfs = NULL;
    uint64_t version = swapDataObjectVersion(data);

    if (datactx->subkeysNum <= 0) {
        return 0;
    }
    serverAssert(intention == SWAP_IN);
    cfs = zmalloc(sizeof(int) * datactx->subkeysNum);
    rawkeys = zmalloc(sizeof(sds) * datactx->subkeysNum);
    for (int i = 0; i < datactx->subkeysNum; i++) {
        cfs[i] = DATA_CF;
        sds keyStr;
        if (datactx->subkeysLogicIdx[i] == -1) {
            keyStr = sdsnewlen("foo", 3);
        } else {
            keyStr = sdsnewlen(&(datactx->subkeysLogicIdx[i]), sizeof(int));
        }
        rawkeys[i] = bitmapEncodeSubkey(data->db,data->key->ptr, version, keyStr);
        sdsfree(keyStr);
    }
    *numkeys = datactx->subkeysNum;
    *pcfs = cfs;
    *prawkeys = rawkeys;

    return 0;
}

int bitmapEncodeRange(struct swapData *data, int intention, void *datactx, int *limit,
                    uint32_t *flags, int *pcf, sds *start, sds *end) {
    UNUSED(intention), UNUSED(datactx);
    uint64_t version = swapDataObjectVersion(data);

    *pcf = DATA_CF;
    *flags = 0;
    *start = rocksEncodeDataRangeStartKey(data->db, data->key->ptr, version);
    *end = rocksEncodeDataRangeEndKey(data->db, data->key->ptr, version);
    *limit = ROCKS_ITERATE_NO_LIMIT;
    return 0;
}

static inline robj *bitmapGetSubVal(robj *o, int phyIdx)
{
    int subValSize = 0;
    int leftSize = stringObjectLen(o) - BITMAP_SUBKEY_SIZE * phyIdx;
    if (leftSize < BITMAP_SUBKEY_SIZE) {
        subValSize = leftSize;
    } else {
        subValSize = BITMAP_SUBKEY_SIZE;
    }
    return createStringObject(o->ptr + phyIdx * BITMAP_SUBKEY_SIZE, subValSize);
}

static inline sds bitmapEncodeSubval(robj *subval) {
    return rocksEncodeValRdb(subval);
}

/* count subkeysNum hot subkeys from front to back in meta->subkeysInRedis, return the real hot subkeys number,
 * output the idxs(malloc by caller).
 * */
static inline int bitmapMetaGetSubkeysPos(bitmapMeta *meta, int subkeysNum, int *subkeysIdx)
{
    if (meta == NULL || meta->subkeysInRedis == NULL || subkeysNum == 0 || subkeysIdx == NULL) {
        return 0;
    }
    return rbmGetBitPos(meta->subkeysInRedis, subkeysNum, subkeysIdx);
}

int bitmapEncodeData(swapData *data, int intention, void *datactx_,
                   int *numkeys, int **pcfs, sds **prawkeys, sds **prawvals) {
    serverAssert(intention == SWAP_OUT);

    bitmapDataCtx *datactx = datactx_;

    if (datactx->subkeysNum == 0) {
        return 0;
    }

    int *cfs = zmalloc(datactx->subkeysNum * sizeof(int));
    sds *rawkeys = zmalloc(datactx->subkeysNum * sizeof(sds));
    sds *rawvals = zmalloc(datactx->subkeysNum * sizeof(sds));
    uint64_t version = swapDataObjectVersion(data);

    bitmapMeta *meta = swapDataGetBitmapMeta(data);

    datactx->subkeysLogicIdx = zmalloc(sizeof(int) * datactx->subkeysNum);
    int num = bitmapMetaGetSubkeysPos(meta, datactx->subkeysNum, datactx->subkeysLogicIdx);
    serverAssert(num == datactx->subkeysNum);

    for (int i = 0; i < datactx->subkeysNum; i++) {

        cfs[i] = DATA_CF;

        int logicIdx = datactx->subkeysLogicIdx[i];
        sds keyStr = sdsnewlen(&logicIdx, sizeof(int));

        robj *subval = bitmapGetSubVal(data->value, i);
        serverAssert(subval);
        rawvals[i] = bitmapEncodeSubval(subval);
        rawkeys[i] = bitmapEncodeSubkey(data->db, data->key->ptr, version, keyStr);
        decrRefCount(subval);
        sdsfree(keyStr);
    }
    *numkeys = datactx->subkeysNum;
    *pcfs = cfs;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    return 0;
}

typedef struct subkeyInterval {
    int startIndex;
    int endIndex;
} subkeyInterval;

typedef struct deltaBitmap {
    int subkeyIntervalsNum;
    subkeyInterval *subkeyIntervals;
    sds *subvalIntervals;
} deltaBitmap;

static inline void freeDeltaBitmap(deltaBitmap *deltaBm)
{
    zfree(deltaBm->subvalIntervals);
    zfree(deltaBm->subkeyIntervals);
    zfree(deltaBm);
}

/* decoded object move to exec module */
int bitmapDecodeData(swapData *data, int num, int *cfs, sds *rawkeys,
                   sds *rawvals, void *datactx, void **pdecoded)
{
    serverAssert(num >= 0);
    UNUSED(cfs);

    bitmapDataCtx *bmDatactx = datactx;

    deltaBitmap *deltaBm = zmalloc(sizeof(deltaBitmap));
    deltaBm->subvalIntervals = sds_malloc(sizeof(subkeyInterval) * num);
    bmDatactx->subkeysLogicIdx = zmalloc(num * sizeof(int));

    uint64_t version = swapDataObjectVersion(data);
    int preSubkeyIdx = -1;

    int intervalCursor = 0;
    int subkeysCursor = 0;
    for (int i = 0; i < num; i++) {
        int dbid;
        const char *keystr, *subkeystr;
        size_t klen, slen;
        robj *subvalobj;
        uint64_t subkey_version;
        int subkeyIdx;

        if (rocksDecodeDataKey(rawkeys[i],sdslen(rawkeys[i]),
                               &dbid,&keystr,&klen,&subkey_version,&subkeystr,&slen) < 0)
            continue;
        if (version != subkey_version)
            continue;

        subkeyIdx = decodeSubkeyIdx(subkeystr, slen);
        if (subkeyIdx == -1) {
            /* "foo" subkey */
            continue;
        }

        bmDatactx->subkeysLogicIdx[subkeysCursor++] = subkeyIdx;

        if (preSubkeyIdx == -1 || subkeyIdx != preSubkeyIdx + 1) {
            /* 1、 it's a first subkey. 2、is not consecutive with the previous subkey */
            intervalCursor++;
            deltaBm->subkeyIntervals[intervalCursor].startIndex = subkeyIdx;
            deltaBm->subkeyIntervals[intervalCursor].endIndex = subkeyIdx;
        } else if (subkeyIdx == preSubkeyIdx + 1) {
            deltaBm->subkeyIntervals[intervalCursor].endIndex = subkeyIdx;
        }

        sds subval = NULL;
        if (rawvals[i] == NULL) {
            continue;
        } else {
            subvalobj = rocksDecodeValRdb(rawvals[i]);
            serverAssert(subvalobj->type == OBJ_BITMAP);
            /* subvalobj might be shared integer, unshared it before
             * add to decoded. */
            subvalobj = unshareStringValue(subvalobj);
            /* steal subvalobj sds */
            subval = subvalobj->ptr;
            subvalobj->ptr = NULL;
            decrRefCount(subvalobj);
        }

        if (deltaBm->subkeyIntervals[intervalCursor].endIndex == deltaBm->subkeyIntervals[intervalCursor].startIndex) {
            deltaBm->subvalIntervals[intervalCursor] = sdsdup(subval);
        } else {
            deltaBm->subvalIntervals[intervalCursor] = sdscat(deltaBm->subvalIntervals[intervalCursor], subval);
        }
        sdsfree(subval);
        preSubkeyIdx = subkeyIdx;
    }
    bmDatactx->subkeysNum = subkeysCursor;
    deltaBm->subkeyIntervalsNum = intervalCursor;
    *pdecoded = deltaBm;
    return 0;
}

static inline robj *mergeSubValstoNewBitmap(deltaBitmap *deltaBm)
{
    sds bitmap = sdsnewlen("", 0);
    for (int i = 0; i < deltaBm->subkeyIntervalsNum; i++) {
        bitmap = sdscat(bitmap, deltaBm->subvalIntervals[i]);
    }
    robj *res = createStringObject(bitmap, sdslen(bitmap));
    sdsfree(bitmap);
    return res;
}

static inline robj *mergeSubValstoOldBitmap(swapData *data, deltaBitmap *deltaBm)
{
    bitmapMeta *meta = swapDataGetBitmapMeta(data);

    sds oldBitmap = data->value->ptr;
    sds newBitmap = sdsnewlen("", 0);

    char *intervalAheadStartPos = oldBitmap;   /* which is already in oldBitmap, not include the inserted subkeys later */
    int intervalAheadStartIdx = 0;
    int oldSubkeysNum = 0;
    for (int i = 0; i < deltaBm->subkeyIntervalsNum; i++) {
        /* subkey of startIndex is absolutely not in old bitmap */
        int subkeysNumAhead = bitmapMetaGetHotSubkeysNum(meta, intervalAheadStartIdx, deltaBm->subkeyIntervals[i].startIndex);
        if (subkeysNumAhead == 0) {
            newBitmap = sdscatsds(newBitmap, deltaBm->subvalIntervals[i]);
            continue;
        }
        sds intervalAhead = sdsnewlen(intervalAheadStartPos, subkeysNumAhead * BITMAP_SUBKEY_SIZE);
        newBitmap = sdscatsds(newBitmap, intervalAhead);
        newBitmap = sdscatsds(newBitmap, deltaBm->subvalIntervals[i]);
        intervalAheadStartPos += subkeysNumAhead * BITMAP_SUBKEY_SIZE;
        intervalAheadStartIdx = deltaBm->subkeyIntervals[i].endIndex + 1;
        oldSubkeysNum += subkeysNumAhead;
        sdsfree(intervalAhead);
    }

    int lastOldIntervalSize = stringObjectLen(oldBitmap) - oldSubkeysNum * BITMAP_SUBKEY_SIZE;

    if (lastOldIntervalSize != 0) {
        sds lastOldInterval = sdsnewlen(intervalAheadStartPos, lastOldIntervalSize);
        newBitmap = sdscatsds(newBitmap, lastOldInterval);
        sdsfree(lastOldInterval);
    }

    robj *res = createStringObject(newBitmap, sdslen(newBitmap));
    sdsfree(newBitmap);
    return res;
}

static inline void subkeysIdxMergeIntoBitmapMeta(bitmapMeta *destMeta, int *subkeysSwapinIdx, int subkeysSwapinNum)
{
    for (int i = 0; i < subkeysSwapinNum; i++) {
        rbmSetBitRange(destMeta->subkeysInRedis, subkeysSwapinIdx[i], subkeysSwapinIdx[i]);
    }
}

void *bitmapCreateOrMergeObject(swapData *data, void *decoded_, void *datactx)
{
    UNUSED(datactx);
    robj *result = NULL;
    deltaBitmap *deltaBm = (deltaBitmap *)decoded_;
    bitmapDataCtx* bmCtx = (bitmapDataCtx*)datactx;

    if (swapDataIsCold(data)) {
        result = mergeSubValstoNewBitmap(deltaBm);
    } else {
        result = mergeSubValstoOldBitmap(data, deltaBm);
    }

    freeDeltaBitmap(deltaBm);
    deltaBm = NULL;

    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    /* update meta */
    subkeysIdxMergeIntoBitmapMeta(meta, bmCtx->subkeysLogicIdx, bmCtx->subkeysNum);

    /* bitmap only swap in the subkeys that don't exist in redis, so no need to judge like hash, set. */
    meta->subkeysNumOnlyRocks -= bmCtx->subkeysNum;  /* todo 查看边界情况， 是否可能为 负数.*/
    return result;
}

static inline robj *createSwapInBitmapObject(robj *newval) {
    robj *swapin = newval;
    serverAssert(newval && newval->type == OBJ_STRING);
    clearObjectDirty(swapin);
    clearObjectPersistKeep(swapin);
    return swapin;
}

int bitmapSwapIn(swapData *data, void *result, void *datactx) {
    /* hot key no need to swap in, this must be a warm or cold key. */
    serverAssert(swapDataPersisted(data));

    if (result == NULL) {
        if (data->value) overwriteObjectPersistent(data->value,!data->persistence_deleted);
        return 0;
    }

    if (swapDataIsCold(data) /* may be empty */) {
        /* cold key swapped in result. */
        robj *swapin = createSwapInBitmapObject(result);
        /* mark persistent after data swap in without
         * persistence deleted, or mark non-persistent else */
        overwriteObjectPersistent(swapin,!data->persistence_deleted);
        dbAdd(data->db,data->key,swapin);
        /* expire will be swapped in later by swap framework. */
        if (data->cold_meta) {
            dbAddMeta(data->db, data->key, data->cold_meta);
            data->cold_meta = NULL; /* moved */
        }
    } else {
        /* update bitmap obj */
        robj *swapin = createSwapInBitmapObject(result);
        overwriteObjectPersistent(swapin,!data->persistence_deleted);
        dbOverwrite(data->db, data->key, swapin);
    }

    return 0;
}

static inline robj *bitmapObjectFreeColdSubkeys(robj *value, bitmapDataCtx *datactx)
{
    size_t bitmapSize = stringObjectLen(value);

    sds newbuf = sdsnewlen(value->ptr + datactx->subkeysSize, bitmapSize - datactx->subkeysSize);
    robj *res = createStringObject(newbuf, sdslen(newbuf));
    sdsfree(newbuf);
    return res;
}

int bitmapCleanObject(swapData *data, void *datactx_, int keep_data) {
    if (swapDataIsCold(data)) return 0;

    bitmapDataCtx *datactx = datactx_;
    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    if (!keep_data) {
        meta->subkeysNumOnlyRocks += datactx->subkeysNum;

        robj *newbitmap = bitmapObjectFreeColdSubkeys(data->value, datactx);
        dbOverwrite(data->db, data->key, newbitmap);
        data->value = newbitmap;
    }

    setObjectPersistent(data->value); /* loss pure hot and persistent data exist. */

    /* update the subkey status in meta*/
    for (int i = 0; i < datactx->subkeysNum; i++) {  /* todo 在这里 可能早了 . */
        bitmapMetaSetSubkeyStatus(meta, datactx->subkeysLogicIdx[i], datactx->subkeysLogicIdx[i], 0);
    }
}

/* subkeys already cleaned by cleanObject(to save cpu usage of main thread),
 * swapout only updates db.dict keyspace, meta (db.meta/db.expire) swapped
 * out by swap framework. */
int bitmapSwapOut(swapData *data, void *datactx_, int keep_data, int *totally_out) {
    UNUSED(datactx_), UNUSED(keep_data);
    serverAssert(!swapDataIsCold(data));

    if (keep_data) {
        clearObjectDataDirty(data->value);
        setObjectPersistent(data->value);
    }

    if (stringObjectLen(data->value) == 0) {
        /* all subkeys swapped out, key turnning into cold:
         * - rocks-meta should have already persisted.
         * - object_meta and value will be deleted by dbDelete, expire already
         *   deleted by swap framework. */
        dbDelete(data->db,data->key);
        if (totally_out) *totally_out = 1;
    } else {
        /* not all subkeys swapped out.  bitmap object has been updated in cleanObject. */
        if (totally_out) *totally_out = 0;
    }
    return 0;
}

static inline void mockBitmapForDeleteIfCold(swapData *data)
{
    if (swapDataIsCold(data)) {
        dbAdd(data->db, data->key, createStringObject("",0));
    }
}

int bitmapSwapDel(swapData *data, void *datactx_, int del_skip) {
    bitmapDataCtx* datactx = (bitmapDataCtx*)datactx_;
    if (datactx->ctx_flag == BIG_DATA_CTX_FLAG_MOCK_VALUE) {
        mockBitmapForDeleteIfCold(data);
    }

    if (del_skip) {
        if (!swapDataIsCold(data))
            dbDeleteMeta(data->db,data->key);
        return 0;
    } else {
        if (!swapDataIsCold(data))
            /* both value/object_meta/expire are deleted */
            dbDelete(data->db,data->key);
        return 0;
    }
}

void freeBitmapSwapData(swapData *data_, void *datactx_) {
    UNUSED(data_);
    bitmapDataCtx *datactx = datactx_;
    zfree(datactx->subkeysLogicIdx);
    zfree(datactx);
}

int bitmapBeforeCall(swapData *data, client *c, void *datactx_)
{
    bitmapDataCtx *datactx = datactx_;
    objectMeta *object_meta;

    object_meta = lookupMeta(data->db,data->key);
    serverAssert(object_meta != NULL && object_meta->object_type == OBJ_BITMAP);

    bitmapMeta *bitmap_meta = objectMetaGetPtr(object_meta);

    if (swapDataIsHot(data)) {
        /* bitmap is purely hot, never swapped out */
        return 0;
    }

    for (int i = 0; i < 2; i++) {
        argRewriteRequest arg_req = datactx->arg_reqs[i];
        if (arg_req.arg_idx <= 0) continue;
        long long bitOffset;
        int ret;

        if (arg_req.mstate_idx < 0) {
            ret = getLongLongFromObject(c->argv[arg_req.arg_idx],&bitOffset);
        } else {
            serverAssert(arg_req.mstate_idx < c->mstate.count);
            ret = getLongLongFromObject(c->mstate.commands[arg_req.mstate_idx].argv[arg_req.arg_idx],&bitOffset);
        }

        serverAssert(ret == C_OK);

        int subkeyIdx = bitOffset / BITS_NUM_IN_SUBKEY;
        if (subkeyIdx == 0) {
            continue;
        }
        int subkeysNumAhead = bitmapMetaGetHotSubkeysNum(bitmap_meta, 0, subkeyIdx - 1);

        if (subkeysNumAhead == subkeyIdx) {
            /* no need to modify offset */
            continue;
        } else {
            long long offsetInSubkey = bitOffset - subkeyIdx * BITS_NUM_IN_SUBKEY;
            long long newOffset = subkeysNumAhead * BITMAP_SUBKEY_SIZE + offsetInSubkey;

            robj *new_arg = createObject(OBJ_STRING,sdsfromlonglong(newOffset));
            clientArgRewrite(c, arg_req, new_arg);
        }
    }

    return 0;
}

// todo check
/*
 * decrRef and incrRef operation
 * memory free
 * sds  robj operation
 * */

swapDataType bitmapSwapDataType = {
        .name = "bitmap",
        .cmd_swap_flags = CMD_SWAP_DATATYPE_BITMAP,
        .swapAna = bitmapSwapAna,
        .swapAnaAction = bitmapSwapAnaAction,
        .encodeKeys = bitmapEncodeKeys,
        .encodeData = bitmapEncodeData,
        .encodeRange = bitmapEncodeRange,
        .decodeData = bitmapDecodeData,
        .swapIn = bitmapSwapIn,
        .swapOut = bitmapSwapOut,
        .swapDel = bitmapSwapDel,
        .createOrMergeObject = bitmapCreateOrMergeObject,
        .cleanObject = bitmapCleanObject,
        .beforeCall = bitmapBeforeCall,
        .free = freeBitmapSwapData,
        .rocksDel = NULL,
        .mergedIsHot = bitmapMergedIsHot,
        .getObjectMetaAux = NULL,
};

// todo  add bitmap type
int swapDataSetupBitmap(swapData *d, void **pdatactx) {
    d->type = &bitmapSwapDataType;
    d->omtype = &bitmapObjectMetaType;
    bitmapDataCtx *datactx = zmalloc(sizeof(bitmapDataCtx));
    *pdatactx = datactx;
    return 0;
}

static inline void bitmapMetaGrow(bitmapMeta *bitmap_meta, int extendSize)
{
    if (!bitmap_meta) {
        return;
    }
    if (extendSize <= 0) {
        return;
    }
    bitmap_meta->bitmapSize += extendSize;
    int subkeysSum = (bitmap_meta->bitmapSize - 1) / BITMAP_SUBKEY_SIZE + 1;
    int oldSubkeysSum = bitmap_meta->subkeysSum;
    bitmap_meta->subkeysSum = MAX(subkeysSum, bitmap_meta->subkeysSum);
    rbmSetBitRange(bitmap_meta->subkeysInRedis, oldSubkeysSum, bitmap_meta->subkeysSum - 1);
}

sds ctripGrowBitmap(client *c, sds s, size_t byte)
{
    size_t oldlen = sdslen(s);
    s = sdsgrowzero(s, byte);
    size_t newlen = sdslen(s);
    if (newlen <= oldlen) {
        return s;
    }
    /* newlen > oldlen */
    objectMeta *om = lookupMeta(c->db,c->argv[1]);
    if (om != NULL) {
        bitmapMetaGrow(objectMetaGetPtr(om), newlen - oldlen);
    }
    return s;
}

#ifdef REDIS_TEST
#define SWAP_EVICT_STEP 2
#define SWAP_EVICT_MEM  (1*1024*1024)

#define INIT_SAVE_SKIP -2

int swapDataBitmapTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);

    initTestRedisServer();
    redisDb* db = server.db + 0;
    int error = 0;
    robj *bitmap_key1, *bitmap1, *cold_key1;
    keyRequest _keyReq, *keyReq1 = &_keyReq, _cold_keyReq, *cold_keyReq1 = &_cold_keyReq;
    swapData *bitmap1_data, *cold1_data;
    objectMeta *pureHot1_meta, *cold1_meta;
    bitmapDataCtx *bitmap1_ctx, *cold1_ctx = NULL;
    int intention;
    uint32_t intention_flags;

    int originEvictStepMaxSubkey = server.swap_evict_step_max_subkeys;
    int originEvictStepMaxMemory = server.swap_evict_step_max_memory;

    TEST("bitmap - init") {
        server.swap_evict_step_max_subkeys = SWAP_EVICT_STEP;
        server.swap_evict_step_max_memory = SWAP_EVICT_MEM;

        bitmap_key1 = createStringObject("bitmap_key1",11);
        pureHot1_meta = createBitmapObjectMeta(0, NULL);

        /* two subkeys in hot bitmap*/
        sds str = sdsnewlen("", BITMAP_SUBKEY_SIZE * 2);
        bitmap1 = createStringObject(str, sdslen(str));
        dbAdd(db,bitmap_key1,bitmap1);

        incrRefCount(bitmap_key1);
        keyReq1->key = bitmap_key1;
        keyReq1->type = KEYREQUEST_TYPE_SUBKEY;
        keyReq1->level = REQUEST_LEVEL_KEY;
        cold_keyReq1->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        keyReq1->l.num_ranges = 0;
        keyReq1->l.ranges = NULL;

        cold_key1 = createStringObject("cold_key1",9);
        cold1_meta = createBitmapObjectMeta(0, NULL);

        /* 3 subkeys in cold bitmap*/
        int bitmapSize = 3 * BITMAP_SUBKEY_SIZE;
        sds coldBitmapSize = sdsnewlen(&bitmapSize, sizeof(int));
        decodeBitmapObjectMeta(cold1_meta, coldBitmapSize, sizeof(int));

        incrRefCount(bitmap_key1);
        cold_keyReq1->key = bitmap_key1;
        cold_keyReq1->level = REQUEST_LEVEL_KEY;
        cold_keyReq1->type = KEYREQUEST_TYPE_SUBKEY;
        cold_keyReq1->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        cold_keyReq1->l.num_ranges = 0;
        cold_keyReq1->l.ranges = NULL;

        bitmap1_data = createSwapData(db, bitmap_key1, bitmap1, NULL);
        swapDataSetupMeta(bitmap1_data,OBJ_BITMAP, -1,(void**)&bitmap1_ctx);
        swapDataSetObjectMeta(bitmap1_data, pureHot1_meta);

        cold1_data = createSwapData(db, cold_key1, NULL, NULL);
        swapDataSetupMeta(cold1_data, OBJ_BITMAP, -1, (void**)&cold1_ctx);
        swapDataSetObjectMeta(cold1_data, cold1_meta);
    }

    TEST("bitmap - swapAna") {
        /* nop: NOP/IN_META/IN_DEL/IN hot/OUT cold... */
        keyReq1->cmd_intention = SWAP_NOP, keyReq1->cmd_intention_flags = 0;
        swapDataAna(bitmap1_data, 0, keyReq1, &intention, &intention_flags, bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_IN_META;
        swapDataAna(bitmap1_data, 0, keyReq1, &intention, &intention_flags, bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = 0;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_IN_DEL_MOCK_VALUE;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_IN_OVERWRITE;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_METASCAN_RANDOMKEY;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_METASCAN_SCAN;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        keyReq1->cmd_intention = SWAP_IN, keyReq1->cmd_intention_flags = SWAP_IN_FORCE_HOT;
        swapDataAna(bitmap1_data,0,keyReq1,&intention,&intention_flags,bitmap1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        cold_keyReq1->cmd_intention = SWAP_OUT, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_keyReq1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        cold_keyReq1->cmd_intention = SWAP_DEL, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold1_data, 0, cold_keyReq1, &intention, &intention_flags, cold1_ctx);
        test_assert(intention == SWAP_DEL && intention_flags == 0);

        /* in: entire or with subkeys */
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold1_data, 0, cold_keyReq1, &intention, &intention_flags, cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->subkeysNum == -1 && cold1_ctx->subkeysLogicIdx == NULL);

        range *range1 = zmalloc(sizeof(range));
        range1->start = 0;
        range1->end = BITS_NUM_IN_SUBKEY * 3 - 1;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_keyReq1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->subkeysNum == -1);

        range1->start = 0;
        range1->end = BITS_NUM_IN_SUBKEY * 2 - 1;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_keyReq1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->subkeysNum == 2);
        test_assert(cold1_ctx->subkeysLogicIdx[0] == 0);
        test_assert(cold1_ctx->subkeysLogicIdx[1] == 1);

        zfree(range1);
    }

    server.swap_evict_step_max_subkeys = originEvictStepMaxSubkey;
    server.swap_evict_step_max_memory = originEvictStepMaxMemory;

    return error;
}

#endif