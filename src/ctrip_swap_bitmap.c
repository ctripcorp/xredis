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
#include "ctrip_roaring_bitmap.h"

#define BITMAP_SUBKEY_SIZE 4096  /* default 4KB */
#define BITS_NUM_IN_SUBKEY 32768
#define BITMAP_MAX_INDEX LONG_MAX

#define BITMAP_GET_SUBKEYS_NUM(size)  ((size - 1) / BITMAP_SUBKEY_SIZE + 1)

/* utils */

static inline sds encodeBitmapSize(long size)
{
    size = htonu64(size);
    return sdsnewlen(&size, sizeof(long));
}

static inline long decodeBitmapSize(const char *str)
{
    long size_be = *(long*)str;
    return ntohu64(size_be);
}

static inline sds bitmapEncodeSubkeyIdx(long idx) {
    idx = htonu64(idx);
    return sdsnewlen(&idx,sizeof(idx));
}

static inline long bitmapDecodeSubkeyIdx(const char *str, size_t len) {
    serverAssert(len == sizeof(long));
    long idx_be = *(long*)str;
    long idx = ntohu64(idx_be);
    return idx;
}

/* bitmap meta */

typedef struct bitmapMeta {
    size_t size;
    int pure_cold_subkeys_num;
    roaringBitmap *subkeys_status;  /* set status as 1, if subkey is hot. */
} bitmapMeta;

bitmapMeta *bitmapMetaCreate(void) {
    bitmapMeta *bitmap_meta = zmalloc(sizeof(bitmapMeta));
    bitmap_meta->size = 0;
    bitmap_meta->pure_cold_subkeys_num = 0;
    bitmap_meta->subkeys_status = rbmCreate();
    return bitmap_meta;
}

void bitmapMetaFree(bitmapMeta *bitmap_meta) {
    if (bitmap_meta == NULL) return;
    rbmDestory(bitmap_meta->subkeys_status);
    zfree(bitmap_meta);
}

static inline void bitmapMetaInit(bitmapMeta *meta, robj *value)
{
    meta->size = stringObjectLen(value);
    meta->pure_cold_subkeys_num = 0;
    rbmSetBitRange(meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(meta->size) - 1);
}

/*
* todo 将在Meta机制修改中添加，首次创建bitmap 对象时， data->new_meta = createBitmapObjectMeta(swapGetAndIncrVersion(),bitmap_meta)，bitmap_meta暂时为NULL，在后续首次 swap out 时 set。
*/

static inline sds bitmapMetaEncode(bitmapMeta *bm) {
    if (bm == NULL) return NULL;
    return encodeBitmapSize(bm->size);
}

static inline bitmapMeta *bitmapMetaDecode(const char *extend, size_t extend_len) {
    if (extend_len != sizeof(long)) return NULL;
    bitmapMeta *bitmap_meta = bitmapMetaCreate();

    bitmap_meta->size = decodeBitmapSize(extend);
    bitmap_meta->pure_cold_subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size);
    return bitmap_meta;
}

static inline bitmapMeta *bitmapMetaDup(bitmapMeta *bitmap_meta) {
    bitmapMeta *meta = zmalloc(sizeof(bitmapMeta));
    meta->size = bitmap_meta->size;
    meta->pure_cold_subkeys_num = bitmap_meta->pure_cold_subkeys_num;
    meta->subkeys_status = rbmCreate();
    rbmdup(meta->subkeys_status, bitmap_meta->subkeys_status);
    return meta;
}

int bitmapMetaEqual(bitmapMeta *dest_meta, bitmapMeta *src_meta) {
    /* todo  此处还需比较 size, redisStatus 等成员， 且须*/
    return dest_meta->pure_cold_subkeys_num == src_meta->pure_cold_subkeys_num;
}

static inline int bitmapMetaGetHotSubkeysNum(bitmapMeta *bitmap_meta, int start_subkey_idx, int end_subkey_idx)
{
    if (bitmap_meta == NULL) {
        return 0;
    }
    serverAssert(start_subkey_idx <= end_subkey_idx && start_subkey_idx >= 0);
    return rbmGetBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
}

#define HOT_STATUS 1
#define COLD_STATUS 0

static inline void bitmapMetaSetSubkeyStatus(bitmapMeta *bitmap_meta, int start_subkey_idx, int end_subkey_idx, int status)
{
    serverAssert(start_subkey_idx <= end_subkey_idx && start_subkey_idx >= 0);
    if (status) {
        rbmSetBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
    } else {
        rbmClearBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
    }
}

static inline int bitmapMetaGetSubkeyStatus(bitmapMeta *bitmap_meta, int start_subkey_idx, int end_subkey_idx)
{
    if (bitmap_meta->subkeys_status == NULL) {
        return 0;
    }
    serverAssert(start_subkey_idx <= end_subkey_idx && start_subkey_idx >= 0);
    return rbmGetBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
}

/* count subkeys_num hot subkeys from subkey idx = 0 in meta->subkeys_status, return the actual selected hot subkeys number,
 * output the subkeys logic idxs(idx array alloc by caller).
 * */
static inline int bitmapMetaGetHotSubkeysIdx(bitmapMeta *meta, int subkeys_num, int *subkeys_idx)
{
    if (meta == NULL || meta->subkeys_status == NULL || subkeys_num == 0 || subkeys_idx == NULL) {
        return 0;
    }
    return rbmGetBitPos(meta->subkeys_status, subkeys_num, subkeys_idx);
}

static inline void bitmapMetaGrow(bitmapMeta *bitmap_meta, int extendSize)
{
    if (!bitmap_meta) {
        return;
    }
    if (extendSize <= 0) {
        return;
    }
    int old_subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size);
    bitmap_meta->size += extendSize;
    int subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size);

    subkeys_num = MAX(subkeys_num, old_subkeys_num);
    rbmSetBitRange(bitmap_meta->subkeys_status, old_subkeys_num, subkeys_num - 1);
}

/* bitmap object meta */

objectMeta *createBitmapObjectMeta(uint64_t version, bitmapMeta *bitmap_meta) {
    objectMeta *object_meta = createObjectMeta(OBJ_BITMAP, version);
    objectMetaSetPtr(object_meta, bitmap_meta);
    return object_meta;
}

void bitmapObjectMetaFree(objectMeta *object_meta) {
    if (object_meta == NULL) return;
    bitmapMetaFree(objectMetaGetPtr(object_meta));
    objectMetaSetPtr(object_meta, NULL);
}

sds bitmapObjectMetaEncode(struct objectMeta *object_meta, void *aux) {
    UNUSED(aux);
    if (object_meta == NULL) return NULL;
    serverAssert(object_meta->object_type == OBJ_BITMAP);
    return bitmapMetaEncode(objectMetaGetPtr(object_meta));
}

int bitmapObjectMetaDecode(struct objectMeta *object_meta, const char *extend, size_t extlen) {
    serverAssert(object_meta->object_type == OBJ_BITMAP);
    serverAssert(objectMetaGetPtr(object_meta) == NULL);
    objectMetaSetPtr(object_meta, bitmapMetaDecode(extend, extlen));
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
        return meta->pure_cold_subkeys_num == 0;
    }
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
    return bitmapMetaEqual(dest_meta, src_meta);
}

int bitmapObjectMetaRebuildFeed(struct objectMeta *rebuild_meta,
                              uint64_t version, const char *subkey, size_t sublen) {
    UNUSED(version);
    bitmapMeta *meta = objectMetaGetPtr(rebuild_meta);
    if (sublen != sizeof(long)) return -1;
    if (subkey) {
        meta->pure_cold_subkeys_num++;
        return 0;
    } else {
        return -1;
    }
}

objectMetaType bitmapObjectMetaType = {
        .encodeObjectMeta = bitmapObjectMetaEncode,
        .decodeObjectMeta = bitmapObjectMetaDecode,
        .objectIsHot = bitmapObjectMetaIsHot,
        .free = bitmapObjectMetaFree,
        .duplicate = bitmapObjectMetaDup,  /* only used in rdbKeySaveDataInitCommon. */
        .equal = bitmapObjectMetaEqual,   /* only used in keyLoadFixAna. */
        .rebuildFeed = bitmapObjectMetaRebuildFeed
};

/* delta bitmap */

typedef struct subkeyInterval {
    int start_index;
    int end_index;
} subkeyInterval;

typedef struct deltaBitmap {
    int subkey_intervals_num;
    subkeyInterval *subkey_intervals;
    sds *subval_intervals;
    int subkeys_num;
    int *subkeys_logic_idx;
} deltaBitmap;

static inline deltaBitmap *deltaBitmapCreate()
{
    return zmalloc(sizeof(deltaBitmap));
}

static inline void deltaBitmapFree(deltaBitmap *delta_bm)
{
    zfree(delta_bm->subval_intervals);
    zfree(delta_bm->subkey_intervals);
    zfree(delta_bm->subkeys_logic_idx);
    zfree(delta_bm);
}

static inline robj *deltaBitmapTransToBitmapObject(deltaBitmap *delta_bm)
{
    sds bitmap = sdsnewlen("", 0);
    for (int i = 0; i < delta_bm->subkey_intervals_num; i++) {
        bitmap = sdscatsds(bitmap, delta_bm->subval_intervals[i]);
    }
    robj *res = createStringObject(bitmap, sdslen(bitmap));
    sdsfree(bitmap);
    return res;
}

robj *deltaBitmapMergeIntoOldBitmapObject(deltaBitmap *delta_bm, robj *old_bitmap_object, bitmapMeta *meta)
{
    sds old_bitmap = old_bitmap_object->ptr;
    sds new_bitmap = sdsnewlen("", 0);

    char *interval_ahead_start_pos = old_bitmap;   /* which is already in old_bitmap, not include the inserted subkeys later */
    int interval_ahead_start_idx = 0;
    int old_subkeys_num = 0;
    for (int i = 0; i < delta_bm->subkey_intervals_num; i++) {
        /* subkey of start_index is absolutely not in old bitmap */
        int subkeys_num_ahead = bitmapMetaGetHotSubkeysNum(meta, interval_ahead_start_idx, delta_bm->subkey_intervals[i].start_index);
        if (subkeys_num_ahead == 0) {
            new_bitmap = sdscatsds(new_bitmap, delta_bm->subval_intervals[i]);
            continue;
        }
        sds interval_ahead = sdsnewlen(interval_ahead_start_pos, subkeys_num_ahead * BITMAP_SUBKEY_SIZE);
        new_bitmap = sdscatsds(new_bitmap, interval_ahead);
        new_bitmap = sdscatsds(new_bitmap, delta_bm->subval_intervals[i]);
        interval_ahead_start_pos += subkeys_num_ahead * BITMAP_SUBKEY_SIZE;
        interval_ahead_start_idx = delta_bm->subkey_intervals[i].end_index + 1;
        old_subkeys_num += subkeys_num_ahead;
        sdsfree(interval_ahead);
    }

    int last_old_interval_size = stringObjectLen(old_bitmap_object) - old_subkeys_num * BITMAP_SUBKEY_SIZE;

    if (last_old_interval_size != 0) {
        sds last_old_interval = sdsnewlen(interval_ahead_start_pos, last_old_interval_size);
        new_bitmap = sdscatsds(new_bitmap, last_old_interval);
        sdsfree(last_old_interval);
    }

    robj *res = createStringObject(new_bitmap, sdslen(new_bitmap));
    sdsfree(new_bitmap);
    return res;
}

typedef struct bitmapDataCtx {
    int ctx_flag;
    int subkeys_total_size;  /* only used in swap out */
    int subkeys_num;
    int *subkeys_logic_idx;
    argRewriteRequest arg_reqs[2];
} bitmapDataCtx;

bitmapDataCtx *bitmapDataCtxCreate()
{
    return zmalloc(sizeof(bitmapDataCtx));
}

void bitmapDataCtxFree(bitmapDataCtx *datactx)
{
    zfree(datactx->subkeys_logic_idx);
    zfree(datactx);
}

void bitmapDataCtxReset(bitmapDataCtx *datactx)
{
    zfree(datactx->subkeys_logic_idx);
    memset(datactx, 0, sizeof(bitmapDataCtx));
}

/* bitmap swap ana */

static inline bitmapMeta *swapDataGetBitmapMeta(swapData *data) {
    objectMeta *object_meta = swapDataObjectMeta(data);
    return object_meta ? objectMetaGetPtr(object_meta) : NULL;
}

static inline void bitmapSwapAnaInSelectSubKeys(bitmapDataCtx *datactx, bitmapMeta *meta, bool is_write_req, range *range)
{
    int required_subkey_start_idx = 0;
    int required_subkey_end_idx = 0;

    if (range->type == BITMAP_BIT_RANGE) {
        required_subkey_start_idx = range->start / BITS_NUM_IN_SUBKEY;
        required_subkey_end_idx = range->end / BITS_NUM_IN_SUBKEY;
    } else {
        /* BITMAP_BYTE_RANGE */
        required_subkey_start_idx = range->start / BITMAP_SUBKEY_SIZE;
        if (range->end == -1) {
            required_subkey_end_idx = BITMAP_GET_SUBKEYS_NUM(meta->size); - 1;
        } else {
            required_subkey_end_idx = range->end / BITMAP_SUBKEY_SIZE;
        }
    }

    /* when subkeys range required exceed the max subkey idx,
     * if it's a write operation, then redis will extend for a bigger new bitmap.
     * so some operations here will be a little tricky, we need ensure the last subkey will be swap in.
     * if it's a read operation, there is no extension operation in redis,
     * we don't need to the last subkey swaped in. */

    /*
     * 1、 required range exceed subkeysSum - 1:
     *    1) we need to swap in the subkeys in the range of [required_subkey_start_idx, subkeysSum - 1]
     *      PS: (if required_subkey_start_idx > subkeysSum - 1) ,  swap in the last subkey (idx = subkeysSum - 1);
     *
     * 2、  required range not exceed the max subkey idx:
     *    1) swap in the subkeys required;
     * */

    int subkeys_swap_in_start_idx = required_subkey_start_idx;
    int subkeys_swap_in_end_idx = required_subkey_end_idx;

    int subkeys_num = BITMAP_GET_SUBKEYS_NUM(meta->size);

    if (is_write_req) {
        if (required_subkey_start_idx > subkeys_num - 1) {
            subkeys_swap_in_start_idx = subkeys_num - 1;
        }

        if (required_subkey_end_idx > subkeys_num - 1) {
            subkeys_swap_in_end_idx = subkeys_num - 1;
        }
    } else {
        if (required_subkey_start_idx > subkeys_num - 1) {
            datactx->subkeys_num = 0;
            return;
        }
        if (required_subkey_end_idx > subkeys_num - 1) {
            subkeys_swap_in_end_idx = subkeys_num - 1;
        }
    }

    int subkey_num_need_swapin = subkeys_swap_in_end_idx - subkeys_swap_in_start_idx + 1 - bitmapMetaGetHotSubkeysNum(meta, subkeys_swap_in_start_idx, subkeys_swap_in_end_idx);

    /* subkeys required are all in redis */
    if (subkey_num_need_swapin == 0) {
        /* subkeys required have been in redis.*/
        datactx->subkeys_num = 0;
        return;
    }

    /* subkeys required are not all in redis */
    if (subkey_num_need_swapin == subkeys_num) {
        /* all subKey of bitmap need to swap in */
        datactx->subkeys_num = -1;
        return;
    }

    datactx->subkeys_logic_idx = zmalloc(sizeof(robj*) * subkey_num_need_swapin);
    int cursor = 0;
    /* record idx of subkey to swap in. */
    for (int i = subkeys_swap_in_start_idx; i <= subkeys_swap_in_end_idx; i++) {
        if (bitmapMetaGetHotSubkeysNum(meta, i, i) == 0) {
            datactx->subkeys_logic_idx[cursor++] = i;
        }
    }
    datactx->subkeys_num = cursor;
}

#define SELECT_MAIN 0
#define SELECT_DSS  1

int bitmapSwapAnaOutSelectSubkeys(swapData *data, bitmapDataCtx *datactx, int *may_keep_data)
{
    int noswap;
    bitmapMeta *meta = swapDataGetBitmapMeta(data);

    int hot_subkeys_num = bitmapMetaGetHotSubkeysNum(meta, 0, BITMAP_GET_SUBKEYS_NUM(meta->size) - 1);
    int subkeys_num_may_swapout = 0;

    /* only SELECT_MAIN */
    if (objectIsDataDirty(data->value)) { /* all subkeys might be dirty */
        subkeys_num_may_swapout = hot_subkeys_num;
        noswap = 0;
    } else {
        /* If data dirty, meta will be persisted as an side effect.
         * If just meta dirty, we still persists meta.
         * If data & meta clean, we persists nothing (just free). */
        if (objectIsMetaDirty(data->value)) { /* meta dirty */
            /* meta dirty */
            subkeys_num_may_swapout = 0;
            noswap = 0;
        } else { /* clean */
            subkeys_num_may_swapout = hot_subkeys_num;
            noswap = 1;
        }
    }

    subkeys_num_may_swapout = MIN(server.swap_evict_step_max_subkeys, subkeys_num_may_swapout);
    if (!noswap) *may_keep_data = 0;

    /* from left to right to select subkeys */
    for (int i = 0; i < subkeys_num_may_swapout; i++) {

        int subval_size = 0;
        int left_size = stringObjectLen(data->value) - BITMAP_SUBKEY_SIZE * i;
        if (left_size > BITMAP_SUBKEY_SIZE) {
            subval_size = BITMAP_SUBKEY_SIZE;
        } else {
            subval_size = left_size;
        }

        if (datactx->subkeys_total_size + subval_size > server.swap_evict_step_max_memory) {
            break;
        }

        datactx->subkeys_total_size += subval_size;
        datactx->subkeys_num += 1;
    }

    datactx->subkeys_logic_idx = zmalloc(sizeof(int) * datactx->subkeys_num);
    int num = bitmapMetaGetHotSubkeysIdx(meta, datactx->subkeys_num, datactx->subkeys_logic_idx);
    serverAssert(num == datactx->subkeys_num);

    return noswap;
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
                    if (meta->pure_cold_subkeys_num == 0) {
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
                        datactx->subkeys_num = 0;
                        datactx->subkeys_logic_idx = zmalloc(sizeof(int));
                        datactx->subkeys_logic_idx[datactx->subkeys_num++] = -1;
                        *intention = SWAP_IN;
                        *intention_flags = 0;
                    }
                } else {
                    /* string, keyspace ... operation */
                    /* swap in all subkeys, and keep them in rocks. */
                    datactx->subkeys_num = -1;
                    *intention = SWAP_IN;
                    *intention_flags = 0;
                }
            } else { /* range requests */
                bitmapSwapAnaInSelectSubKeys(datactx, meta, cmd_intention_flags == SWAP_IN_DEL, req->l.ranges);

                *intention = datactx->subkeys_num == 0 ? SWAP_NOP : SWAP_IN;
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
                    bitmapMetaInit(meta, data->value);
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
            if (datactx->subkeys_num > 0) *action = ROCKS_GET; /* Swap in specific fields */
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

static inline sds bitmapEncodeSubval(robj *subval) {
    return rocksEncodeValRdb(subval);
}

int bitmapEncodeKeys(swapData *data, int intention, void *datactx_,
                     int *numkeys, int **pcfs, sds **prawkeys) {
    bitmapDataCtx *datactx = datactx_;
    sds *rawkeys = NULL;
    int *cfs = NULL;
    uint64_t version = swapDataObjectVersion(data);

    if (datactx->subkeys_num <= 0) {
        return 0;
    }
    serverAssert(intention == SWAP_IN);
    cfs = zmalloc(sizeof(int) * datactx->subkeys_num);
    rawkeys = zmalloc(sizeof(sds) * datactx->subkeys_num);
    for (int i = 0; i < datactx->subkeys_num; i++) {
        cfs[i] = DATA_CF;
        sds keyStr;
        if (datactx->subkeys_logic_idx[i] == -1) {
            keyStr = sdsnewlen("foo", 3);
        } else {
            keyStr = bitmapEncodeSubkeyIdx(datactx->subkeys_logic_idx[i]);
        }
        rawkeys[i] = bitmapEncodeSubkey(data->db,data->key->ptr, version, keyStr);
        sdsfree(keyStr);
    }
    *numkeys = datactx->subkeys_num;
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

robj *bitmapGetSubVal(robj *o, int phyIdx)
{
    int subval_size = 0;
    int left_size = stringObjectLen(o) - BITMAP_SUBKEY_SIZE * phyIdx;
    if (left_size < BITMAP_SUBKEY_SIZE) {
        subval_size = left_size;
    } else {
        subval_size = BITMAP_SUBKEY_SIZE;
    }
    return createStringObject(o->ptr + phyIdx * BITMAP_SUBKEY_SIZE, subval_size);
}

int bitmapEncodeData(swapData *data, int intention, void *datactx_,
                     int *numkeys, int **pcfs, sds **prawkeys, sds **prawvals) {
    serverAssert(intention == SWAP_OUT);

    bitmapDataCtx *datactx = datactx_;

    if (datactx->subkeys_num == 0) {
        return 0;
    }

    int *cfs = zmalloc(datactx->subkeys_num * sizeof(int));
    sds *rawkeys = zmalloc(datactx->subkeys_num * sizeof(sds));
    sds *rawvals = zmalloc(datactx->subkeys_num * sizeof(sds));
    uint64_t version = swapDataObjectVersion(data);

    for (int i = 0; i < datactx->subkeys_num; i++) {
        cfs[i] = DATA_CF;

        long logicIdx = datactx->subkeys_logic_idx[i];
        sds keyStr = bitmapEncodeSubkeyIdx(logicIdx);

        robj *subval = bitmapGetSubVal(data->value, i);
        serverAssert(subval);
        rawvals[i] = bitmapEncodeSubval(subval);
        rawkeys[i] = bitmapEncodeSubkey(data->db, data->key->ptr, version, keyStr);
        decrRefCount(subval);
        sdsfree(keyStr);
    }
    *numkeys = datactx->subkeys_num;
    *pcfs = cfs;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    return 0;
}

/* decoded object move to exec module */
int bitmapDecodeData(swapData *data, int num, int *cfs, sds *rawkeys,
                     sds *rawvals, void **pdecoded)
{
    serverAssert(num >= 0);
    UNUSED(cfs);

    deltaBitmap *delta_bm = deltaBitmapCreate();
    delta_bm->subval_intervals = sds_malloc(sizeof(subkeyInterval) * num);
    delta_bm->subkeys_logic_idx = zmalloc(num * sizeof(int));
    delta_bm->subkey_intervals = zmalloc(num * sizeof(subkeyInterval));

    uint64_t version = swapDataObjectVersion(data);
    int pre_subkey_idx = -1;

    int interval_cursor = 0;
    int subkeys_cursor = 0;
    for (int i = 0; i < num; i++) {
        int dbid;
        const char *keystr, *subkeystr;
        size_t klen, slen;
        robj *subvalobj;
        uint64_t subkey_version;
        long subkey_idx;

        if (rocksDecodeDataKey(rawkeys[i],sdslen(rawkeys[i]),
                               &dbid,&keystr,&klen,&subkey_version,&subkeystr,&slen) < 0)
            continue;
        if (version != subkey_version)
            continue;

        if (slen == strlen("foo")) {
            /* "foo" subkey */
            continue;
        }
        if (rawvals[i] == NULL) {
            continue;
        }
        subkey_idx = bitmapDecodeSubkeyIdx(subkeystr, slen);

        delta_bm->subkeys_logic_idx[subkeys_cursor++] = subkey_idx;

        if (pre_subkey_idx == -1) {
            /* 1、 it's a first subkey. */
            delta_bm->subkey_intervals[interval_cursor].start_index = subkey_idx;
            delta_bm->subkey_intervals[interval_cursor].end_index = subkey_idx;
        } else if (subkey_idx != pre_subkey_idx + 1) {
            /* 2、is not consecutive with the previous subkey. */
            interval_cursor++;
            delta_bm->subkey_intervals[interval_cursor].start_index = subkey_idx;
            delta_bm->subkey_intervals[interval_cursor].end_index = subkey_idx;
        } else if (subkey_idx == pre_subkey_idx + 1) {
            delta_bm->subkey_intervals[interval_cursor].end_index = subkey_idx;
        }

        subvalobj = rocksDecodeValRdb(rawvals[i]);
        serverAssert(subvalobj->type == OBJ_STRING);
        /* subvalobj might be shared integer, unshared it before
         * add to decoded. */
        subvalobj = unshareStringValue(subvalobj);
        /* steal subvalobj sds */
        sds subval = subvalobj->ptr;
        subvalobj->ptr = NULL;

        if (delta_bm->subkey_intervals[interval_cursor].end_index == delta_bm->subkey_intervals[interval_cursor].start_index) {
            delta_bm->subval_intervals[interval_cursor] = sdsdup(subval);
        } else {
            delta_bm->subval_intervals[interval_cursor] = sdscatsds(delta_bm->subval_intervals[interval_cursor], subval);
        }
        sdsfree(subval);
        decrRefCount(subvalobj);
        pre_subkey_idx = subkey_idx;
    }
    delta_bm->subkeys_num = subkeys_cursor;
    delta_bm->subkey_intervals_num = interval_cursor + 1;
    *pdecoded = delta_bm;
    return 0;
}

void *bitmapCreateOrMergeObject(swapData *data, void *decoded_, void *datactx)
{
    UNUSED(datactx);
    robj *result = NULL;
    deltaBitmap *delta_bm = (deltaBitmap *)decoded_;

    if (swapDataIsCold(data)) {
        result = deltaBitmapTransToBitmapObject(delta_bm); // todo bitmapObjectCreateFromDeltaBm  类似
    } else {
        bitmapMeta *meta = swapDataGetBitmapMeta(data);
        result = deltaBitmapMergeIntoOldBitmapObject(delta_bm, data->value, meta);
    }

    bitmapMeta *meta = swapDataGetBitmapMeta(data);

    /* update meta */
    for (int i = 0; i < delta_bm->subkeys_num; i++) {
        rbmSetBitRange(meta->subkeys_status, delta_bm->subkeys_logic_idx[i], delta_bm->subkeys_logic_idx[i]);
    }
    meta->pure_cold_subkeys_num -= delta_bm->subkeys_num;

    /* bitmap only swap in the subkeys that don't exist in redis, so no need to judge like hash, set. */
    deltaBitmapFree(delta_bm);
    delta_bm = NULL;
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
    UNUSED(datactx);
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
    /* from subkey idx = 0 to right to swap out subkeys in bitmap. */
    size_t bitmap_size = stringObjectLen(value);
    return createStringObject(value->ptr + datactx->subkeys_total_size, bitmap_size - datactx->subkeys_total_size);
}

static int bitmapCleanObject(swapData *data, void *datactx_, int keep_data) {
    if (swapDataIsCold(data)) {
        return 0;
    }

    bitmapDataCtx *datactx = datactx_;
    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    if (!keep_data) {

        robj *newbitmap = bitmapObjectFreeColdSubkeys(data->value, datactx);
        dbOverwrite(data->db, data->key, newbitmap);
        data->value = newbitmap;

        /* update the subkey status in meta*/
        for (int i = 0; i < datactx->subkeys_num; i++) {
            bitmapMetaSetSubkeyStatus(meta, datactx->subkeys_logic_idx[i], datactx->subkeys_logic_idx[i], COLD_STATUS);
        }
        meta->pure_cold_subkeys_num += datactx->subkeys_num;
    }

    setObjectPersistent(data->value); /* loss pure hot and persistent data exist. */
    return 0;
}

/* subkeys already cleaned by cleanObject(to save cpu usage of main thread),
 * swapout only updates db.dict keyspace, meta (db.meta/db.expire) swapped
 * out by swap framework. */
int bitmapSwapOut(swapData *data, void *datactx_, int keep_data, int *totally_out) {
    UNUSED(datactx_);
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
        data->value = NULL;
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

void bitmapSwapDataFree(swapData *data_, void *datactx_) {
    UNUSED(data_);
    bitmapDataCtxFree(datactx_);
}

int bitmapBeforeCall(swapData *data, client *c, void *datactx_)
{
    objectMeta *object_meta = lookupMeta(data->db,data->key);
    serverAssert(object_meta != NULL && object_meta->object_type == OBJ_BITMAP);
    bitmapMeta *bitmap_meta = objectMetaGetPtr(object_meta);

    bitmapDataCtx *datactx = datactx_;
    argRewriteRequest first_arg_req = datactx->arg_reqs[0];

    /* no need to rewrite. */
    if (first_arg_req.arg_idx < 0) {
        return 0;
    }

    if (swapDataIsHot(data)) {
        /* bitmap is purely hot, never swapped out */
        return 0;
    }

    bool offset_arg_is_byte = first_arg_req.arg_type == BITMAP_BYTE_RANGE ? true : false;

    for (int i = 0; i < 2; i++) {
        argRewriteRequest arg_req = datactx->arg_reqs[i];
        /* 1. impossible to modify when the arg_idx = 0, 2. the arg needed to rewrite maybe not exist. */
        if (arg_req.arg_idx <= 0 || arg_req.arg_idx >= c->argc) continue;
        long long offset;
        int ret;

        if (arg_req.mstate_idx < 0) {
            ret = getLongLongFromObject(c->argv[arg_req.arg_idx], &offset);
        } else {
            serverAssert(arg_req.mstate_idx < c->mstate.count);
            ret = getLongLongFromObject(c->mstate.commands[arg_req.mstate_idx].argv[arg_req.arg_idx], &offset);
        }

        serverAssert(ret == C_OK);

        int subkey_idx = 0;
        if (offset_arg_is_byte && offset < 0) {
            offset = bitmap_meta->size + offset;
        }

        if (offset_arg_is_byte) {
            subkey_idx = offset / BITMAP_SUBKEY_SIZE;
        } else {
            subkey_idx = offset / BITS_NUM_IN_SUBKEY;
        }
        if (subkey_idx == 0) {
            continue;
        }
        int subkeys_num_ahead = bitmapMetaGetHotSubkeysNum(bitmap_meta, 0, subkey_idx - 1);

        if (subkeys_num_ahead == subkey_idx) {
            /* no need to modify offset */
            continue;
        } else {
            if (offset_arg_is_byte) {
                long long offset_in_subkey = offset - subkey_idx * BITMAP_SUBKEY_SIZE;
                long long newOffset = subkeys_num_ahead * BITMAP_SUBKEY_SIZE + offset_in_subkey;

                robj *new_arg = createObject(OBJ_STRING,sdsfromlonglong(newOffset));
                clientArgRewrite(c, arg_req, new_arg);
            } else {
                long long offset_in_subkey = offset - subkey_idx * BITS_NUM_IN_SUBKEY;
                long long newOffset = subkeys_num_ahead * BITS_NUM_IN_SUBKEY + offset_in_subkey;

                robj *new_arg = createObject(OBJ_STRING,sdsfromlonglong(newOffset));
                clientArgRewrite(c, arg_req, new_arg);
            }
        }
    }

    return 0;
}

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
        .free = bitmapSwapDataFree,
        .rocksDel = NULL,
        .mergedIsHot = bitmapMergedIsHot,
        .getObjectMetaAux = NULL,
};

int swapDataSetupBitmap(swapData *d, void **pdatactx) {
    d->type = &bitmapSwapDataType;
    d->omtype = &bitmapObjectMetaType;
    bitmapDataCtx *datactx = bitmapDataCtxCreate();
    argRewriteRequestInit(datactx->arg_reqs + 0);
    argRewriteRequestInit(datactx->arg_reqs + 1);
    *pdatactx = datactx;
    return 0;
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

/* bitmap save */

 /* only used for bitmap saving rdb, subkey of idx or subkey_phy_cursor has not been saved. */
typedef struct bitmapIterator {
    long subkey_idx;
    long subkey_phy_cursor;  /* only points to the bitmap in memory. */
} bitmapIterator;

void *bitmapSaveIterCreate()
{
    bitmapIterator *iter = zmalloc(sizeof(bitmapIterator));
    iter->subkey_idx = 0;
    iter->subkey_phy_cursor = 0;
    return iter;
}

void bitmapSaveIterFree(bitmapIterator *iter)
{
    zfree(iter);
}

int bitmapSaveStart(rdbKeySaveData *save, rio *rdb) {
    robj *key = save->key;
    /* save header */
    if (rdbSaveKeyHeader(rdb,key,key,RDB_TYPE_STRING,save->expire) == -1) {
        return -1;
    }
    return 0;
}

/* save subkeys in memory untill subkey idx(not included) */
int bitmapSaveHotSubkeysUntill(rdbKeySaveData *save, rio *rdb, long idx) {

    /* 1. no hot subkeys in the front, 2. cold bitmap, no hot subkeys. */
    if (idx == 0 || save->iter == NULL) {
        return 0;
    }

    bitmapIterator *iter = save->iter;

    /* no hot subkeys in the front. */
    if (iter->subkey_idx == idx) {
        return 0;
    }

    bitmapMeta *bitmap_meta = objectMetaGetPtr(save->object_meta);

    int hot_subkeys_num_to_save = bitmapMetaGetHotSubkeysNum(bitmap_meta, iter->subkey_idx, idx - 1);
    serverAssert(idx - iter->subkey_idx == hot_subkeys_num_to_save);

    long hot_subkeys_size_to_save;

    int subkeysSum = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size);
    if (idx >= subkeysSum) {
        /* hot subkeys to save include the last subkey. */
        hot_subkeys_size_to_save = hot_subkeys_num_to_save * BITMAP_SUBKEY_SIZE;
    } else {
        hot_subkeys_size_to_save = stringObjectLen(save->value) - iter->subkey_phy_cursor;
    }

    if (rdbWriteRaw(rdb, save->value->ptr + iter->subkey_phy_cursor, hot_subkeys_size_to_save) == -1) {
        return -1;
    }
    iter->subkey_idx = idx;
    iter->subkey_phy_cursor += hot_subkeys_size_to_save;
    return 0;
}

/* only called in cold or warm data. */
int bitmapSave(rdbKeySaveData *save, rio *rdb, decodedData *decoded) {
    long idx;
    robj *key = save->key;
    serverAssert(!sdscmp(decoded->key, key->ptr));

    if (decoded->rdbtype != RDB_TYPE_STRING) {
        /* check failed, skip this key */
        return 0;
    }

    /* save subkeys in prior to current saving subkey in memory. */
    idx = bitmapDecodeSubkeyIdx(decoded->subkey,sdslen(decoded->subkey));

    if (save->value != NULL) {
        bitmapMeta *bitmap_meta = objectMetaGetPtr(save->object_meta);
        if (bitmapMetaGetSubkeyStatus(bitmap_meta, idx, idx)) {
            /* hot subkey, skip this subkey */
            return 0;
        }
    }

    bitmapSaveHotSubkeysUntill(save, rdb, idx);

    if (rdbWriteRaw(rdb,(unsigned char*)decoded->rdbraw,
                    sdslen(decoded->rdbraw)) == -1) {
        return -1;
    }

    bitmapIterator *iter = save->iter;
    if (iter != NULL) {
        iter->subkey_idx = idx + 1;
    }

    save->saved++;
    return 0;
}

int bitmapSaveEnd(rdbKeySaveData *save, rio *rdb, int save_result) {
    bitmapMeta *meta = objectMetaGetPtr(save->object_meta);
    long expected = meta->pure_cold_subkeys_num + server.swap_debug_bgsave_metalen_addition;

    if (save_result != -1) {
        /* save tail hot subkeys */
        bitmapSaveHotSubkeysUntill(save, rdb, BITMAP_MAX_INDEX);
    }

    if (save->saved != expected) {
        sds key  = save->key->ptr;
        sds repr = sdscatrepr(sdsempty(), key, sdslen(key));
        serverLog(LL_WARNING,
                  "bitmapSave %s: saved(%d) != bitmapMeta.pure_cold_subkeys_num(%ld)",
                  repr, save->saved, expected);
        sdsfree(repr);
        return SAVE_ERR_META_LEN_MISMATCH;
    }

    return save_result;
}

void bitmapSaveDeinit(rdbKeySaveData *save) {
    if (save->iter) {
        bitmapSaveIterFree(save->iter);
        save->iter = NULL;
    }
}

rdbKeySaveType bitmapSaveType = {
        .save_start = bitmapSaveStart,
        .save = bitmapSave,
        .save_end = bitmapSaveEnd,
        .save_deinit = bitmapSaveDeinit,
};

int bitmapSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen) {
    int retval = 0;
    save->type = &bitmapSaveType;
    save->omtype = &bitmapObjectMetaType;
    if (extend) { /* cold */
        serverAssert(save->object_meta == NULL && save->value == NULL);
        retval = buildObjectMeta(OBJ_BITMAP,version,extend,extlen,&save->object_meta);
    } else { /* warm */
        serverAssert(save->object_meta && save->value);
        save->iter = bitmapSaveIterCreate();
    }
    return retval;
}

#ifdef REDIS_TEST
#define SWAP_EVICT_STEP 13
#define SWAP_EVICT_MEM  (BITMAP_SUBKEY_SIZE * 10)

#define INIT_SAVE_SKIP -2
void initServerConfig(void);

int swapDataBitmapTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);
    initServerConfig();
    ACLInit();
    server.hz = 10;
    initTestRedisServer();

    redisDb* db = server.db + 0;
    int error = 0;
    robj *purehot_key1, *purehot_bitmap1, *purehot_key2, *purehot_bitmap2, *purehot_key3, *purehot_bitmap3, *purehot_key4, *purehot_bitmap4, *hot_key1, *hot_bitmap1, *cold_key1, *cold_key2, *warm_key1, *warm_bitmap1, *warm_key2, *warm_bitmap2;
    keyRequest _keyReq, *purehot_keyReq1 = &_keyReq, _keyReq2, *purehot_keyReq2 = &_keyReq2, _keyReq3, *purehot_keyReq3 = &_keyReq3, _keyReq4, *purehot_keyReq4 = &_keyReq4, _hot_keyReq, *hot_keyReq1 = &_hot_keyReq, _cold_keyReq, *cold_keyReq1 = &_cold_keyReq, _cold_keyReq2, *cold_keyReq2 = &_cold_keyReq2, _warm_keyReq, *warm_keyReq1 = &_warm_keyReq, _warm_keyReq2, *warm_keyReq2 = &_warm_keyReq2;
    swapData *purehot_data1, *purehot_data2, *purehot_data3, *purehot_data4, *hot_data1, *cold_data1, *warm_data1, *cold_data2, *warm_data2;
    objectMeta *purehot_meta1, *purehot_meta2, *purehot_meta3, *purehot_meta4, *hot_meta1, *cold_meta1, *warm_meta1, *cold_meta2, *warm_meta2;
    bitmapDataCtx *purehot_ctx1, *purehot_ctx2, *purehot_ctx3, *purehot_ctx4, *hot_ctx1, *cold_ctx1, *warm_ctx1, *cold_ctx2, *warm_ctx2 = NULL;
    int intention;
    uint32_t intention_flags;
    int action, numkeys, *cfs;
    sds *rawkeys, *rawvals;

    int originEvictStepMaxSubkey = server.swap_evict_step_max_subkeys;
    int originEvictStepMaxMemory = server.swap_evict_step_max_memory;

    TEST("bitmap - swap test init") {

        server.swap_evict_step_max_subkeys = SWAP_EVICT_STEP;
        server.swap_evict_step_max_memory = SWAP_EVICT_MEM;

        purehot_key1 = createStringObject("purehot_key1",12);
        purehot_meta1 = createBitmapObjectMeta(0, NULL);

        /* 2 subkeys in pure hot bitmap*/
        sds str = sdsnewlen("", BITMAP_SUBKEY_SIZE * 2);
        purehot_bitmap1 = createStringObject(str, sdslen(str));
        dbAdd(db,purehot_key1,purehot_bitmap1);

        incrRefCount(purehot_key1);
        purehot_keyReq1->key = purehot_key1;
        purehot_keyReq1->type = KEYREQUEST_TYPE_SUBKEY;
        purehot_keyReq1->level = REQUEST_LEVEL_KEY;
        purehot_keyReq1->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        purehot_keyReq1->l.num_ranges = 0;
        purehot_keyReq1->l.ranges = NULL;

        /* 3 subkeys in hot bitmap*/
        hot_key1 = createStringObject("hot_key1", 8);
        hot_meta1 = createBitmapObjectMeta(0, NULL);

        bitmapMeta *hot_bitmap_meta = bitmapMetaCreate();

        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 2, HOT_STATUS);
        hot_bitmap_meta->pure_cold_subkeys_num = 0;
        hot_bitmap_meta->size = 3 * BITMAP_SUBKEY_SIZE;
        objectMetaSetPtr(hot_meta1, hot_bitmap_meta);

        sds str1 = sdsnewlen("", BITMAP_SUBKEY_SIZE * 3);
        hot_bitmap1 = createStringObject(str1, sdslen(str1));
        dbAdd(db,hot_key1,hot_bitmap1);

        incrRefCount(hot_key1);
        hot_keyReq1->key = hot_key1;
        hot_keyReq1->type = KEYREQUEST_TYPE_SUBKEY;
        hot_keyReq1->level = REQUEST_LEVEL_KEY;
        hot_keyReq1->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        hot_keyReq1->l.num_ranges = 0;
        hot_keyReq1->l.ranges = NULL;

        /* 3 subkeys in cold bitmap*/
        cold_key1 = createStringObject("cold_key1",9);
        cold_meta1 = createBitmapObjectMeta(0, NULL);

        int size = 3 * BITMAP_SUBKEY_SIZE;
        sds coldBitmapSize = encodeBitmapSize(size);

        bitmapObjectMetaDecode(cold_meta1, coldBitmapSize, sdslen(coldBitmapSize));

        incrRefCount(cold_key1);
        cold_keyReq1->key = cold_key1;
        cold_keyReq1->level = REQUEST_LEVEL_KEY;
        cold_keyReq1->type = KEYREQUEST_TYPE_SUBKEY;
        cold_keyReq1->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        cold_keyReq1->l.num_ranges = 0;
        cold_keyReq1->l.ranges = NULL;

        purehot_data1 = createSwapData(db, purehot_key1, purehot_bitmap1, NULL);
        swapDataSetupMeta(purehot_data1,OBJ_BITMAP, -1,(void**)&purehot_ctx1);
        swapDataSetObjectMeta(purehot_data1, purehot_meta1);

        hot_data1 = createSwapData(db, hot_key1, hot_bitmap1, NULL);
        swapDataSetupMeta(hot_data1, OBJ_BITMAP, -1, (void**)&hot_ctx1);
        swapDataSetObjectMeta(hot_data1, hot_meta1);
        
        cold_data1 = createSwapData(db, cold_key1, NULL, NULL);
        swapDataSetupMeta(cold_data1, OBJ_BITMAP, -1, (void**)&cold_ctx1);
        swapDataSetObjectMeta(cold_data1, cold_meta1);
    }

    TEST("bitmap - meta api test") {
        objectMeta *object_meta1;
        bitmapMeta *bitmap_meta1;
        bitmap_meta1 = bitmapMetaCreate();

        bitmap_meta1->size = BITMAP_SUBKEY_SIZE + 1;
        bitmap_meta1->pure_cold_subkeys_num = 1;
        bitmapMetaSetSubkeyStatus(bitmap_meta1, 0, 0, HOT_STATUS);

        object_meta1 = createBitmapObjectMeta(0, bitmap_meta1);
        sds meta_buf1 = bitmapObjectMetaEncode(object_meta1, NULL);

        objectMeta *object_meta2 = createBitmapObjectMeta(0, NULL);
        test_assert(0 == bitmapObjectMetaDecode(object_meta2, meta_buf1, sdslen(meta_buf1)));
        test_assert(0 == bitmapObjectMetaEqual(object_meta1, object_meta2));
        test_assert(0 == bitmapObjectMetaIsHot(object_meta2, NULL));

        objectMeta *object_meta3 = createBitmapObjectMeta(0, NULL);
        bitmapObjectMetaDup(object_meta3, object_meta1);
        test_assert(1 == bitmapObjectMetaEqual(object_meta1, object_meta3));

        bitmapMeta *bitmap_meta3 = objectMetaGetPtr(object_meta3);
        test_assert(1 == bitmapMetaGetHotSubkeysNum(bitmap_meta3, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta3->size) - 1));
        test_assert(0 == bitmapObjectMetaIsHot(object_meta2, NULL));

        bitmapObjectMetaFree(object_meta1);
        bitmapObjectMetaFree(object_meta2);
        bitmapObjectMetaFree(object_meta3);
    }

    TEST("bitmap - swapAna") {
        /* result: nop, NOP/IN/DEL for hot data, OUT for cold data... */
        purehot_keyReq1->cmd_intention = SWAP_NOP, purehot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(purehot_data1, 0, purehot_keyReq1, &intention, &intention_flags, purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_IN_META;
        swapDataAna(purehot_data1, 0, purehot_keyReq1, &intention, &intention_flags, purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_IN_DEL_MOCK_VALUE;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_IN_OVERWRITE;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_METASCAN_RANDOMKEY;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_METASCAN_SCAN;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_IN, purehot_keyReq1->cmd_intention_flags = SWAP_IN_FORCE_HOT;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        purehot_keyReq1->cmd_intention = SWAP_DEL, purehot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(objectMetaGetPtr(purehot_meta1) == NULL);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        cold_keyReq1->cmd_intention = SWAP_OUT, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(((bitmapMeta *)objectMetaGetPtr(cold_meta1))->pure_cold_subkeys_num == 3);
        test_assert((BITMAP_GET_SUBKEYS_NUM(((bitmapMeta *)objectMetaGetPtr(cold_meta1))->size)) == 3);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        /* in: for persist data, l.num_ranges != 0 */
        range *range1 = zmalloc(sizeof(range));

        range1->start = BITS_NUM_IN_SUBKEY * 4;            /* out of range, read operation */
        range1->end = BITS_NUM_IN_SUBKEY * 5 - 1;
        range1->type = BITMAP_BIT_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_NOP);
        test_assert(cold_ctx1->subkeys_num == 0);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY * 4;            /* out of range, read operation */
        range1->end = BITS_NUM_IN_SUBKEY * 4;
        range1->type = BITMAP_BIT_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_NOP);
        test_assert(cold_ctx1->subkeys_num == 0);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY * 4;                /* out of range, write operation */
        range1->end = BITS_NUM_IN_SUBKEY * 5 - 1;
        range1->type = BITMAP_BIT_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == SWAP_EXEC_IN_DEL);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY * 5 - 1;                /* out of range, write operation */
        range1->end = BITS_NUM_IN_SUBKEY * 5 - 1;
        range1->type = BITMAP_BIT_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == SWAP_EXEC_IN_DEL);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = 0;                /* swap in all */
        range1->end = BITMAP_SUBKEY_SIZE * 3 - 1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_ITERATE);
        test_assert(cold_ctx1->subkeys_num == -1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE;              /* swap in part */
        range1->end = BITMAP_SUBKEY_SIZE * 2 - 1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY;
        range1->end = BITS_NUM_IN_SUBKEY;
        range1->type = BITMAP_BIT_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;          /* getbit */
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE;
        range1->end = BITMAP_SUBKEY_SIZE * 3 - 1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;             /* bitcount */
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 2);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[1] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = 0;
        range1->end = BITMAP_SUBKEY_SIZE * 2 - 1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;             /* bitpos */
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 2);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 0);
        test_assert(cold_ctx1->subkeys_logic_idx[1] == 1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = 0;
        range1->end = -1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;             /* bitpos */
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == -1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE * 2;            /* range across boundry.  bitcount. */
        range1->end = BITMAP_SUBKEY_SIZE * 5 - 1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE * 2;            /* range across boundry.  bitpos. */
        range1->end = BITMAP_SUBKEY_SIZE * 5 - 1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE;            /* range across boundry.  bitpos. */
        range1->end = -1;
        range1->type = BITMAP_BYTE_RANGE;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 2);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[1] == 2);
        bitmapDataCtxReset(cold_ctx1);

        zfree(range1);

        /* in : for persist data, l.num_ranges = 0, specific flags*/
        cold_keyReq1->l.num_ranges = 0;
        cold_keyReq1->l.ranges = NULL;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = SWAP_IN_DEL_MOCK_VALUE;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_DEL && intention_flags == SWAP_FIN_DEL_SKIP);
        test_assert(cold_ctx1->ctx_flag == BIG_DATA_CTX_FLAG_MOCK_VALUE);
        bitmapDataCtxReset(cold_ctx1);

        cold_keyReq1->l.num_ranges = 0;
        cold_keyReq1->l.ranges = NULL;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == SWAP_EXEC_IN_DEL);
        bitmapDataCtxReset(cold_ctx1);

        hot_keyReq1->l.num_ranges = 0;
        hot_keyReq1->l.ranges = NULL;
        hot_keyReq1->cmd_intention = SWAP_IN, hot_keyReq1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(hot_data1,0,hot_keyReq1,&intention,&intention_flags,hot_ctx1);
        test_assert(intention == SWAP_DEL && intention_flags == SWAP_FIN_DEL_SKIP);
        bitmapDataCtxReset(hot_ctx1);

        hot_keyReq1->l.num_ranges = 0;
        hot_keyReq1->l.ranges = NULL;
        hot_keyReq1->cmd_intention = SWAP_IN, hot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(hot_data1,0,hot_keyReq1,&intention,&intention_flags,hot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        bitmapDataCtxReset(hot_ctx1);

        hot_keyReq1->l.num_ranges = 0;
        hot_keyReq1->l.ranges = NULL;
        hot_keyReq1->cmd_intention = SWAP_IN, hot_keyReq1->cmd_intention_flags = SWAP_IN_META;
        swapDataAna(hot_data1,0,hot_keyReq1,&intention,&intention_flags,hot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        bitmapDataCtxReset(hot_ctx1);

        cold_keyReq1->l.num_ranges = 0;
        cold_keyReq1->l.ranges = NULL;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = SWAP_IN_META;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_GET);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == -1);
        bitmapDataCtxReset(cold_ctx1);

        cold_keyReq1->l.num_ranges = 0;
        cold_keyReq1->l.ranges = NULL;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_ITERATE);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == -1);
        bitmapDataCtxReset(cold_ctx1);

        /* del: for hot data*/
        purehot_keyReq1->cmd_intention = SWAP_DEL, purehot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(purehot_data1, 0, purehot_keyReq1, &intention, &intention_flags, purehot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);

        /* DEL for cold data. */
        cold_keyReq1->cmd_intention = SWAP_DEL, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1, 0, cold_keyReq1, &intention, &intention_flags, cold_ctx1);
        test_assert(intention == SWAP_DEL && intention_flags == 0);
        test_assert(((bitmapMeta *)objectMetaGetPtr(cold_meta1))->pure_cold_subkeys_num == 3);
        test_assert(BITMAP_GET_SUBKEYS_NUM(((bitmapMeta *)objectMetaGetPtr(cold_meta1))->size) == 3);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        /* out: for not cold data, evict by small steps */
        setObjectDirty(purehot_bitmap1);
        purehot_keyReq1->cmd_intention = SWAP_OUT, purehot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(purehot_data1,0,purehot_keyReq1,&intention,&intention_flags,purehot_ctx1);
        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(purehot_ctx1->subkeys_num == 2);
        test_assert(purehot_ctx1->subkeys_logic_idx[0] == 0);
        test_assert(purehot_ctx1->subkeys_logic_idx[1] == 1);
        bitmapDataCtxReset(purehot_ctx1);

        clearObjectDirty(hot_bitmap1);
        hot_keyReq1->cmd_intention = SWAP_OUT, hot_keyReq1->cmd_intention_flags = 0;
        swapDataAna(hot_data1,0,hot_keyReq1,&intention,&intention_flags,hot_ctx1);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(hot_ctx1->subkeys_num == 3);
        bitmapDataCtxReset(hot_ctx1);
    }

    TEST("bitmap - encodeData/DecodeData") {
        void *decoded;

        warm_key1 = createStringObject("warm_key1",13);
        warm_meta1 = createBitmapObjectMeta(0, NULL);

        bitmapMeta *warm_bitmap_meta1 = bitmapMetaCreate();

        /* subkeys sum = 8, part {0, 1, 3, 4, 7} in redis, and will swap out  */
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta1, 0, 1, HOT_STATUS);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta1, 3, 4, HOT_STATUS);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta1, 7, 7, HOT_STATUS);

        warm_bitmap_meta1->pure_cold_subkeys_num = 3;

        /* size of subkey 7 is half of BITMAP_SUBKEY_SIZE*/
        warm_bitmap_meta1->size = 7 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;
        objectMetaSetPtr(warm_meta1, warm_bitmap_meta1);

        /* 5 subkeys are hot */
        sds str = sdsnewlen("", BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2);
        warm_bitmap1 = createStringObject(str, sdslen(str));
        dbAdd(db,warm_key1,warm_bitmap1);

        incrRefCount(warm_key1);
        warm_keyReq1->key = warm_key1;
        warm_keyReq1->type = KEYREQUEST_TYPE_SUBKEY;
        warm_keyReq1->level = REQUEST_LEVEL_KEY;
        warm_keyReq1->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        warm_keyReq1->l.num_ranges = 0;
        warm_keyReq1->l.ranges = NULL;

        warm_data1 = createSwapData(db, warm_key1, warm_bitmap1, NULL);
        swapDataSetupMeta(warm_data1,OBJ_BITMAP, -1,(void**)&warm_ctx1);
        swapDataSetObjectMeta(warm_data1, warm_meta1);

        setObjectDirty(warm_bitmap1);
        warm_keyReq1->cmd_intention = SWAP_OUT, warm_keyReq1->cmd_intention_flags = 0;
        swapDataAna(warm_data1,0,warm_keyReq1,&intention,&intention_flags,warm_ctx1);

        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(warm_ctx1->subkeys_num == 5);
        test_assert(warm_ctx1->subkeys_total_size == BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(warm_ctx1->subkeys_logic_idx[0] == 0);
        test_assert(warm_ctx1->subkeys_logic_idx[4] == 7);

        bitmapSwapAnaAction(warm_data1,intention,warm_ctx1,&action);
        test_assert(action == ROCKS_PUT);

        bitmapEncodeData(warm_data1,intention,warm_ctx1,&numkeys,&cfs,&rawkeys,&rawvals);
        test_assert(numkeys == warm_ctx1->subkeys_num);

        bitmapDecodeData(warm_data1,numkeys,cfs,rawkeys,rawvals,&decoded);

        deltaBitmap *deltaBm = (deltaBitmap *)decoded;
        test_assert(deltaBm->subkey_intervals_num == 3);
        test_assert(deltaBm->subkeys_num == 5);

        test_assert(deltaBm->subkey_intervals[2].start_index == 7);
        test_assert(deltaBm->subkey_intervals[2].end_index == 7);
        test_assert(sdslen(deltaBm->subval_intervals[2]) == BITMAP_SUBKEY_SIZE / 2);

        test_assert(deltaBm->subkey_intervals[1].start_index == 3);
        test_assert(deltaBm->subkey_intervals[1].end_index == 4);
        test_assert(sdslen(deltaBm->subval_intervals[1]) == BITMAP_SUBKEY_SIZE * 2);

        test_assert(deltaBm->subkey_intervals[0].start_index == 0);
        test_assert(deltaBm->subkey_intervals[0].end_index == 1);
        test_assert(sdslen(deltaBm->subval_intervals[0]) == BITMAP_SUBKEY_SIZE * 2);

        test_assert(deltaBm->subkeys_logic_idx[0] == 0);
        test_assert(deltaBm->subkeys_logic_idx[4] == 7);

        bitmapDataCtxReset(warm_ctx1);
        deltaBitmapFree(deltaBm);
        sdsfree(str);
        // decrRefCount(warm_key1);
        for (int i = 0; i < numkeys; i++) {
            sdsfree(rawkeys[i]), sdsfree(rawvals[i]);
        }
        zfree(cfs), zfree(rawkeys), zfree(rawvals);
    }

    TEST("bitmap - swapIn for cold data") { /* cold to warm */
        cold_key2 = createStringObject("cold_key2",9);
        cold_meta2 = createBitmapObjectMeta(0, NULL);

        /* subkeys 0 ~ 7 in rocksDb, swap in {0, 1, 3, 4, 7} */
        int size = 7 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;
        sds coldBitmapSize = encodeBitmapSize(size);
        bitmapObjectMetaDecode(cold_meta2, coldBitmapSize, sdslen(coldBitmapSize));

        incrRefCount(cold_key2);
        cold_keyReq2->key = cold_key2;
        cold_keyReq2->level = REQUEST_LEVEL_KEY;
        cold_keyReq2->type = KEYREQUEST_TYPE_SUBKEY;
        cold_keyReq2->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        cold_keyReq2->l.num_ranges = 0;
        cold_keyReq2->l.ranges = NULL;

        cold_data2 = createSwapData(db, cold_key2, NULL, NULL);
        swapDataSetupMeta(cold_data2, OBJ_BITMAP, -1, (void**)&cold_ctx2);
        swapDataSetObjectMeta(cold_data2, cold_meta2);

        deltaBitmap *deltaBm = deltaBitmapCreate();
        deltaBm->subkey_intervals_num = 3;
        deltaBm->subkeys_num = 5;
        deltaBm->subkey_intervals = zmalloc(sizeof(subkeyInterval) * 3);
        deltaBm->subkeys_logic_idx = zmalloc(sizeof(int) * 5);
        deltaBm->subval_intervals = zmalloc(sizeof(sds) * 3);

        deltaBm->subkey_intervals[0].start_index = 0;
        deltaBm->subkey_intervals[0].start_index = 1;

        deltaBm->subkey_intervals[1].start_index = 3;
        deltaBm->subkey_intervals[1].start_index = 4;

        deltaBm->subkey_intervals[2].start_index = 7;
        deltaBm->subkey_intervals[2].start_index = 7;

        deltaBm->subval_intervals[0] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2);
        deltaBm->subval_intervals[1] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2);
        deltaBm->subval_intervals[2] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        deltaBm->subkeys_logic_idx[0] = 0;
        deltaBm->subkeys_logic_idx[1] = 1;
        deltaBm->subkeys_logic_idx[2] = 3;
        deltaBm->subkeys_logic_idx[3] = 4;
        deltaBm->subkeys_logic_idx[4] = 7;

        robj *result = bitmapCreateOrMergeObject(cold_data2, deltaBm, cold_ctx2);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(cold_data2);

        test_assert(bitmap_meta->pure_cold_subkeys_num == 3);
        test_assert(bitmap_meta->size == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size) - 1) == 5);
        test_assert(stringObjectLen(result) == BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2);

        test_assert(bitmapMergedIsHot(cold_data2, result, cold_ctx2) == 0);

        bitmapSwapIn(cold_data2, result, cold_ctx2);

        robj *bm;
        test_assert((bm = lookupKey(db,cold_key2,LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(bm->persistent);

        // decrRefCount(cold_key2);
    }

    TEST("bitmap - swapIn for warm data") { /* warm to hot */
        warm_key2 = createStringObject("warm_key2",13);
        warm_meta2 = createBitmapObjectMeta(0, NULL);

        bitmapMeta *warm_bitmap_meta2 = bitmapMetaCreate();

        /* subkeys sum = 8, part {0, 1, 3, 4, 6} both in redis and rocks, {2, 5, 7} will swap in  */
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta2, 0, 1, HOT_STATUS);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta2, 3, 4, HOT_STATUS);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta2, 6, 6, HOT_STATUS);

        warm_bitmap_meta2->pure_cold_subkeys_num = 3;

        /* size of subkey 7 is half of BITMAP_SUBKEY_SIZE*/
        warm_bitmap_meta2->size = 7 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;
        objectMetaSetPtr(warm_meta2, warm_bitmap_meta2);

        /* 5 subkeys are hot */
        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 4);
        sds str1 = sdsnewlen("1", BITMAP_SUBKEY_SIZE);  /* subkey 6 is not all zero bits. */

        str = sdscatsds(str, str1);
        warm_bitmap2 = createStringObject(str, sdslen(str));

        sds str2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 4);
        sds str3 = sdsnewlen("1", BITMAP_SUBKEY_SIZE);  /* subkey 6 is not all zero bits. */
        str2 = sdscatsds(str2, str3);

        test_assert(0 == memcmp(warm_bitmap2->ptr, str2, BITMAP_SUBKEY_SIZE * 5));
        test_assert(0 == memcmp(warm_bitmap2->ptr + BITMAP_SUBKEY_SIZE * 4, "1", 1));

        dbAdd(db,warm_key2,warm_bitmap2);

        incrRefCount(warm_key2);
        warm_keyReq2->key = warm_key2;
        warm_keyReq2->type = KEYREQUEST_TYPE_SUBKEY;
        warm_keyReq2->level = REQUEST_LEVEL_KEY;
        warm_keyReq2->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        warm_keyReq2->l.num_ranges = 0;
        warm_keyReq2->l.ranges = NULL;

        warm_data2 = createSwapData(db, warm_key2, warm_bitmap2, NULL);
        swapDataSetupMeta(warm_data2,OBJ_BITMAP, -1,(void**)&warm_ctx2);
        swapDataSetObjectMeta(warm_data2, warm_meta2);

        deltaBitmap *deltaBm = deltaBitmapCreate();
        deltaBm->subkey_intervals_num = 3;
        deltaBm->subkeys_num = 3;
        deltaBm->subkey_intervals = zmalloc(sizeof(subkeyInterval) * 3);
        deltaBm->subkeys_logic_idx = zmalloc(sizeof(int) * 3);
        deltaBm->subval_intervals = zmalloc(sizeof(sds) * 3);

        deltaBm->subkey_intervals[0].start_index = 2;
        deltaBm->subkey_intervals[0].end_index = 2;

        deltaBm->subkey_intervals[1].start_index = 5;
        deltaBm->subkey_intervals[1].end_index = 5;

        deltaBm->subkey_intervals[2].start_index = 7;
        deltaBm->subkey_intervals[2].end_index = 7;

        deltaBm->subval_intervals[0] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subval_intervals[1] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subval_intervals[2] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        deltaBm->subkeys_logic_idx[0] = 2;
        deltaBm->subkeys_logic_idx[1] = 5;
        deltaBm->subkeys_logic_idx[2] = 7;

        robj *result = bitmapCreateOrMergeObject(warm_data2, deltaBm, warm_ctx2);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(warm_data2);

        test_assert(bitmap_meta->pure_cold_subkeys_num == 0);
        test_assert(bitmap_meta->size == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size) - 1) == 8);
        test_assert(stringObjectLen(result) == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);

        test_assert(bitmapMergedIsHot(warm_data2, result, warm_ctx2) == 1);

        bitmapSwapIn(warm_data2, result, warm_ctx2);
        robj *bm;
        test_assert((bm = lookupKey(db,warm_key2,LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);

        sds str5 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 6);
        sds str6 = sdsnewlen("1", BITMAP_SUBKEY_SIZE);  /* subkey 6 is not all zero bits. */
        sds str7 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        str5 = sdscatsds(str5, str6);
        str5 = sdscatsds(str5, str7);

        int tmp_num = 0;
        test_assert(0 == memcmp(bm->ptr + BITMAP_SUBKEY_SIZE * 6 + 1, &tmp_num, 1));
        test_assert(0 == memcmp(bm->ptr, str5, BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2));
        test_assert(0 == memcmp(bm->ptr + BITMAP_SUBKEY_SIZE * 6, "1", 1));

        test_assert(bm->persistent);

        sdsfree(str);
        sdsfree(str1);
        sdsfree(str2);
        sdsfree(str3);
        sdsfree(str5);
        sdsfree(str6);
        sdsfree(str7);
        // decrRefCount(warm_key2);
    }

    TEST("bitmap - swap out for pure hot data1") { /* pure hot to cold */
        purehot_key2 = createStringObject("purehot_key2",12);
        purehot_meta2 = createBitmapObjectMeta(0, NULL);

        /* 2 subkeys in pure hot bitmap */
        sds str = sdsnewlen("", BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2);
        purehot_bitmap2 = createStringObject(str, sdslen(str));
        dbAdd(db, purehot_key2, purehot_bitmap2);

        incrRefCount(purehot_key2);
        purehot_keyReq2->key = purehot_key2;
        purehot_keyReq2->type = KEYREQUEST_TYPE_SUBKEY;
        purehot_keyReq2->level = REQUEST_LEVEL_KEY;
        purehot_keyReq2->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        purehot_keyReq2->l.num_ranges = 0;
        purehot_keyReq2->l.ranges = NULL;

        purehot_data2 = createSwapData(db, purehot_key2, purehot_bitmap2, NULL);
        swapDataSetupMeta(purehot_data2,OBJ_BITMAP, -1,(void**)&purehot_ctx2);
        swapDataSetObjectMeta(purehot_data2, purehot_meta2);

        setObjectDirty(purehot_bitmap2);
        purehot_keyReq2->cmd_intention = SWAP_OUT, purehot_keyReq2->cmd_intention_flags = 0;
        swapDataAna(purehot_data2,0,purehot_keyReq2,&intention,&intention_flags,purehot_ctx2);

        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(purehot_ctx2->subkeys_num == 2);
        test_assert(purehot_ctx2->subkeys_total_size == BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2);
        test_assert(purehot_ctx2->subkeys_logic_idx[0] == 0);
        test_assert(purehot_ctx2->subkeys_logic_idx[1] == 1);

        bitmapCleanObject(purehot_data2, purehot_ctx2, 0);

        robj *bm;
        test_assert((bm = lookupKey(db, purehot_key2, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == 0);
        test_assert(bm->persistent);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(purehot_data2);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size) - 1) == 0);
        test_assert(bitmap_meta->pure_cold_subkeys_num == 2);

        int totally_out;
        bitmapSwapOut(purehot_data2, purehot_ctx2, 0, &totally_out);

        test_assert((bm = lookupKey(db, purehot_key2, LOOKUP_NOTOUCH)) == NULL);
        test_assert(totally_out == 1);

        sdsfree(str);
        // decrRefCount(purehot_key2);
    }

    TEST("bitmap - swap out for pure hot data2") { /* pure hot to hot */
        purehot_key3 = createStringObject("purehot_key3",12);
        purehot_meta3 = createBitmapObjectMeta(0, NULL);

        /* 13 subkeys in pure hot bitmap */
        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 10);
        sds str1 = sdsnewlen("1", BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        str = sdscatsds(str, str1);

        purehot_bitmap3 = createStringObject(str, sdslen(str));
        dbAdd(db, purehot_key3, purehot_bitmap3);

        incrRefCount(purehot_key3);
        purehot_keyReq3->key = purehot_key3;
        purehot_keyReq3->type = KEYREQUEST_TYPE_SUBKEY;
        purehot_keyReq3->level = REQUEST_LEVEL_KEY;
        purehot_keyReq3->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        purehot_keyReq3->l.num_ranges = 0;
        purehot_keyReq3->l.ranges = NULL;

        purehot_data3 = createSwapData(db, purehot_key3, purehot_bitmap3, NULL);
        swapDataSetupMeta(purehot_data3,OBJ_BITMAP, -1,(void**)&purehot_ctx3);
        swapDataSetObjectMeta(purehot_data3, purehot_meta3);

        setObjectDirty(purehot_bitmap3);
        purehot_keyReq3->cmd_intention = SWAP_OUT, purehot_keyReq3->cmd_intention_flags = 0;
        swapDataAna(purehot_data3,0,purehot_keyReq3,&intention,&intention_flags,purehot_ctx3);

        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(purehot_ctx3->subkeys_num == 10);
        test_assert(purehot_ctx3->subkeys_total_size == 10 * BITMAP_SUBKEY_SIZE);
        test_assert(purehot_ctx3->subkeys_logic_idx[0] == 0);
        test_assert(purehot_ctx3->subkeys_logic_idx[9] == 9);

        bitmapCleanObject(purehot_data3, purehot_ctx3, 1);

        robj *bm;
        test_assert((bm = lookupKey(db, purehot_key3, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 12 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(bm->persistent);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(purehot_data3);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size) - 1) == 13);
        test_assert(bitmap_meta->pure_cold_subkeys_num == 0);

        int totally_out;
        bitmapSwapOut(purehot_data3, purehot_ctx3, 1, &totally_out);

        test_assert((bm = lookupKey(db, purehot_key3, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 12 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(totally_out == 0);

        sdsfree(str);
        sdsfree(str1);
    }

    TEST("bitmap - swap out for pure hot data3") { /* pure hot to warm */
        purehot_key4 = createStringObject("purehot_key4",12);
        purehot_meta4 = createBitmapObjectMeta(0, NULL);

        /* 13 subkeys in pure hot bitmap */
        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 10);
        sds str1 = sdsnewlen("1", BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        str = sdscatsds(str, str1);

        purehot_bitmap4 = createStringObject(str, sdslen(str));
        dbAdd(db, purehot_key4, purehot_bitmap4);

        incrRefCount(purehot_key4);
        purehot_keyReq4->key = purehot_key4;
        purehot_keyReq4->type = KEYREQUEST_TYPE_SUBKEY;
        purehot_keyReq4->level = REQUEST_LEVEL_KEY;
        purehot_keyReq4->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        purehot_keyReq4->l.num_ranges = 0;
        purehot_keyReq4->l.ranges = NULL;

        purehot_data4 = createSwapData(db, purehot_key4, purehot_bitmap4, NULL);
        swapDataSetupMeta(purehot_data4,OBJ_BITMAP, -1,(void**)&purehot_ctx4);
        swapDataSetObjectMeta(purehot_data4, purehot_meta4);

        setObjectDirty(purehot_bitmap4);
        purehot_keyReq4->cmd_intention = SWAP_OUT, purehot_keyReq4->cmd_intention_flags = 0;
        swapDataAna(purehot_data4,0,purehot_keyReq4,&intention,&intention_flags,purehot_ctx4);

        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(purehot_ctx4->subkeys_num == 10);
        test_assert(purehot_ctx4->subkeys_total_size == 10 * BITMAP_SUBKEY_SIZE);
        test_assert(purehot_ctx4->subkeys_logic_idx[0] == 0);
        test_assert(purehot_ctx4->subkeys_logic_idx[9] == 9);

        bitmapCleanObject(purehot_data4, purehot_ctx4, 0);

        robj *bm;
        test_assert((bm = lookupKey(db, purehot_key4, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(bm->persistent);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(purehot_data4);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size) - 1) == 3);
        test_assert(bitmap_meta->pure_cold_subkeys_num == 10);

        int totally_out;
        bitmapSwapOut(purehot_data4, purehot_ctx4, 0, &totally_out);

        test_assert((bm = lookupKey(db, purehot_key4, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(totally_out == 0);

        sds str2 = sdsnewlen("1", BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(0 == memcmp(bm->ptr, str2, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2));

        sdsfree(str);
        sdsfree(str1);
        sdsfree(str2);
    }

    void rewriteResetClientCommandCString(client *c, int argc, ...);

    TEST("bitmap - arg rewrite") {
        client *c = createClient(NULL);
        selectDb(c,0);

        robj *key = createStringObject("key", 3);
        sds str = sdsnewlen("", BITMAP_SUBKEY_SIZE);
        robj *bitmap = createStringObject(str, sdslen(str));

        bitmapMeta *bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 5, part { 2, 3 } both in redis and rocks, {0, 1, 4} in rocksDb. */
        bitmapMetaSetSubkeyStatus(bitmap_meta, 2, 2, HOT_STATUS);
        bitmapMetaSetSubkeyStatus(bitmap_meta, 3, 3, HOT_STATUS);

        bitmap_meta->pure_cold_subkeys_num = 3;

        /* size of subkey 4 is half of BITMAP_SUBKEY_SIZE */
        bitmap_meta->size = 4 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *object_meta = createBitmapObjectMeta(0, bitmap_meta);

        bitmapDataCtx *datactx;
        swapData *data = createSwapData(db, key, bitmap, NULL);
        swapDataSetupMeta(data, OBJ_BITMAP, -1, &datactx);
        swapDataSetObjectMeta(data, object_meta);

        incrRefCount(key);
        dbAdd(db,key,bitmap);
        dbAddMeta(db,key,object_meta);

        datactx->arg_reqs[0].mstate_idx = -1;
        datactx->arg_reqs[1].mstate_idx = -1;

        datactx->arg_reqs[0].arg_idx = 2;
        datactx->arg_reqs[1].arg_idx = -1;

        datactx->arg_reqs[0].arg_type = BITMAP_BIT_RANGE;
        datactx->arg_reqs[0].arg_type = BITMAP_BIT_RANGE;

        rewriteResetClientCommandCString(c, 3, "GETBIT", "mybitmap", "65636"); /* 32768 * 2 + 100 */
        bitmapBeforeCall(data, c, datactx);
        test_assert(!strcmp(c->argv[2]->ptr,"100"));
        clientArgRewritesRestore(c);
        test_assert(!strcmp(c->argv[2]->ptr,"65636"));

        datactx->arg_reqs[0].mstate_idx = -1;
        datactx->arg_reqs[1].mstate_idx = -1;
        datactx->arg_reqs[0].arg_idx = 2;
        datactx->arg_reqs[1].arg_idx = 3;

        datactx->arg_reqs[0].arg_type = BITMAP_BYTE_RANGE;
        datactx->arg_reqs[1].arg_type = BITMAP_BYTE_RANGE;

        rewriteResetClientCommandCString(c, 4, "bITCOUNT", "mybitmap", "8292", "12388"); /* 4096 * 2 + 100,  4096 * 3 + 100*/
        bitmapBeforeCall(data, c, datactx);
        test_assert(!strcmp(c->argv[2]->ptr,"100"));
        test_assert(!strcmp(c->argv[3]->ptr,"4196"));
        clientArgRewritesRestore(c);
        test_assert(!strcmp(c->argv[2]->ptr,"8292"));
        test_assert(!strcmp(c->argv[3]->ptr,"12388"));

        datactx->arg_reqs[0].mstate_idx = -1;
        datactx->arg_reqs[1].mstate_idx = -1;
        datactx->arg_reqs[0].arg_idx = 2;
        datactx->arg_reqs[1].arg_idx = 3;

        datactx->arg_reqs[0].arg_type = BITMAP_BYTE_RANGE;
        datactx->arg_reqs[1].arg_type = BITMAP_BYTE_RANGE;

        /* BITCOUNT key [start end], both of start end may not exist.  */
        rewriteResetClientCommandCString(c, 2, "bITCOUNT", "mybitmap");
        bitmapBeforeCall(data, c, datactx);
        clientArgRewritesRestore(c);

        /* bitpos */
        datactx->arg_reqs[0].mstate_idx = -1;
        datactx->arg_reqs[1].mstate_idx = -1;
        datactx->arg_reqs[0].arg_idx = 3;
        datactx->arg_reqs[1].arg_idx = 4;

        datactx->arg_reqs[0].arg_type = BITMAP_BYTE_RANGE;
        datactx->arg_reqs[1].arg_type = BITMAP_BYTE_RANGE;

        rewriteResetClientCommandCString(c, 5, "BITPos", "mybitmap", "1", "8192", "16383"); /* pos : 4096 * 2,  4096 * 4 - 1 */
        bitmapBeforeCall(data, c, datactx);
        test_assert(!strcmp(c->argv[3]->ptr,"0"));
        test_assert(!strcmp(c->argv[4]->ptr,"8191"));
        clientArgRewritesRestore(c);
        test_assert(!strcmp(c->argv[3]->ptr,"8192"));
        test_assert(!strcmp(c->argv[4]->ptr,"16383"));

        /* BITPOS key bit [start [end] ], end may not exist. */
        rewriteResetClientCommandCString(c, 4, "BITPos", "mybitmap", "1", "8192"); /* pos : 4096 * 2 */
        bitmapBeforeCall(data, c, datactx);
        test_assert(!strcmp(c->argv[3]->ptr,"0"));
        clientArgRewritesRestore(c);
        test_assert(!strcmp(c->argv[3]->ptr,"8192"));

        /* BITPOS key bit [start [end] ], both of start end may not exist. */
        rewriteResetClientCommandCString(c, 3, "BITPos", "mybitmap", "1");
        bitmapBeforeCall(data, c, datactx);
        clientArgRewritesRestore(c);

        dbDelete(db, key);
        swapDataFree(data, datactx);
        decrRefCount(key);
        sdsfree(str);
    }

    TEST("bitmap - swap test deinit") {
        swapDataFree(purehot_data1, purehot_ctx1);
        swapDataFree(purehot_data2, purehot_ctx2);
        swapDataFree(purehot_data3, purehot_ctx3);
        swapDataFree(purehot_data4, purehot_ctx4);
        swapDataFree(hot_data1, hot_ctx1);
        swapDataFree(cold_data1, cold_ctx1);
        swapDataFree(cold_data2, cold_ctx2);
        swapDataFree(warm_data1, warm_ctx1);
        swapDataFree(warm_data2, warm_ctx2);
    }

//    TEST("bitmap - save RDB_TYPE_STRING") {
//
//        robj *purehot_key1 = createStringObject("save_hot_key1",13);
//
//        sds rdb_subval1 = bitmapEncodeSubval(createStringObject(sdsnewlen("1", BITMAP_SUBKEY_SIZE)));
//        sds rdb_subval2 = bitmapEncodeSubval(createStringObject(sdsnewlen("", BITMAP_SUBKEY_SIZE)));
//        sds rdb_subval3 = bitmapEncodeSubval(createStringObject(sdsnewlen("", BITMAP_SUBKEY_SIZE / 2)));
//
//        /* rdbSave */
//        sds coldraw,warmraw,hotraw;
//        rio rdbcold, rdbwarm, rdbhot;
//        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;
//
//        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
//        decodedData _decoded_data, *decoded_data = &_decoded_data;
//        decoded_meta->dbid = decoded_data->dbid = db->id;
//        decoded_meta->key = decoded_data->key = purehot_key1->ptr;
//        decoded_meta->cf = META_CF, decoded_data->cf = DATA_CF;
//        decoded_meta->version = 0, decoded_data->version = 0;
//        decoded_meta->object_type = OBJ_BITMAP, decoded_meta->expire = -1;
//        decoded_data->rdbtype = 0;
//
//        /* rdbSave - save cold, 3 subkeys */
//        dbDelete(db, purehot_key1);
//        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
//        rioInitWithBuffer(&rdbcold,sdsempty());
//        test_assert(rdbKeySaveDataInit(saveData, db, (decodedResult*)decoded_meta) == 0);
//
//        test_assert(bitmapSaveStart(saveData, &rdbcold) == 0);
//
//        decoded_data->subkey = f1, decoded_data->rdbraw = rdb_subval1;
//        decoded_data->version = saveData->object_meta->version;
//        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0);
//
//        decoded_data->subkey = f2, decoded_data->rdbraw = rdb_subval2;
//        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0);
//
//    }

    server.swap_evict_step_max_subkeys = originEvictStepMaxSubkey;
    server.swap_evict_step_max_memory = originEvictStepMaxMemory;

    return error;
}

#endif