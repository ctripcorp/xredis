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

#define BITMAP_SUBKEY_SIZE (server.swap_bitmap_subkey_size) /* default 4KB */
#define BITS_NUM_IN_SUBKEY (BITMAP_SUBKEY_SIZE * 8)
#define BITMAP_MAX_INDEX INT_MAX

#define BITMAP_GET_SUBKEYS_NUM(size, subkey_size) (size == 0 ? 0 : ((int)(((size - 1) / subkey_size + 1))))

#define BITMAP_GET_SPECIFIED_SUBKEY_SIZE(size, subkey_size, idx) \
    (idx == (BITMAP_GET_SUBKEYS_NUM(size, subkey_size) - 1) ? (size - subkey_size * idx) : subkey_size)

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
    return sdsnewlen(&idx,sizeof(long));
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

size_t bitmapMetaGetSize(struct bitmapMeta *bitmap_meta)
{
    serverAssert(bitmap_meta != NULL);
    return bitmap_meta->size;
}

static inline void bitmapMetaInit(bitmapMeta *meta, robj *value)
{
    meta->size = stringObjectLen(value);
    meta->pure_cold_subkeys_num = 0;
    rbmSetBitRange(meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(meta->size, BITMAP_SUBKEY_SIZE) - 1);
}

static inline sds bitmapMetaEncode(bitmapMeta *bm) {
    if (bm == NULL) {
        /* maybe it is just a marker. */
        return encodeBitmapSize(0);
    }
    return encodeBitmapSize(bm->size);
}

static inline bitmapMeta *bitmapMetaDecode(const char *extend, size_t extend_len) {
    serverAssert(extend_len == sizeof(long));
    bitmapMeta *bitmap_meta = bitmapMetaCreate();

    long size = decodeBitmapSize(extend);
    if (size == 0) {
        /* maybe it is just a marker. */
        return NULL;
    }
    bitmap_meta->size = size;
    bitmap_meta->pure_cold_subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE);
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
    return (dest_meta->pure_cold_subkeys_num == src_meta->pure_cold_subkeys_num) && (dest_meta->size == src_meta->size) && rbmIsEqual(dest_meta->subkeys_status, src_meta->subkeys_status);
}

static inline int bitmapMetaGetHotSubkeysNum(bitmapMeta *bitmap_meta, int start_subkey_idx, int end_subkey_idx)
{
    serverAssert(bitmap_meta != NULL);
    serverAssert(start_subkey_idx <= end_subkey_idx && start_subkey_idx >= 0);
    int subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE);
    if (start_subkey_idx >= subkeys_num) {
        return 0;
    }
    if (end_subkey_idx >= subkeys_num) {
        end_subkey_idx = subkeys_num - 1;
    }
    return rbmGetBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
}

#define STATUS_HOT 1
#define STATUS_COLD 0

static inline void bitmapMetaSetSubkeyStatus(bitmapMeta *bitmap_meta, int start_subkey_idx, int end_subkey_idx, int status)
{
    serverAssert(bitmap_meta != NULL);
    serverAssert(start_subkey_idx <= end_subkey_idx && start_subkey_idx >= 0);
    if (status) {
        rbmSetBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
    } else {
        rbmClearBitRange(bitmap_meta->subkeys_status, start_subkey_idx, end_subkey_idx);
    }
}

static inline int bitmapMetaGetSubkeyStatus(bitmapMeta *bitmap_meta, int start_subkey_idx, int end_subkey_idx)
{
    serverAssert(bitmap_meta != NULL);
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
    return rbmLocateSetBitPos(meta->subkeys_status, subkeys_num, (uint32_t*)subkeys_idx);
}

static inline void bitmapMetaGrow(bitmapMeta *bitmap_meta, size_t extendSize)
{
    serverAssert(extendSize > 0);
    if (bitmap_meta == NULL) {
        /* just a marker, no need to grow.*/
        return;
    }
    int old_subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE);
    bitmap_meta->size += extendSize;
    int subkeys_num = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE);

    if (subkeys_num > old_subkeys_num) {
        rbmSetBitRange(bitmap_meta->subkeys_status, old_subkeys_num, subkeys_num - 1);
    }
}

/* bitmap object meta */

objectMeta *createBitmapObjectMeta(uint64_t version, bitmapMeta *bitmap_meta) {
    objectMeta *object_meta = createObjectMeta(SWAP_TYPE_BITMAP, version);
    objectMetaSetPtr(object_meta, bitmap_meta);
    return object_meta;
}

void bitmapObjectMetaDeinit(objectMeta *object_meta) {
    if (object_meta == NULL) return;
    bitmapMetaFree(objectMetaGetPtr(object_meta));
    objectMetaSetPtr(object_meta, NULL);
}

sds bitmapObjectMetaEncode(struct objectMeta *object_meta, void *aux) {
    UNUSED(aux);
    if (object_meta == NULL) return NULL;
    serverAssert(object_meta->swap_type == SWAP_TYPE_BITMAP);
    return bitmapMetaEncode(objectMetaGetPtr(object_meta));
}

int bitmapObjectMetaDecode(struct objectMeta *object_meta, const char *extend, size_t extlen) {
    serverAssert(object_meta->swap_type == SWAP_TYPE_BITMAP);
    serverAssert(objectMetaGetPtr(object_meta) == NULL);
    objectMetaSetPtr(object_meta, bitmapMetaDecode(extend, extlen));
    return 0;
}

int bitmapObjectMetaIsHot(struct objectMeta *object_meta, robj *value)
{
    UNUSED(value);
    serverAssert(object_meta && object_meta->swap_type == SWAP_TYPE_BITMAP);
    if (bitmapObjectMetaIsMarker(object_meta)) {
        /* ptr not set, bitmap is purely hot, never swapped out */
        return 1;
    } else {
        bitmapMeta *meta = objectMetaGetPtr(object_meta);
        return meta->pure_cold_subkeys_num == 0;
    }
}

void bitmapObjectMetaDup(struct objectMeta *dup_meta, struct objectMeta *object_meta) {
    if (object_meta == NULL) return;
    serverAssert(dup_meta->swap_type == SWAP_TYPE_BITMAP);
    serverAssert(objectMetaGetPtr(dup_meta) == NULL);
    if (objectMetaGetPtr(object_meta) == NULL) return;
    objectMetaSetPtr(dup_meta, bitmapMetaDup(objectMetaGetPtr(object_meta)));
}

sds bitmapMetaDump(sds result, bitmapMeta *bm) {
    if (bm == NULL) {
        result = sdscatprintf(result,"(marker=true,size=0,pure_cold_subkeys_num=0,)");
    } else {
        result = sdscatprintf(result,"(marker=false,size=%ld,pure_cold_subkeys_num=%d,)",
            bm->size,bm->pure_cold_subkeys_num);
    }
    return result;
}

int bitmapObjectMetaEqual(struct objectMeta *dest_om, struct objectMeta *src_om) {
    bitmapMeta *dest_meta = objectMetaGetPtr(dest_om);
    bitmapMeta *src_meta = objectMetaGetPtr(src_om);
    return bitmapMetaEqual(dest_meta, src_meta);
}

int bitmapObjectMetaRebuildFeed(struct objectMeta *rebuild_meta,
                              uint64_t version, const char *subkey, size_t sublen, robj *subval) {
    UNUSED(version);
    bitmapMeta *meta = objectMetaGetPtr(rebuild_meta);
    serverAssert(meta != NULL);
    if (sublen != sizeof(long)) return -1;
    if (subval == NULL || subval->type != OBJ_STRING) return -1;
    if (subkey) {
        meta->pure_cold_subkeys_num++;
        meta->size += stringObjectLen(subval);
        return 0;
    } else {
        return -1;
    }
}

objectMetaType bitmapObjectMetaType = {
        .encodeObjectMeta = bitmapObjectMetaEncode,
        .decodeObjectMeta = bitmapObjectMetaDecode,
        .objectIsHot = bitmapObjectMetaIsHot,
        .free = bitmapObjectMetaDeinit,
        .duplicate = bitmapObjectMetaDup,  /* only used in rdbKeySaveDataInitCommon. */
        .equal = bitmapObjectMetaEqual,   /* only used in keyLoadFixAna. */
        .rebuildFeed = bitmapObjectMetaRebuildFeed
};

/* delta bitmap */
/* just used in swap out process. */
typedef struct deltaBitmap {
    sds *subvals;
    size_t subvals_total_len;
    int subkeys_num;
    int *subkeys_logic_idx;
} deltaBitmap;


static inline deltaBitmap *deltaBitmapCreate(int num)
{
    deltaBitmap *delta_bm = zmalloc(sizeof(deltaBitmap));
    delta_bm->subvals = zmalloc(sizeof(sds) * num);
    memset(delta_bm->subvals, 0, sizeof(sds) * num);
    delta_bm->subkeys_logic_idx = zmalloc(num * sizeof(int));
    memset(delta_bm->subkeys_logic_idx, 0, num * sizeof(int));
    delta_bm->subkeys_num = 0;
    delta_bm->subvals_total_len = 0;
    return delta_bm;
}

static inline void deltaBitmapFree(deltaBitmap *delta_bm)
{
    zfree(delta_bm->subkeys_logic_idx);
    for (int i = 0; i < delta_bm->subkeys_num; i++) {
        if (delta_bm->subvals[i] != NULL) {
            sdsfree(delta_bm->subvals[i]);
            delta_bm->subvals[i] = NULL;
        }
    }
    zfree(delta_bm->subvals);
    zfree(delta_bm);
}

robj *deltaBitmapMakeBitmapObject(bitmapMeta *meta, deltaBitmap *delta_bm)
{
    robj *new_bitmap = createRawStringObject(NULL, delta_bm->subvals_total_len);

    size_t offset_in_new_bitmap = 0;

    for (int i = 0; i < delta_bm->subkeys_num; i++) {
        memcpy((char*)new_bitmap->ptr + offset_in_new_bitmap, delta_bm->subvals[i], sdslen(delta_bm->subvals[i]));
        offset_in_new_bitmap += sdslen(delta_bm->subvals[i]);

        /* update meta */
        bitmapMetaSetSubkeyStatus(meta, delta_bm->subkeys_logic_idx[i], delta_bm->subkeys_logic_idx[i], STATUS_HOT);
    }
    serverAssert(delta_bm->subvals_total_len == offset_in_new_bitmap);

    meta->pure_cold_subkeys_num -= delta_bm->subkeys_num;

    return new_bitmap;
}

robj *bitmapObjectMerge(robj *bitmap_object, bitmapMeta *meta, deltaBitmap *delta_bm)
{
    serverAssert(bitmap_object && meta);
    size_t old_obj_len = stringObjectLen(bitmap_object);

    long needed_merge_in_len = 0;

    for (int i = 0; i < delta_bm->subkeys_num; i++) {
        if (bitmapMetaGetSubkeyStatus(meta, delta_bm->subkeys_logic_idx[i], delta_bm->subkeys_logic_idx[i]) == 0) {
            needed_merge_in_len += sdslen(delta_bm->subvals[i]);
        }
    }

    robj *new_bitmap = createRawStringObject(NULL, old_obj_len + needed_merge_in_len);

    size_t offset_in_new_bitmap = 0;
    size_t offset_in_old_bitmap = 0;

    int subkeys_idx_cursor = 0;

    size_t actual_new_bitmap_len = 0;

    for (int i = 0; i < delta_bm->subkeys_num; i++) {
        int subkeys_num_ahead = 0;
        if (subkeys_idx_cursor <= delta_bm->subkeys_logic_idx[i] - 1) {
            subkeys_num_ahead = bitmapMetaGetHotSubkeysNum(meta, subkeys_idx_cursor, delta_bm->subkeys_logic_idx[i] - 1);
        }
        if (subkeys_num_ahead != 0) {
            memcpy((char*)new_bitmap->ptr + offset_in_new_bitmap, (char*)bitmap_object->ptr + offset_in_old_bitmap, BITMAP_SUBKEY_SIZE * subkeys_num_ahead);
            offset_in_new_bitmap += BITMAP_SUBKEY_SIZE * subkeys_num_ahead;
            offset_in_old_bitmap += BITMAP_SUBKEY_SIZE * subkeys_num_ahead;
            actual_new_bitmap_len += BITMAP_SUBKEY_SIZE * subkeys_num_ahead;
        }

        if (bitmapMetaGetSubkeyStatus(meta, delta_bm->subkeys_logic_idx[i], delta_bm->subkeys_logic_idx[i]) == 1) {
            /* subkey both exist in redis and rocksDb.
             * need to copy the buffer in old bitmap, so remain the hot subkey copying in next operation. */
            subkeys_idx_cursor = delta_bm->subkeys_logic_idx[i];
        } else {
            memcpy((char*)new_bitmap->ptr + offset_in_new_bitmap, delta_bm->subvals[i], sdslen(delta_bm->subvals[i]));
            offset_in_new_bitmap += sdslen(delta_bm->subvals[i]);
            actual_new_bitmap_len += sdslen(delta_bm->subvals[i]);
            subkeys_idx_cursor = delta_bm->subkeys_logic_idx[i] + 1;

            /* update meta */
            meta->pure_cold_subkeys_num -= 1;
            bitmapMetaSetSubkeyStatus(meta, delta_bm->subkeys_logic_idx[i], delta_bm->subkeys_logic_idx[i], STATUS_HOT);
        }

    }

    if (offset_in_old_bitmap < old_obj_len) {
        /* copy buffer behind subkeys in delta_bm. */
        memcpy((char*)new_bitmap->ptr + offset_in_new_bitmap, (char*)bitmap_object->ptr + offset_in_old_bitmap, old_obj_len - offset_in_old_bitmap);
        actual_new_bitmap_len += (old_obj_len - offset_in_old_bitmap);
    }

    serverAssert(old_obj_len + needed_merge_in_len == actual_new_bitmap_len);
    return new_bitmap;
}

typedef struct bitmapDataCtx {
    int ctx_flag;
    size_t subkeys_total_size;  /* only used in swap out */
    int subkeys_num;
    int *subkeys_logic_idx;
    robj *newbitmap; /* ref, only used in swap out  */
    argRewriteRequest arg_reqs[2];
} bitmapDataCtx;

/* bitmap swap ana */

static inline bitmapMeta *swapDataGetBitmapMeta(swapData *data) {
    objectMeta *object_meta = swapDataObjectMeta(data);
    return object_meta ? objectMetaGetPtr(object_meta) : NULL;
}

void bitmapSwapAnaInSelectSubKeys(swapData *data, bitmapDataCtx *datactx, struct keyRequest *req)
{
    objectMeta *object_meta = swapDataObjectMeta(data);
    serverAssert(!bitmapObjectMetaIsMarker(object_meta));
    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    serverAssert(meta != NULL);

    long required_subkey_start_idx = 0;
    long required_subkey_end_idx = 0;

    range *range = req->l.ranges;

    int subkeys_num = BITMAP_GET_SUBKEYS_NUM(meta->size, BITMAP_SUBKEY_SIZE);

    if (range->type == RANGE_BIT_BITMAP) {
        serverAssert(range->start == range->end);
        if (range->start < 0) {
            /* setbit, getbit command, argument offset is non-negative, 
             * which will be check firstly in setbitCommand/getbitCommand,
             * so no need to swap in. */
            return;
        }
        required_subkey_start_idx = range->start / BITS_NUM_IN_SUBKEY;


        /* when setbit command required subkeys range that exceeded the max subkey idx,
            * then maybe redis will extend for a bigger new bitmap.
            * so some operations here will be a little tricky, we need ensure the last subkey will be swap in.
            * getbit command is treated same as setbit.
            * */
        if (required_subkey_start_idx > subkeys_num - 1) {
            required_subkey_start_idx = subkeys_num - 1;
        }

        required_subkey_end_idx = required_subkey_start_idx;
    }

    if (range->type == RANGE_BYTE_BITMAP) {
        /* bitcount, bitpos command, maybe argument offset is negative. */
        long long start = range->start;
        if (range->start < 0) {
            start = range->start + meta->size;
            if (start < 0) {
                start = 0;
            }
        }
        required_subkey_start_idx = start / BITMAP_SUBKEY_SIZE;

        long long end = range->end;
        if (range->end < 0) {
            end = range->end + meta->size;
            if (end < 0) {
                end = 0;
            }
        }
        if (end >= meta->size)
            end = meta->size - 1;
        required_subkey_end_idx = end / BITMAP_SUBKEY_SIZE;
    }

    /* bitcount, bitpos command, if start > end after adjusting, it's treated as error, 
     * which will be checked after checking if the object exist in bitposCommand/bitcountCommand,
     * so we need to ensure the hot object exist. */
    if (required_subkey_start_idx > required_subkey_end_idx) {
        required_subkey_start_idx = 0;
        required_subkey_end_idx = 0;
    }

    int subkey_num_need_swapin = required_subkey_end_idx - required_subkey_start_idx + 1 - bitmapMetaGetHotSubkeysNum(meta, required_subkey_start_idx, required_subkey_end_idx);

    serverAssert(subkey_num_need_swapin >= 0);

    int hot_subkeys_num = 0;
    if (data->value != NULL) {
        hot_subkeys_num = BITMAP_GET_SUBKEYS_NUM(stringObjectLen(data->value), BITMAP_SUBKEY_SIZE);
    }
    serverAssert(hot_subkeys_num + subkey_num_need_swapin <= subkeys_num);

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

    datactx->subkeys_logic_idx = zmalloc(sizeof(int) * subkey_num_need_swapin);
    int cursor = 0;
    /* record idx of subkey to swap in. */
    for (int i = required_subkey_start_idx; i <= required_subkey_end_idx; i++) {
        if (req->cmd_intention_flags == SWAP_IN_DEL || data->value == NULL ||
            bitmapMetaGetHotSubkeysNum(meta, i, i) == 0) {
            datactx->subkeys_logic_idx[cursor++] = i;
        }
    }
    datactx->subkeys_num = cursor;
}

#define SELECT_MAIN 0
#define SELECT_DSS  1

int bitmapSwapAnaOutSelectSubkeys(swapData *data, bitmapDataCtx *datactx, int *may_keep_data)
{
    int noswap; /* if need to swap ? */
    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    serverAssert(meta != NULL);

    int subkeys_num = BITMAP_GET_SUBKEYS_NUM(meta->size, BITMAP_SUBKEY_SIZE);

    int hot_subkeys_num = BITMAP_GET_SUBKEYS_NUM(stringObjectLen(data->value), BITMAP_SUBKEY_SIZE);
    
    int hot_subkeys_num_expected = bitmapMetaGetHotSubkeysNum(meta, 0, subkeys_num - 1);
    
    serverAssert(hot_subkeys_num == hot_subkeys_num_expected);

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
    *may_keep_data = 1;

    /* from left to right to select subkeys */
    for (int i = 0; i < hot_subkeys_num; i++) {

        size_t subval_size = BITMAP_GET_SPECIFIED_SUBKEY_SIZE(stringObjectLen(data->value), BITMAP_SUBKEY_SIZE, i);

        if (datactx->subkeys_total_size + subval_size > server.swap_evict_step_max_memory ||
            datactx->subkeys_num + 1 > subkeys_num_may_swapout) {
            if (!noswap) *may_keep_data = 0;
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
                    datactx->ctx_flag |= BIG_DATA_CTX_FLAG_MOCK_VALUE;
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

                    /* swap in first element if cold */
                    datactx->subkeys_num = 1;
                    datactx->subkeys_logic_idx = zmalloc(sizeof(int));
                    datactx->subkeys_logic_idx[0] = 0;
                    *intention = SWAP_IN;
                    *intention_flags = 0;
                } else {
                    /* string, keyspace ... operation */
                    /* swap in all subkeys, and keep them in rocks. */
                    datactx->subkeys_num = -1; /* all subKey of bitmap need to swap in */
                    *intention = SWAP_IN;
                    *intention_flags = 0;
                }
            } else { /* range requests */

                bitmapSwapAnaInSelectSubKeys(data, datactx, req);

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
                if (!swapDataPersisted(data)) {
                    /* bitmap marker must exist. */
                    objectMeta *object_meta = swapDataObjectMeta(data);
                    serverAssert(object_meta != NULL);
                    bitmapMarkerTransToMetaIfNeeded(object_meta, data->value);
                }

                int may_keep_data;

                int noswap = bitmapSwapAnaOutSelectSubkeys(data, datactx, &may_keep_data);
                int keep_data = swapDataPersistKeepData(data,cmd_intention_flags,may_keep_data);

                if (noswap) {
                    /* directly evict value from db.dict if not dirty. */
                    swapDataCleanObject(data,datactx,keep_data);
                    if (datactx->newbitmap != NULL && stringObjectLen(datactx->newbitmap) == 0) {
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
            *intention = SWAP_DEL;
            *intention_flags = 0;
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
        sds keyStr = bitmapEncodeSubkeyIdx(datactx->subkeys_logic_idx[i]);
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
    serverAssert(o != NULL && phyIdx >= 0);
    size_t subval_size = BITMAP_GET_SPECIFIED_SUBKEY_SIZE(stringObjectLen(o), BITMAP_SUBKEY_SIZE, phyIdx);

    robj *decoded_bitmap = getDecodedObject(o);
    robj *subval_obj = createStringObject((char*)decoded_bitmap->ptr + phyIdx * BITMAP_SUBKEY_SIZE, subval_size);
    decrRefCount(decoded_bitmap);
    
    return subval_obj;   
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

        int logicIdx = datactx->subkeys_logic_idx[i];
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

    deltaBitmap *delta_bm = deltaBitmapCreate(num);

    uint64_t version = swapDataObjectVersion(data);

    int subkeys_cursor = 0;
    for (int i = 0; i < num; i++) {
        int dbid;
        const char *keystr, *subkeystr;
        size_t klen, slen;
        robj *subvalobj;
        uint64_t subkey_version;
        int subkey_idx;

        if (rawvals[i] == NULL) {
            continue;
        }
        if (rocksDecodeDataKey(rawkeys[i],sdslen(rawkeys[i]),
                               &dbid,&keystr,&klen,&subkey_version,&subkeystr,&slen) < 0)
            continue;
        if (!swapDataPersisted(data))
            continue;
        if (version != subkey_version)
            continue;

        subkey_idx = bitmapDecodeSubkeyIdx(subkeystr, slen);

        delta_bm->subkeys_logic_idx[subkeys_cursor] = subkey_idx;

        subvalobj = rocksDecodeValRdb(rawvals[i]);
        serverAssert(subvalobj->type == OBJ_STRING);
        /* subvalobj might be shared integer, unshared it before
         * add to decoded. */
        delta_bm->subvals_total_len += stringObjectLen(subvalobj);
        subvalobj = unshareStringValue(subvalobj);
        /* steal subvalobj sds */
        sds subval = subvalobj->ptr;
        subvalobj->ptr = NULL;

        delta_bm->subvals[subkeys_cursor] = subval;
        decrRefCount(subvalobj);
        subkeys_cursor++;
    }
    delta_bm->subkeys_num = subkeys_cursor;
    *pdecoded = delta_bm;
    return 0;
}

void *bitmapCreateOrMergeObject(swapData *data, void *decoded_, void *datactx)
{
    UNUSED(datactx);
    robj *result = NULL;
    deltaBitmap *delta_bm = (deltaBitmap *)decoded_;

    if (delta_bm->subkeys_num == 0) {
        return NULL;
    }

    if (swapDataIsCold(data)) {
        bitmapMeta *meta = swapDataGetBitmapMeta(data);
        result = deltaBitmapMakeBitmapObject(meta, delta_bm);
    } else {
        bitmapMeta *meta = swapDataGetBitmapMeta(data);

        robj *decoded_bitmap = getDecodedObject(data->value);

        robj *merged_obj = bitmapObjectMerge(decoded_bitmap, meta, delta_bm);

        decrRefCount(decoded_bitmap);

        merged_obj->lru = data->value->lru;
        merged_obj->dirty_data = data->value->dirty_data;
        merged_obj->dirty_meta = data->value->dirty_meta;
        merged_obj->persist_keep = data->value->persist_keep;
        merged_obj->persistent = data->value->persistent; 

        /* replace the old object */
        decrRefCount(data->value);
        dbOverwrite(data->db,data->key,merged_obj);

        data->value = merged_obj;
        incrRefCount(data->value);

        result = NULL;
    }

    /* bitmap only swap in the subkeys that don't exist in redis, so no need to judge like hash, set. */
    deltaBitmapFree(delta_bm);
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
    if (swapDataIsCold(data) && result != NULL /* may be empty */) {
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
        if (result) {
            decrRefCount(result);
        }
        if (data->value) overwriteObjectPersistent(data->value,!data->persistence_deleted);
    }

    return 0;
}

int bitmapCleanObject(swapData *data, void *datactx_, int keep_data) {
    bitmapDataCtx *datactx = datactx_;
    
    if (swapDataIsCold(data) || datactx->subkeys_num == 0) {
        return 0;
    }

    bitmapMeta *meta = swapDataGetBitmapMeta(data);
    if (!keep_data) {
        robj *decoded_bitmap = getDecodedObject(data->value);

        /* from subkey idx = 0 to max idx to evict subkeys in bitmap.
         * the evicting way determined in bitmapSwapAnaOutSelectSubkeys */
        serverAssert(datactx->subkeys_num > 0);
        int evicted_max_idx = datactx->subkeys_logic_idx[datactx->subkeys_num - 1];

        serverAssert(meta != NULL);
        long hot_subkeys_num = bitmapMetaGetHotSubkeysNum(meta, 0, evicted_max_idx);
        serverAssert(hot_subkeys_num == datactx->subkeys_num);

        size_t bitmap_size = stringObjectLen(decoded_bitmap);
        serverAssert(datactx->subkeys_total_size != 0);
        size_t left_size = bitmap_size - datactx->subkeys_total_size;
        datactx->newbitmap = createRawStringObject((char*)decoded_bitmap->ptr + datactx->subkeys_total_size, left_size);

        decrRefCount(decoded_bitmap);

        /* update the subkey status in meta*/
        for (int i = 0; i < datactx->subkeys_num; i++) {
            serverAssert(datactx->subkeys_logic_idx[i] <= evicted_max_idx);
            bitmapMetaSetSubkeyStatus(meta, datactx->subkeys_logic_idx[i], datactx->subkeys_logic_idx[i], STATUS_COLD);
        }
        meta->pure_cold_subkeys_num += datactx->subkeys_num;
    }
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

    bitmapDataCtx *datactx = datactx_;

    if (datactx->newbitmap == NULL) {
        /* keep data */
        if (totally_out) *totally_out = 0;
        return 0;
    } else {
        size_t bitmap_old_size = stringObjectLen(data->value);
        size_t bitmap_new_size = stringObjectLen(datactx->newbitmap);

        if (bitmap_new_size == 0) {
            /* all subkeys swapped out, key turnning into cold:
                * - rocks-meta should have already persisted.
                * - object_meta and value will be deleted by dbDelete, expire already
                *   deleted by swap framework. */
            dbDelete(data->db,data->key);
            decrRefCount(datactx->newbitmap);  /* newbitmap is useless ever, free it! */
            datactx->newbitmap = NULL;
            if (totally_out) *totally_out = 1;
        } else if (bitmap_new_size < bitmap_old_size) {
            /* replace value with new bitmap. */

            datactx->newbitmap->lru = data->value->lru;
            datactx->newbitmap->dirty_data = data->value->dirty_data;
            datactx->newbitmap->dirty_meta = data->value->dirty_meta;
            datactx->newbitmap->persist_keep = data->value->persist_keep;
            datactx->newbitmap->persistent = data->value->persistent;

            decrRefCount(data->value);
            dbOverwrite(data->db,data->key,datactx->newbitmap);

            data->value = datactx->newbitmap;
            incrRefCount(data->value);

            datactx->newbitmap = NULL;

            setObjectPersistent(data->value); /* persistent data exist. */
            if (totally_out) *totally_out = 0;
        } else {
            /* no keep data, bitmap_new_size != bitmap_old_size */
            serverAssert(false);
        }
    }
    return 0;
}

// int bitmapSwapOut(swapData *data, void *datactx_, int keep_data, int *totally_out) {
//     UNUSED(datactx_);
//     serverAssert(!swapDataIsCold(data));

//     if (keep_data) {
//         clearObjectDataDirty(data->value);
//         setObjectPersistent(data->value);
//     }

//     if (stringObjectLen(data->value) == 0) {
//         /* all fields swapped out, key turnning into cold:
//          * - rocks-meta should have already persisted.
//          * - object_meta and value will be deleted by dbDelete, expire already
//          *   deleted by swap framework. */
//         dbDelete(data->db,data->key);
//         if (totally_out) *totally_out = 1;
//     } else {
//         setObjectPersistent(data->value); /* persistent data exist. */
//         if (totally_out) *totally_out = 0;

//     }
//     return 0;
// }

static inline void mockBitmapForDeleteIfCold(swapData *data)
{
    if (swapDataIsCold(data)) {
        dbAdd(data->db, data->key, createStringObject("",0));
    }
}

int bitmapSwapDel(swapData *data, void *datactx_, int del_skip) {
    bitmapDataCtx* datactx = (bitmapDataCtx*)datactx_;
    if (datactx->ctx_flag & BIG_DATA_CTX_FLAG_MOCK_VALUE) {
        mockBitmapForDeleteIfCold(data);
    }

    if (del_skip) {
        if (!swapDataIsCold(data)) {
            /* different from bighash, set, no need to delete meta, just free bitmap meta, keep bitmap Marker in db.meta. */
            objectMeta *meta = lookupMeta(data->db,data->key);
            serverAssert(meta != NULL);
            bitmapMetaTransToMarkerIfNeeded(meta);
        }
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
    bitmapDataCtx *datactx = datactx_;
    zfree(datactx->subkeys_logic_idx);
    datactx->newbitmap = NULL;
    zfree(datactx);
}

objectMeta *createBitmapObjectMarker() {
    return createBitmapObjectMeta(swapGetAndIncrVersion(), NULL);
}

int bitmapObjectMetaIsMarker(objectMeta *object_meta) {
    serverAssert(object_meta->swap_type == SWAP_TYPE_BITMAP);
    return NULL == objectMetaGetPtr(object_meta);
}

void bitmapSetObjectMarkerIfNeeded(redisDb *db, robj *key) {
    objectMeta *object_meta = lookupMeta(db,key);
    if (object_meta == NULL) {
        dbAddMeta(db,key,createBitmapObjectMarker());
    }
}

void bitmapClearObjectMarkerIfNeeded(redisDb *db, robj *key) {
    objectMeta *object_meta = lookupMeta(db,key);
    if (object_meta && bitmapObjectMetaIsMarker(object_meta)) {
        dbDeleteMeta(db,key);
    }
}

void bitmapMetaTransToMarkerIfNeeded(objectMeta *object_meta) {
    if (!bitmapObjectMetaIsMarker(object_meta)) {
        bitmapMetaFree(objectMetaGetPtr(object_meta));
        objectMetaSetPtr(object_meta, NULL);
    }
}

void bitmapMarkerTransToMetaIfNeeded(objectMeta *object_meta, robj *value) {
    serverAssert(value != NULL);
    if (bitmapObjectMetaIsMarker(object_meta)) {
        bitmapMeta *meta = bitmapMetaCreate();
        bitmapMetaInit(meta, value);
        objectMetaSetPtr(object_meta, meta);
    }
}

int bitmapBeforeCall(swapData *data, keyRequest *key_request, client *c,
        void *datactx_) {

    /* Clear bitmap marker if non-bitmap command touching bitmap. 
     * note: all string command: set,get,etc
     *       key space set command: sunionstore,sdiffstore
     *       key space zset command:zunionstore,zinterstore,zdiffstore
     *                    georadius ... STORE key,
     * it's sure no need to rewrite. */
    if (key_request && (key_request->cmd_flags & CMD_SWAP_DATATYPE_STRING ||
        ((key_request->cmd_flags & CMD_SWAP_DATATYPE_SET) && (key_request->cmd_flags & CMD_SWAP_DATATYPE_KEYSPACE)) ||
        ((key_request->cmd_flags & CMD_SWAP_DATATYPE_ZSET) && (key_request->cmd_flags & CMD_SWAP_DATATYPE_KEYSPACE)))) {
        bitmapClearObjectMarkerIfNeeded(data->db,data->key);
        if (key_request->cmd_flags & CMD_SWAP_DATATYPE_STRING) {
            atomicIncr(server.swap_bitmap_switched_to_string_count, 1);
        }
        return 0;
    }

    bitmapDataCtx *datactx = datactx_;
    argRewriteRequest first_arg_req = datactx->arg_reqs[0];

    /* 1. no arg need to rewrite.
     * 2. no need to rewrite bitcount,bitpos arg. */
    if (first_arg_req.arg_idx < 0 || first_arg_req.arg_type == RANGE_BYTE_BITMAP) {
        return 0;
    }
    objectMeta *object_meta = lookupMeta(data->db,data->key);

    /* no bitmap in memory, no need to rewrite for hot bitmap. */
    if (object_meta == NULL) {
        return 0;
    }

    serverAssert(object_meta != NULL && object_meta->swap_type == SWAP_TYPE_BITMAP);

    /* no need to rewrite for hot bitmap. */
    if (bitmapObjectMetaIsHot(object_meta, lookupKey(data->db,data->key,LOOKUP_NOTOUCH))) {
        return 0;
    }

    serverAssert(!bitmapObjectMetaIsMarker(object_meta));
    bitmapMeta *bitmap_meta = objectMetaGetPtr(object_meta);

    for (int i = 0; i < 2; i++) {
        argRewriteRequest arg_req = datactx->arg_reqs[i];
        /* 1. impossible to modify when the arg_idx = 0 and -1 (no need to rewrite)
         * 2. the arg needed to rewrite maybe not exist. */
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

        if (offset < 0) {
            /* setbit, getbit command, argument offset is non-negative.
             * command must be syntax error, just return. */
            return 0;
        }

        int subkey_idx = offset / BITS_NUM_IN_SUBKEY;

        if (subkey_idx == 0) {
            /* no hole in front of idx = 0 of bitmap, no need to rewrite. */
            continue;
        }
        int subkeys_num_ahead = bitmapMetaGetHotSubkeysNum(bitmap_meta, 0, subkey_idx - 1);

        int cold_subkeys_num_ahead = subkey_idx - subkeys_num_ahead;
        if (subkeys_num_ahead == subkey_idx) {
            /* no need to modify offset */
            continue;
        } else {
            /* setbit, getbit */
            long long newOffset = offset - cold_subkeys_num_ahead * BITS_NUM_IN_SUBKEY;

            robj *new_arg = createObject(OBJ_STRING,sdsfromlonglong(newOffset));
            clientArgRewrite(c, arg_req, new_arg);
        }
    }

    return 0;
}

int bitmapMergedIsHot(swapData *d, void *result, void *datactx) {
    UNUSED(result);
    UNUSED(datactx);
    objectMeta *meta = swapDataObjectMeta(d);
    return bitmapObjectMetaIsHot(meta, NULL);
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
    bitmapDataCtx *datactx = zmalloc(sizeof(bitmapDataCtx));
    memset(datactx, 0, sizeof(bitmapDataCtx));
    argRewriteRequestInit(datactx->arg_reqs + 0);
    argRewriteRequestInit(datactx->arg_reqs + 1);
    *pdatactx = datactx;
    return 0;
}

void metaBitmapInit(metaBitmap *meta_bitmap, struct bitmapMeta *bitmap_meta, robj *bitmap)
{
    serverAssert(meta_bitmap != NULL && bitmap != NULL);
    meta_bitmap->meta = bitmap_meta;
    meta_bitmap->bitmap = bitmap;
}

long metaBitmapGetSize(metaBitmap *meta_bitmap)
{
    return meta_bitmap->meta->size;
}

void metaBitmapGrow(metaBitmap *meta_bitmap, size_t byte)
{
    size_t oldlen = stringObjectLen(meta_bitmap->bitmap);
    meta_bitmap->bitmap->ptr = sdsgrowzero(meta_bitmap->bitmap->ptr, byte);
    size_t newlen = stringObjectLen(meta_bitmap->bitmap);
    if (newlen == oldlen) {
        return;
    }
    serverAssert(newlen > oldlen);
    /* newlen > oldlen */
    bitmapMetaGrow(meta_bitmap->meta, newlen - oldlen);
    return;
}

/* metaBitmap get the size of cold subkeys ahead of the offset, not include the subkey that offset exists. */
long metaBitmapGetColdSubkeysSize(metaBitmap *meta_bitmap, long offset)
{
    if (meta_bitmap->meta == NULL) {
        /* object meta is just marker, bitmap meta is NULL, bitmap is pure hot. */
        return 0;
    }

    int subkey_idx = offset / BITMAP_SUBKEY_SIZE;
    if (subkey_idx == 0) {
        /* no cold subkeys ahead. */
        return 0;
    }

    int subkeys_num_ahead = bitmapMetaGetHotSubkeysNum(meta_bitmap->meta, 0, subkey_idx - 1);
    int cold_subkeys_num_ahead = subkey_idx - subkeys_num_ahead;

    if (subkeys_num_ahead == subkey_idx) {
        /* no cold subkeys ahead. */
        return 0;
    } else {
        return cold_subkeys_num_ahead * BITMAP_SUBKEY_SIZE;
    }
}

/* bitmap save */

 /* only used for bitmap saving process, subkey of subkey_idx or offset has not been saved. */
typedef struct bitmapSaveIterator {
    int subkey_idx; /* logic subkey idx, the subkey has not been saved. */
    size_t offset;  /* points to the hot bitmap, the buffer behind offset (include offset) has not been saved. */
} bitmapSaveIterator;

void *bitmapTypeCreateSaveIter()
{
    bitmapSaveIterator *iter = zmalloc(sizeof(bitmapSaveIterator));
    iter->subkey_idx = 0;
    iter->offset = 0;
    return iter;
}

void bitmapTypeReleaseSaveIter(bitmapSaveIterator *iter) {
    zfree(iter);
}

int bitmapSaveStart(rdbKeySaveData *save, rio *rdb) {
    robj *key = save->key;
    /* save header */
    if (rdbSaveKeyHeader(rdb,key,key,save->rdbtype,save->expire) == -1) {
        return -1;
    }

    bitmapMeta *bitmap_meta = objectMetaGetPtr(save->object_meta);

    serverAssert(bitmap_meta != NULL);
    if ((rdbSaveLen(rdb, bitmap_meta->size)) == -1) {
        return -1;
    }

    if (save->rdbtype == RDB_TYPE_BITMAP) {
        if ((rdbSaveLen(rdb, BITMAP_SUBKEY_SIZE)) == -1)
            return -1;
    }

    return 0;
}

int bitmapSaveToRdbString(rdbKeySaveData *save, rio *rdb)
{
    if (rdbSaveStringObject(rdb, save->value) == -1) {
        return -1;
    }
    return 0;
}

int bitmapSaveToRdbBitmap(rdbKeySaveData *save, rio *rdb)
{
    size_t bitmap_size = 0;

    bitmapMeta *bitmap_meta = objectMetaGetPtr(save->object_meta);
    if (bitmap_meta == NULL) {
        /* just a marker */
        bitmap_size = stringObjectLen(save->value);
    } else {
        bitmap_size = bitmap_meta->size;
        serverAssert(bitmap_meta->pure_cold_subkeys_num == 0 && bitmap_meta->size == stringObjectLen(save->value));
    }

    if ((rdbSaveLen(rdb, bitmap_size)) == -1)
        return -1;
    if ((rdbSaveLen(rdb, BITMAP_SUBKEY_SIZE)) == -1)
        return -1;

    /* hot bitmap object will be saved here, not like other types which saved in rdbSaveKeyValuePair(). */
    size_t waiting_save_size = bitmap_size;

    size_t subkey_size = BITMAP_SUBKEY_SIZE;
    size_t offset = 0;
    robj *decoded_bitmap = getDecodedObject(save->value);

    while (waiting_save_size > 0)
    {
        size_t hot_subkey_size = waiting_save_size < subkey_size ? waiting_save_size : subkey_size;
        sds subval = sdsnewlen((char*)decoded_bitmap->ptr + offset, hot_subkey_size);

        robj subvalobj;
        initStaticStringObject(subvalobj, subval);

        if (rdbSaveStringObject(rdb, &subvalobj) == -1) {
            sdsfree(subval);
            decrRefCount(decoded_bitmap);
            return -1;
        }

        sdsfree(subval);

        offset += hot_subkey_size;
        waiting_save_size -= hot_subkey_size;
    }
    
    decrRefCount(decoded_bitmap);
    serverAssert(waiting_save_size == 0);

    return 0;
}

int bitmapSaveHotExtension(rdbKeySaveData *save, rio *rdb) {
    robj *key = save->key;
    /* save header */
    if (rdbSaveKeyHeader(rdb,key,key,save->rdbtype,save->expire) == -1) {
        return -1;
    }

    if (save->rdbtype == RDB_TYPE_STRING) {
        return bitmapSaveToRdbString(save, rdb);
    } else {
        return bitmapSaveToRdbBitmap(save, rdb);
    }
}

/* save subkeys in memory untill subkey idx(not included) */
int bitmapSaveHotSubkeysUntill(rdbKeySaveData *save, rio *rdb, int idx, int rdbtype) {

    bitmapSaveIterator *iter = save->iter;
    /* no hot subkeys in the front of idx. */
    if (iter->subkey_idx >= idx) {
        return 0;
    }

    robj *decoded_bitmap = getDecodedObject(save->value);

    bitmapMeta *bitmap_meta = objectMetaGetPtr(save->object_meta);
    serverAssert(bitmap_meta != NULL);

    int hot_subkeys_num_to_save = bitmapMetaGetHotSubkeysNum(bitmap_meta, iter->subkey_idx, idx - 1);
    serverAssert(idx - iter->subkey_idx == hot_subkeys_num_to_save);

    if (rdbtype == RDB_TYPE_STRING) {
        /* RDB_TYPE_STRING */

        size_t hot_subkeys_size_to_save;

        int subkeys_sum = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE);
        if (idx >= subkeys_sum) {
            /* hot subkeys to be saved include the last subkey. */
            hot_subkeys_size_to_save = stringObjectLen(decoded_bitmap) - iter->offset;
        } else {
            hot_subkeys_size_to_save = hot_subkeys_num_to_save * BITMAP_SUBKEY_SIZE;
        }

        if (rdbWriteRaw(rdb, (char*)decoded_bitmap->ptr + iter->offset, hot_subkeys_size_to_save) == -1) {
            decrRefCount(decoded_bitmap);
            return -1;
        }

        iter->subkey_idx = idx;
        iter->offset += hot_subkeys_size_to_save;
        decrRefCount(decoded_bitmap);

        return 0;
    }

    /* RDB_TYPE_BITMAP */
    int subkeys_sum = BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE);
    size_t hot_subkey_size;

    for (int i = iter->subkey_idx; i <= idx - 1; i++) {
        if (i == subkeys_sum - 1) {
            /* hot subkeys to save include the last subkey. */
            hot_subkey_size = stringObjectLen(decoded_bitmap) - iter->offset;
        } else {
            hot_subkey_size = BITMAP_SUBKEY_SIZE;
        }

        sds subval = sdsnewlen((char*)decoded_bitmap->ptr + iter->offset, hot_subkey_size);
        robj subvalobj;
        initStaticStringObject(subvalobj, subval);

        if (rdbSaveStringObject(rdb, &subvalobj) == -1) {
            sdsfree(subval);
            decrRefCount(decoded_bitmap);
            return -1;
        }

        sdsfree(subval);

        iter->subkey_idx = i + 1;
        iter->offset += hot_subkey_size;
    }

    decrRefCount(decoded_bitmap);
    return 0;
}

int checkBeforeSaveDecoded(rdbKeySaveData *save, decodedData *decoded)
{
    robj *key = save->key;
    serverAssert(!sdscmp(decoded->key, key->ptr));

    if (decoded->rdbtype != RDB_TYPE_STRING) {
        /* check failed, skip this key */
        return -1;
    }

    bitmapMeta *bitmap_meta = objectMetaGetPtr(save->object_meta);

    bitmapSaveIterator *iter = save->iter;
    serverAssert(iter != NULL);
    if (iter->subkey_idx == BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE)) {
        /* bitmap has been totaly saved. */
        return -1;
    }

    /* save hot subkeys in prior to current saving subkey. */
    int idx = bitmapDecodeSubkeyIdx(decoded->subkey, sdslen(decoded->subkey));

    if (save->value != NULL) {
        serverAssert(bitmap_meta);
        if (bitmapMetaGetSubkeyStatus(bitmap_meta, idx, idx)) {
            /* hot subkey exist both redis and rocksDb, skip this subkey, it will be saved in the next cold subkey process. */
            return -1;
        }
    }

    return 0;
}

/* only called in cold or warm data. */
int bitmapSave(rdbKeySaveData *save, rio *rdb, decodedData *decoded) {
    if (checkBeforeSaveDecoded(save, decoded) == -1) {
        return 0;
    }

    int idx = bitmapDecodeSubkeyIdx(decoded->subkey, sdslen(decoded->subkey));

    bitmapSaveHotSubkeysUntill(save, rdb, idx, save->rdbtype);

    if (save->rdbtype == RDB_TYPE_STRING) {
        rio sdsrdb;
        rioInitWithBuffer(&sdsrdb,decoded->rdbraw);

        robj *subvalobj = rdbLoadObject(RDB_TYPE_STRING,&sdsrdb,NULL,NULL,0);
        serverAssert(subvalobj->type == OBJ_STRING);

        subvalobj = unshareStringValue(subvalobj);
        /* steal subvalobj sds */
        sds subval = subvalobj->ptr;
        int retval = rdbWriteRaw(rdb,subval,sdslen(subval));
        decrRefCount(subvalobj);

        if (retval == -1) {
            return -1;
        }

    } else {
        if (rdbWriteRaw(rdb, decoded->rdbraw, sdslen(decoded->rdbraw)) == -1) {
            return -1;
        }
    }

    bitmapSaveIterator *iter = save->iter;
    serverAssert(iter != NULL);
    iter->subkey_idx = idx + 1;

    save->saved++;
    return 0;
}

int bitmapSaveEnd(rdbKeySaveData *save, rio *rdb, int save_result) {
    bitmapMeta *meta = objectMetaGetPtr(save->object_meta);
    int expected = meta->pure_cold_subkeys_num + server.swap_debug_bgsave_metalen_addition;

    if (save_result != -1) {
        /* save tail hot subkeys */
        bitmapSaveHotSubkeysUntill(save, rdb, BITMAP_GET_SUBKEYS_NUM(meta->size, BITMAP_SUBKEY_SIZE), save->rdbtype);
    }

    bitmapSaveIterator *iter = save->iter;
    serverAssert(iter->subkey_idx == BITMAP_GET_SUBKEYS_NUM(meta->size, BITMAP_SUBKEY_SIZE));

    if (save->value) {
        serverAssert(iter->offset == stringObjectLen(save->value));
    }

    if (save->saved != expected) {
        sds key  = save->key->ptr;
        sds repr = sdscatrepr(sdsempty(), key, sdslen(key));
        serverLog(LL_WARNING,
                  "bitmapSave %s: saved(%d) != bitmapMeta.pure_cold_subkeys_num(%d)",
                  repr, save->saved, expected);
        sdsfree(repr);
        return SAVE_ERR_META_LEN_MISMATCH;
    }

    return save_result;
}

void bitmapSaveDeinit(rdbKeySaveData *save) {
    if (save->iter) {
        bitmapTypeReleaseSaveIter(save->iter);
        save->iter = NULL;
    }
}

/* save bitmap object as RDB_TYPE_BITMAP or RDB_TYPE_STRING. */
rdbKeySaveType bitmapSaveType = {
        .save_start = bitmapSaveStart,
        .save_hot_ext = bitmapSaveHotExtension,
        .save = bitmapSave,
        .save_end = bitmapSaveEnd,
        .save_deinit = bitmapSaveDeinit,
};

int bitmapSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen) {
    int retval = 0;
    save->type = &bitmapSaveType;
    if (server.swap_rdb_bitmap_encode_enabled) {
        save->rdbtype = RDB_TYPE_BITMAP;
    } else {
        save->rdbtype = RDB_TYPE_STRING;
    }
    save->omtype = &bitmapObjectMetaType;
    if (extend) { /* cold */
        serverAssert(save->object_meta == NULL && save->value == NULL);
        save->iter = bitmapTypeCreateSaveIter();
        retval = buildObjectMeta(SWAP_TYPE_BITMAP,version,extend,extlen,&save->object_meta);
    } else if (!keyIsHot(save->object_meta, save->value)){ /* warm */
        save->iter = bitmapTypeCreateSaveIter();
    }
    return retval;
}

/* bitmap subkey size may differ for different config, we need transfer subkey with size A to size B, thus intermediate state is recorded in bitmapLoadInfo. */
typedef struct bitmapLoadInfo
{
    sds consuming_old_subval; /* subval read , part of it has not been comsumed to transfer to subval with new size. */
    size_t consuming_offset;  /* offset description for consuming_old_subval, data behind offset is not yet consumed. */
    int num_old_subvals_waiting_load; /* num of old subvals waiting be load in rio. */
    size_t bitmap_size;
} bitmapLoadInfo;

void bitmapLoadStart(struct rdbKeyLoadData *load, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {

    int isencode;
    unsigned long long bitmap_size, old_subkey_size;
    sds header, extend = NULL;

    header = rdbVerbatimNew((unsigned char)load->rdbtype);

    /* bitmap_size */
    if (rdbLoadLenVerbatim(rdb, &header, &isencode, &bitmap_size)) {
        sdsfree(header);
        *error = RDB_LOAD_ERR_OTHER;
        return;
    }

    /* subkey_size */
    if (rdbLoadLenVerbatim(rdb, &header, &isencode, &old_subkey_size)) {
        sdsfree(header);
        *error = RDB_LOAD_ERR_OTHER;
        return;
    }

    size_t new_subkey_size = BITMAP_SUBKEY_SIZE;

    load->total_fields = BITMAP_GET_SUBKEYS_NUM(bitmap_size, new_subkey_size);

    bitmapLoadInfo *load_info = load->load_info;

    load_info->num_old_subvals_waiting_load = BITMAP_GET_SUBKEYS_NUM(bitmap_size, old_subkey_size);
    load_info->bitmap_size = bitmap_size;

    extend = encodeBitmapSize(bitmap_size);

    *cf = META_CF;
    *rawkey = rocksEncodeMetaKey(load->db,load->key);
    *rawval = rocksEncodeMetaVal(load->swap_type,load->expire,load->version,extend);
    *error = 0;

    sdsfree(extend);
    sdsfree(header);
}

int bitmapLoad(struct rdbKeyLoadData *load, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    
    bitmapLoadInfo *load_info = load->load_info;

    size_t subkey_size = BITMAP_GET_SPECIFIED_SUBKEY_SIZE(load_info->bitmap_size, BITMAP_SUBKEY_SIZE, load->loaded_fields);
    serverAssert(subkey_size >= 0);
    *error = RDB_LOAD_ERR_OTHER;

    size_t left_len_consuming_subval = 0;

    robj *old_subval_obj = NULL;
    if (load_info->consuming_old_subval == NULL) {
        serverAssert(load_info->consuming_offset == 0 && load_info->num_old_subvals_waiting_load != 0);

        if((old_subval_obj = rdbLoadStringObject(rdb)) == NULL) { 
            return 0;
        }

        load_info->consuming_old_subval = old_subval_obj->ptr;
        load_info->num_old_subvals_waiting_load--;

        old_subval_obj->ptr = NULL;
        decrRefCount(old_subval_obj);
    }

    left_len_consuming_subval = sdslen(load_info->consuming_old_subval) - load_info->consuming_offset;

    while (left_len_consuming_subval < subkey_size) {
        serverAssert(load_info->num_old_subvals_waiting_load != 0);

        if((old_subval_obj = rdbLoadStringObject(rdb)) == NULL) { 
            return 0;
        }

        size_t old_subval_len = stringObjectLen(old_subval_obj);

        load_info->consuming_old_subval = sdscatlen(load_info->consuming_old_subval, old_subval_obj->ptr, old_subval_len);
        load_info->num_old_subvals_waiting_load--;

        left_len_consuming_subval += old_subval_len;

        decrRefCount(old_subval_obj);
    }


    sds new_subval_raw = sdsnewlen(load_info->consuming_old_subval + load_info->consuming_offset, subkey_size);

    load_info->consuming_offset += subkey_size;

    serverAssert(load_info->consuming_offset <= sdslen(load_info->consuming_old_subval));

    if (load_info->consuming_offset == sdslen(load_info->consuming_old_subval)) {
        /* consuming_old_subval fully consumed. */
        sdsfree(load_info->consuming_old_subval);
        load_info->consuming_old_subval = NULL;
        load_info->consuming_offset = 0;
    }

    /* consuming_old_subval not fully consumed, resize to avoid occupying too much memory. */
    if (load_info->consuming_offset > BITMAP_SUBKEY_SIZE) {
        sds previous_str = load_info->consuming_old_subval;
        load_info->consuming_old_subval = sdsnewlen(previous_str + load_info->consuming_offset, sdslen(previous_str) - load_info->consuming_offset);
        load_info->consuming_offset = 0;

        sdsfree(previous_str);
    }

    /* new subval finished. */
    int subkey_idx = load->loaded_fields;
    sds subkey_str = bitmapEncodeSubkeyIdx(subkey_idx);

    robj subval_obj;
    initStaticStringObject(subval_obj, new_subval_raw);

    *cf = DATA_CF;
    *rawkey = bitmapEncodeSubkey(load->db, load->key, load->version, subkey_str);
    *rawval = bitmapEncodeSubval(&subval_obj);
    *error = 0;

    sdsfree(subkey_str);
    sdsfree(new_subval_raw);
    load->loaded_fields++;

    return load->loaded_fields < load->total_fields;
}

void bitmapLoadDeinit(struct rdbKeyLoadData *load) {

    if (load->value) {
        decrRefCount(load->value);
        load->value = NULL;
    }
    if (load->load_info) {
        bitmapLoadInfo *load_info = load->load_info;
        zfree(load_info);
        load->load_info = NULL;
    }
}

/* load for RDB_TYPE_BITMAP */
rdbKeyLoadType bitmapLoadType = {
    .load_start = bitmapLoadStart,
    .load = bitmapLoad,
    .load_end = NULL,
    .load_deinit = bitmapLoadDeinit,
};


void bitmapLoadInit(rdbKeyLoadData *load) {
    load->type = &bitmapLoadType;
    load->omtype = &bitmapObjectMetaType;
    load->swap_type = SWAP_TYPE_BITMAP;
    load->load_info = zmalloc(sizeof(bitmapLoadInfo));
    memset(load->load_info, 0, sizeof(bitmapLoadInfo));
}

sds genSwapBitmapStringSwitchedInfoString(sds info)
{
    info = sdscatprintf(info,
            "bitmap_string_switched_count:string_to_bitmap_count=%llu, bitmap_to_string_count=%llu\r\n",server.swap_string_switched_to_bitmap_count,server.swap_bitmap_switched_to_string_count);
    return info;
}

#ifdef REDIS_TEST
#define SWAP_EVICT_STEP 13
#define SWAP_EVICT_MEM  (BITMAP_SUBKEY_SIZE * 10)

#define INIT_SAVE_SKIP -2
void initServerConfig(void);

void bitmapDataCtxReset(bitmapDataCtx *datactx)
{
    zfree(datactx->subkeys_logic_idx);
    datactx->newbitmap = NULL;
    memset(datactx, 0, sizeof(bitmapDataCtx));
}

int swapDataBitmapTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);
    initServerConfig();
    ACLInit();
    server.hz = 10;
    initTestRedisServer();

    redisDb* db = server.db + 0;
    int error = 0;
    robj *purehot_key1, *purehot_bitmap1, *purehot_key2, *purehot_bitmap2, *purehot_key3, *purehot_bitmap3, \
         *purehot_key4, *purehot_bitmap4, *hot_key1, *hot_bitmap1, *cold_key1, *cold_key2, *cold_key3, *warm_key1, *warm_bitmap1, \
         *warm_key2, *warm_bitmap2, *warm_key3, *warm_bitmap3;
    keyRequest _keyReq, *purehot_keyReq1 = &_keyReq, _keyReq2, *purehot_keyReq2 = &_keyReq2, \ 
          _keyReq3, *purehot_keyReq3 = &_keyReq3, _keyReq4, *purehot_keyReq4 = &_keyReq4, \
          _hot_keyReq, *hot_keyReq1 = &_hot_keyReq, _cold_keyReq, *cold_keyReq1 = &_cold_keyReq, \
          _cold_keyReq2, *cold_keyReq2 = &_cold_keyReq2, _cold_keyReq3, *cold_keyReq3 = &_cold_keyReq3, \
          _warm_keyReq, *warm_keyReq1 = &_warm_keyReq, \
          _warm_keyReq2, *warm_keyReq2 = &_warm_keyReq2, _warm_keyReq3, *warm_keyReq3 = &_warm_keyReq3;
    swapData *purehot_data1, *purehot_data2, *purehot_data3, *purehot_data4, *hot_data1, *cold_data1, \
          *warm_data1, *cold_data2, *cold_data3, *warm_data2, *warm_data3;
    objectMeta *purehot_meta1, *purehot_meta2, *purehot_meta3, *purehot_meta4, *hot_meta1, *cold_meta1, \
          *warm_meta1, *cold_meta2, *cold_meta3, *warm_meta2, *warm_meta3;
    bitmapDataCtx *purehot_ctx1, *purehot_ctx2, *purehot_ctx3, *purehot_ctx4, *hot_ctx1, *cold_ctx1, \ 
          *warm_ctx1, *cold_ctx2, *cold_ctx3, *warm_ctx2, *warm_ctx3 = NULL;
    int intention;
    uint32_t intention_flags;
    int action, numkeys, *cfs;
    sds *rawkeys, *rawvals;

    int originEvictStepMaxSubkey = server.swap_evict_step_max_subkeys;
    int originEvictStepMaxMemory = server.swap_evict_step_max_memory;

    long long NOW = 1661657836000;

    TEST("bitmap - swap test init") {

        server.swap_evict_step_max_subkeys = SWAP_EVICT_STEP;
        server.swap_evict_step_max_memory = SWAP_EVICT_MEM;

        purehot_key1 = createStringObject("purehot_key1",12);
        purehot_meta1 = createBitmapObjectMeta(0, NULL);

        /* 2 subkeys in pure hot bitmap*/
        sds str = sdsnewlen("", BITMAP_SUBKEY_SIZE * 2);
        purehot_bitmap1 = createStringObject(str, sdslen(str));
        dbAdd(db,purehot_key1,purehot_bitmap1);

        sdsfree(str);
        str = NULL;

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

        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 2, STATUS_HOT);
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

        size_t size = 3 * BITMAP_SUBKEY_SIZE;
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
        swapDataSetupMeta(purehot_data1,SWAP_TYPE_BITMAP, -1,(void**)&purehot_ctx1);
        swapDataSetObjectMeta(purehot_data1, purehot_meta1);

        hot_data1 = createSwapData(db, hot_key1, hot_bitmap1, NULL);
        swapDataSetupMeta(hot_data1, SWAP_TYPE_BITMAP, -1, (void**)&hot_ctx1);
        swapDataSetObjectMeta(hot_data1, hot_meta1);
        
        cold_data1 = createSwapData(db, cold_key1, NULL, NULL);
        swapDataSetupMeta(cold_data1, SWAP_TYPE_BITMAP, -1, (void**)&cold_ctx1);
        swapDataSetObjectMeta(cold_data1, cold_meta1);

        /* warm bitmap with hole */
        /* 0,1,2,3 cold, 2,3 hot */
        warm_key3 =  createStringObject("warm_key3",9);
        warm_bitmap3 = createStringObject(NULL, BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2);

        bitmapMeta *meta = bitmapMetaCreate();
        meta->size = BITMAP_SUBKEY_SIZE * 3 + BITMAP_SUBKEY_SIZE / 2;
        meta->pure_cold_subkeys_num = 2;
        bitmapMetaSetSubkeyStatus(meta, 3, 3, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(meta, 2, 2, STATUS_HOT);

        warm_meta3 = createBitmapObjectMeta(0, meta);

        warm_data3 = createSwapData(db, warm_key3, NULL, NULL);
        swapDataSetupMeta(warm_data3, SWAP_TYPE_BITMAP, -1, (void**)&warm_ctx3);
        swapDataSetObjectMeta(warm_data3, warm_meta3);

        incrRefCount(warm_key3);
        warm_keyReq3->key = warm_key3;
        warm_keyReq3->type = KEYREQUEST_TYPE_SUBKEY;
        warm_keyReq3->level = REQUEST_LEVEL_KEY;
        warm_keyReq3->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        warm_keyReq3->l.num_ranges = 0;
        warm_keyReq3->l.ranges = NULL;

        sdsfree(str1);
        str1 = NULL;

        sdsfree(coldBitmapSize);
        coldBitmapSize = NULL;

        decrRefCount(cold_key1);
        decrRefCount(cold_key1);
    }

    TEST("bitmap - meta api test create free dup encode decode equal ishot") {
        objectMeta *object_meta1;
        bitmapMeta *bitmap_meta1;
        bitmap_meta1 = bitmapMetaCreate();

        bitmap_meta1->size = BITMAP_SUBKEY_SIZE + 1;
        bitmap_meta1->pure_cold_subkeys_num = 1;
        bitmapMetaSetSubkeyStatus(bitmap_meta1, 0, 0, STATUS_HOT);

        object_meta1 = createBitmapObjectMeta(0, bitmap_meta1);
        sds meta_buf1 = bitmapObjectMetaEncode(object_meta1, NULL);

        objectMeta *object_meta2 = createBitmapObjectMeta(0, NULL);
        test_assert(0 == bitmapObjectMetaDecode(object_meta2, meta_buf1, sdslen(meta_buf1)));
        test_assert(0 == bitmapObjectMetaEqual(object_meta1, object_meta2));
        test_assert(0 == bitmapObjectMetaIsHot(object_meta2, NULL));

        bitmapMetaSetSubkeyStatus(bitmap_meta1, 1, 1, STATUS_HOT);
        bitmap_meta1->pure_cold_subkeys_num = 0;

        objectMeta *object_meta3 = createBitmapObjectMeta(0, NULL);
        bitmapObjectMetaDup(object_meta3, object_meta1);
        test_assert(1 == bitmapObjectMetaEqual(object_meta1, object_meta3));
        test_assert(1 == bitmapObjectMetaIsHot(object_meta3, NULL));

        bitmapMeta *bitmap_meta3 = objectMetaGetPtr(object_meta3);
        test_assert(2 == bitmapMetaGetHotSubkeysNum(bitmap_meta3, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta3->size, BITMAP_SUBKEY_SIZE) - 1));

        freeObjectMeta(object_meta1);
        freeObjectMeta(object_meta2);
        freeObjectMeta(object_meta3);

        sdsfree(meta_buf1);
        meta_buf1 = NULL;
    }

    TEST("bitmap - meta api test rebuildFeed metaBitmapGrow marker") {
    
        bitmapMeta *bitmap_meta1;
        bitmap_meta1 = bitmapMetaCreate();

        objectMeta *object_meta1;
        object_meta1 = createBitmapObjectMeta(0, bitmap_meta1);

        sds subval = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        robj subval_obj;
        initStaticStringObject(subval_obj, subval);

        sds subkey = sdsnewlen(NULL, sizeof(long));

        for (int i = 0; i < 4; i++) {
            bitmapObjectMetaRebuildFeed(object_meta1,0,subkey,sdslen(subkey),&subval_obj);
        }
    
        sdsfree(subval);
        sdsfree(subkey);

        test_assert(bitmap_meta1->pure_cold_subkeys_num == 4);
        test_assert(bitmap_meta1->size == BITMAP_SUBKEY_SIZE*4);

        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 4);
        robj *bitmap1 = createStringObject(str, sdslen(str));

        for (int i = 0; i < 4; i++) {
            bitmapMetaSetSubkeyStatus(bitmap_meta1, i, i, STATUS_HOT);
        }

        metaBitmap meta_bitmap;
        metaBitmapInit(&meta_bitmap, bitmap_meta1, bitmap1);
        metaBitmapGrow(&meta_bitmap, BITMAP_SUBKEY_SIZE*6);

        test_assert(bitmap_meta1->pure_cold_subkeys_num == 4);
        test_assert(stringObjectLen(bitmap1) == BITMAP_SUBKEY_SIZE*6);
        test_assert(bitmap_meta1->size == BITMAP_SUBKEY_SIZE*6);
        test_assert(6 == bitmapMetaGetHotSubkeysNum(bitmap_meta1, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta1->size, BITMAP_SUBKEY_SIZE) - 1));

        sdsfree(str);
        decrRefCount(bitmap1);
        freeObjectMeta(object_meta1);

        robj *tmp_key = createStringObject("tmp_key", 7);
        robj *tmp_value = createStringObject("tmp_value", 9);

        dbAdd(db, tmp_key, tmp_value);

        bitmapSetObjectMarkerIfNeeded(db, tmp_key);
        objectMeta *object_meta = lookupMeta(db,tmp_key);
        test_assert(object_meta->swap_type == SWAP_TYPE_BITMAP);

        bitmapClearObjectMarkerIfNeeded(db, tmp_key);

        test_assert(lookupMeta(db,tmp_key) == NULL);

        dbDelete(db, tmp_key);
        decrRefCount(tmp_key);
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
        test_assert((BITMAP_GET_SUBKEYS_NUM(((bitmapMeta *)objectMetaGetPtr(cold_meta1))->size, BITMAP_SUBKEY_SIZE)) == 3);
        test_assert(purehot_ctx1->ctx_flag == 0);
        test_assert(purehot_ctx1->subkeys_num == 0);

        range *range1 = zmalloc(sizeof(range));

        range1->start = BITMAP_SUBKEY_SIZE * 2;            /* out of range, read operation */
        range1->end = BITMAP_SUBKEY_SIZE * 3 + BITMAP_SUBKEY_SIZE / 2 - 1;
        range1->type = RANGE_BYTE_BITMAP;
        warm_keyReq3->l.num_ranges = 1;
        warm_keyReq3->l.ranges = range1;

        warm_keyReq3->cmd_intention = SWAP_IN, warm_keyReq3->cmd_intention_flags = 0;
        swapDataAna(warm_data3,0,warm_keyReq3,&intention,&intention_flags,warm_ctx3);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        test_assert(warm_ctx3->subkeys_num == 0);

        /* in: for persist data, l.num_ranges != 0 */

        range1->start = BITS_NUM_IN_SUBKEY * 4;            /* out of range, read operation */
        range1->end = BITS_NUM_IN_SUBKEY * 4;
        range1->type = RANGE_BIT_BITMAP;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_GET);
        test_assert(cold_ctx1->subkeys_num == 1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY * 4;            /* out of range, read operation */
        range1->end = BITS_NUM_IN_SUBKEY * 4;
        range1->type = RANGE_BIT_BITMAP;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        bitmapSwapAnaAction(cold_data1, intention, cold_ctx1, &action);
        test_assert(action == ROCKS_GET);
        test_assert(cold_ctx1->subkeys_num == 1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY * 4 + 1;                /* out of range, write operation */
        range1->end = BITS_NUM_IN_SUBKEY * 4 + 1;
        range1->type = RANGE_BIT_BITMAP;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITS_NUM_IN_SUBKEY * 5 - 1;                /* out of range, write operation */
        range1->end = BITS_NUM_IN_SUBKEY * 5 - 1;
        range1->type = RANGE_BIT_BITMAP;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = 0;                /* swap in all */
        range1->end = BITMAP_SUBKEY_SIZE * 3 - 1;
        range1->type = RANGE_BYTE_BITMAP;

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
        range1->type = RANGE_BYTE_BITMAP;

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
        range1->type = RANGE_BIT_BITMAP;

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
        range1->type = RANGE_BYTE_BITMAP;

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
        range1->type = RANGE_BYTE_BITMAP;

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
        range1->end = UINT_MAX;
        range1->type = RANGE_BYTE_BITMAP;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;             /* bitpos */
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == -1);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE * 2;            /* range across boundry.  bitcount. */
        range1->end = BITMAP_SUBKEY_SIZE * 5 - 1;
        range1->type = RANGE_BYTE_BITMAP;

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
        range1->type = RANGE_BYTE_BITMAP;

        cold_keyReq1->l.num_ranges = 1;
        cold_keyReq1->l.ranges = range1;
        cold_keyReq1->cmd_intention = SWAP_IN, cold_keyReq1->cmd_intention_flags = 0;
        swapDataAna(cold_data1,0,cold_keyReq1,&intention,&intention_flags,cold_ctx1);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold_ctx1->subkeys_num == 1);
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 2);
        bitmapDataCtxReset(cold_ctx1);

        range1->start = BITMAP_SUBKEY_SIZE;            /* range across boundry.  bitpos. */
        range1->end = UINT_MAX;
        range1->type = RANGE_BYTE_BITMAP;

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
        test_assert(cold_ctx1->ctx_flag & BIG_DATA_CTX_FLAG_MOCK_VALUE);
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
        test_assert(cold_ctx1->subkeys_logic_idx[0] == 0);
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
        test_assert(BITMAP_GET_SUBKEYS_NUM(((bitmapMeta *)objectMetaGetPtr(cold_meta1))->size, BITMAP_SUBKEY_SIZE) == 3);
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
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta1, 0, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta1, 3, 4, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta1, 7, 7, STATUS_HOT);

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
        swapDataSetupMeta(warm_data1,SWAP_TYPE_BITMAP, -1,(void**)&warm_ctx1);
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
        test_assert(deltaBm->subkeys_num == 5);

        test_assert(deltaBm->subkeys_logic_idx[2] == 3);
        test_assert(deltaBm->subkeys_logic_idx[3] == 4);
        test_assert(deltaBm->subkeys_logic_idx[4] == 7);

        test_assert(deltaBm->subkeys_logic_idx[0] == 0);
        test_assert(deltaBm->subkeys_logic_idx[1] == 1);

        test_assert(sdslen(deltaBm->subvals[0]) == BITMAP_SUBKEY_SIZE);
        test_assert(sdslen(deltaBm->subvals[1]) == BITMAP_SUBKEY_SIZE);
        test_assert(sdslen(deltaBm->subvals[2]) == BITMAP_SUBKEY_SIZE);
        test_assert(sdslen(deltaBm->subvals[3]) == BITMAP_SUBKEY_SIZE);
        test_assert(sdslen(deltaBm->subvals[4]) == BITMAP_SUBKEY_SIZE / 2);


        test_assert(deltaBm->subkeys_logic_idx[0] == 0);
        test_assert(deltaBm->subkeys_logic_idx[4] == 7);

        bitmapDataCtxReset(warm_ctx1);
        deltaBitmapFree(deltaBm);
        sdsfree(str);
        str = NULL;

        dbDelete(db,warm_key1);
    }

    TEST("bitmap - swapIn for cold data") { /* cold to warm */
        cold_key2 = createStringObject("cold_key2",9);
        cold_meta2 = createBitmapObjectMeta(0, NULL);

        /* subkeys 0 ~ 7 in rocksDb, swap in {0, 1, 3, 4, 7} */
        size_t size = 7 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;
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
        swapDataSetupMeta(cold_data2, SWAP_TYPE_BITMAP, -1, (void**)&cold_ctx2);
        swapDataSetObjectMeta(cold_data2, cold_meta2);

        deltaBitmap *deltaBm = deltaBitmapCreate(5);
        deltaBm->subkeys_num = 5;

        deltaBm->subvals[0] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subvals[1] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subvals[2] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subvals[3] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subvals[4] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        deltaBm->subkeys_logic_idx[0] = 0;
        deltaBm->subkeys_logic_idx[1] = 1;
        deltaBm->subkeys_logic_idx[2] = 3;
        deltaBm->subkeys_logic_idx[3] = 4;
        deltaBm->subkeys_logic_idx[4] = 7;

        deltaBm->subvals_total_len = BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2;

        robj *result = bitmapCreateOrMergeObject(cold_data2, deltaBm, cold_ctx2);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(cold_data2);

        test_assert(bitmap_meta->pure_cold_subkeys_num == 3);
        test_assert(bitmap_meta->size == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE) - 1) == 5);
        test_assert(stringObjectLen(result) == BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2);

        test_assert(bitmapMergedIsHot(cold_data2, result, cold_ctx2) == 0);

        bitmapSwapIn(cold_data2, result, cold_ctx2);

        robj *bm;
        test_assert((bm = lookupKey(db,cold_key2,LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 4 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(bm->persistent);

        sdsfree(coldBitmapSize);
        coldBitmapSize = NULL;
    }

    TEST("bitmap - swapIn for warm data") { /* warm to hot */
        warm_key2 = createStringObject("warm_key2",13);
        warm_meta2 = createBitmapObjectMeta(0, NULL);

        bitmapMeta *warm_bitmap_meta2 = bitmapMetaCreate();

        /* subkeys sum = 8, part {0, 1, 3, 4, 6} both in redis and rocks, {2, 5, 7} will swap in  */
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta2, 0, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta2, 3, 4, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta2, 6, 6, STATUS_HOT);

        warm_bitmap_meta2->pure_cold_subkeys_num = 3;

        /* size of subkey 7 is half of BITMAP_SUBKEY_SIZE*/
        warm_bitmap_meta2->size = 7 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;
        objectMetaSetPtr(warm_meta2, warm_bitmap_meta2);

        /* 5 subkeys are hot */
        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 4);
        sds str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);  /* subkey 6 is not all zero bits. */
        str1[0] = '1';

        str = sdscatsds(str, str1);
        warm_bitmap2 = createStringObject(str, sdslen(str));

        sds str2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 4);
        sds str3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);  /* subkey 6 is not all zero bits. */
        str3[0] = '1';
        str2 = sdscatsds(str2, str3);

        test_assert(0 == memcmp((char*)warm_bitmap2->ptr, str2, BITMAP_SUBKEY_SIZE * 5));
        test_assert(0 == memcmp((char*)warm_bitmap2->ptr + BITMAP_SUBKEY_SIZE * 4, "1", 1));

        dbAdd(db,warm_key2,warm_bitmap2);

        incrRefCount(warm_key2);
        warm_keyReq2->key = warm_key2;
        warm_keyReq2->type = KEYREQUEST_TYPE_SUBKEY;
        warm_keyReq2->level = REQUEST_LEVEL_KEY;
        warm_keyReq2->cmd_flags = CMD_SWAP_DATATYPE_BITMAP;
        warm_keyReq2->l.num_ranges = 0;
        warm_keyReq2->l.ranges = NULL;

        warm_data2 = createSwapData(db, warm_key2, warm_bitmap2, NULL);
        swapDataSetupMeta(warm_data2,SWAP_TYPE_BITMAP, -1,(void**)&warm_ctx2);
        swapDataSetObjectMeta(warm_data2, warm_meta2);

        deltaBitmap *deltaBm = deltaBitmapCreate(3);

        deltaBm->subkeys_num = 3;

        deltaBm->subvals[0] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subvals[1] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        deltaBm->subvals[2] = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        deltaBm->subkeys_logic_idx[0] = 2;
        deltaBm->subkeys_logic_idx[1] = 5;
        deltaBm->subkeys_logic_idx[2] = 7;

        deltaBm->subvals_total_len = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        robj *result = bitmapCreateOrMergeObject(warm_data2, deltaBm, warm_ctx2);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(warm_data2);

        test_assert(bitmap_meta->pure_cold_subkeys_num == 0);
        test_assert(bitmap_meta->size == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE) - 1) == 8);
        test_assert(stringObjectLen(warm_data2->value) == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);

        test_assert(bitmapMergedIsHot(warm_data2, result, warm_ctx2) == 1);

        bitmapSwapIn(warm_data2, result, warm_ctx2);
        robj *bm;
        test_assert((bm = lookupKey(db,warm_key2,LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2);

        sds str5 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 6);
        sds str6 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);  /* subkey 6 is not all zero bits. */
        str6[0] = '1';
        sds str7 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        str5 = sdscatsds(str5, str6);
        str5 = sdscatsds(str5, str7);

        int tmp_num = 0;
        test_assert(0 == memcmp((char *)bm->ptr + BITMAP_SUBKEY_SIZE * 6 + 1, &tmp_num, 1));
        test_assert(0 == memcmp((char *)bm->ptr, str5, BITMAP_SUBKEY_SIZE * 7 + BITMAP_SUBKEY_SIZE / 2));
        test_assert(0 == memcmp((char *)bm->ptr + BITMAP_SUBKEY_SIZE * 6, "1", 1));

        test_assert(bm->persistent);

        sdsfree(str);
        sdsfree(str1);
        sdsfree(str2);
        sdsfree(str3);
        sdsfree(str5);
        sdsfree(str6);
        sdsfree(str7);

        str = NULL;
        str1 = NULL;
        str2 = NULL;
        str3 = NULL;
        str5 = NULL;
        str6 = NULL;
        str7 = NULL;

        dbDelete(db,warm_key2);
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
        swapDataSetupMeta(purehot_data2,SWAP_TYPE_BITMAP, -1,(void**)&purehot_ctx2);
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
        test_assert(stringObjectLen(purehot_ctx2->newbitmap) == 0);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(purehot_data2);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE) - 1) == 0);
        test_assert(bitmap_meta->pure_cold_subkeys_num == 2);

        int totally_out;
        bitmapSwapOut(purehot_data2, purehot_ctx2, 0, &totally_out);

        test_assert((bm = lookupKey(db, purehot_key2, LOOKUP_NOTOUCH)) == NULL);
        test_assert(totally_out == 1);

        sdsfree(str);
        str = NULL;

        dbDelete(db, purehot_key2);
    }

    TEST("bitmap - swap out for pure hot data2") { /* pure hot to hot */
        purehot_key3 = createStringObject("purehot_key3",12);
        purehot_meta3 = createBitmapObjectMeta(0, NULL);

        /* 13 subkeys in pure hot bitmap */
        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 10);
        sds str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        str1[0] = '1';
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
        swapDataSetupMeta(purehot_data3,SWAP_TYPE_BITMAP, -1,(void**)&purehot_ctx3);
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

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(purehot_data3);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE) - 1) == 13);
        test_assert(bitmap_meta->pure_cold_subkeys_num == 0);

        int totally_out;
        bitmapSwapOut(purehot_data3, purehot_ctx3, 1, &totally_out);

        test_assert((bm = lookupKey(db, purehot_key3, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 12 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(totally_out == 0);

        sdsfree(str);
        sdsfree(str1);

        str = NULL;
        str1 = NULL;

        dbDelete(db, purehot_key3);

    }

    TEST("bitmap - swap out for pure hot data3") { /* pure hot to warm */
        purehot_key4 = createStringObject("purehot_key4",12);
        purehot_meta4 = createBitmapObjectMeta(0, NULL);

        /* 13 subkeys in pure hot bitmap */
        sds str = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 10);
        sds str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        str1[0] = '1';
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
        swapDataSetupMeta(purehot_data4,SWAP_TYPE_BITMAP, -1,(void**)&purehot_ctx4);
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
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 12 + BITMAP_SUBKEY_SIZE / 2);

        bitmapMeta *bitmap_meta = swapDataGetBitmapMeta(purehot_data4);
        test_assert(rbmGetBitRange(bitmap_meta->subkeys_status, 0, BITMAP_GET_SUBKEYS_NUM(bitmap_meta->size, BITMAP_SUBKEY_SIZE) - 1) == 3);
        test_assert(bitmap_meta->pure_cold_subkeys_num == 10);

        int totally_out;
        bitmapSwapOut(purehot_data4, purehot_ctx4, 0, &totally_out);

        test_assert((bm = lookupKey(db, purehot_key4, LOOKUP_NOTOUCH)) != NULL);
        test_assert(stringObjectLen(bm) == BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        test_assert(totally_out == 0);

        sds str2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        str2[0] = '1';
        test_assert(0 == memcmp(bm->ptr, str2, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2));

        sdsfree(str);
        sdsfree(str1);
        sdsfree(str2);

        str = NULL;
        str1 = NULL;
        str2 = NULL;
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
        bitmapMetaSetSubkeyStatus(bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(bitmap_meta, 3, 3, STATUS_HOT);

        bitmap_meta->pure_cold_subkeys_num = 3;

        /* size of subkey 4 is half of BITMAP_SUBKEY_SIZE */
        bitmap_meta->size = 4 * BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *object_meta = createBitmapObjectMeta(0, bitmap_meta);

        bitmapDataCtx *datactx;
        swapData *data = createSwapData(db, key, bitmap, NULL);
        swapDataSetupMeta(data, SWAP_TYPE_BITMAP, -1, (void**)&datactx);
        swapDataSetObjectMeta(data, object_meta);

        incrRefCount(key);
        dbAdd(db,key,bitmap);
        dbAddMeta(db,key,object_meta);

        datactx->arg_reqs[0].mstate_idx = -1;
        datactx->arg_reqs[1].mstate_idx = -1;

        datactx->arg_reqs[0].arg_idx = 2;
        datactx->arg_reqs[1].arg_idx = -1;

        datactx->arg_reqs[0].arg_type = RANGE_BIT_BITMAP;
        datactx->arg_reqs[0].arg_type = RANGE_BIT_BITMAP;

        rewriteResetClientCommandCString(c, 3, "GETBIT", "mybitmap", "65636"); /* 32768 * 2 + 100 */
        bitmapBeforeCall(data, NULL, c, datactx);
        test_assert(!strcmp(c->argv[2]->ptr,"100"));
        clientArgRewritesRestore(c);
        test_assert(!strcmp(c->argv[2]->ptr,"65636"));

        datactx->arg_reqs[0].mstate_idx = -1;
        datactx->arg_reqs[1].mstate_idx = -1;
        datactx->arg_reqs[0].arg_idx = 2;
        datactx->arg_reqs[1].arg_idx = 3;

        datactx->arg_reqs[0].arg_type = RANGE_BYTE_BITMAP;
        datactx->arg_reqs[1].arg_type = RANGE_BYTE_BITMAP;

        dbDelete(db, key);
        swapDataFree(data, datactx);
        decrRefCount(key);
        sdsfree(str);

        str = NULL;
        // freeClient(c);
    }

    TEST("bitmap - swap test deinit") {
        for (int i = 0; i < numkeys; i++) {
            sdsfree(rawkeys[i]), sdsfree(rawvals[i]);
            rawkeys[i] = NULL;
            rawvals[i] = NULL;
        }
        zfree(cfs), zfree(rawkeys), zfree(rawvals);

        swapDataFree(purehot_data1, purehot_ctx1);
        swapDataFree(purehot_data2, purehot_ctx2);
        swapDataFree(purehot_data3, purehot_ctx3);
        swapDataFree(purehot_data4, purehot_ctx4);
        swapDataFree(hot_data1, hot_ctx1);
        swapDataFree(cold_data1, cold_ctx1);
        swapDataFree(cold_data2, cold_ctx2);
        swapDataFree(warm_data1, warm_ctx1);
        swapDataFree(warm_data2, warm_ctx2);
        swapDataFree(warm_data3, warm_ctx3);

        freeObjectMeta(purehot_meta1);
        freeObjectMeta(purehot_meta2);
        freeObjectMeta(purehot_meta3);
        freeObjectMeta(purehot_meta4);
        freeObjectMeta(hot_meta1);
        freeObjectMeta(cold_meta1);
        freeObjectMeta(cold_meta2);
        freeObjectMeta(warm_meta1);
        freeObjectMeta(warm_meta2);
        freeObjectMeta(warm_meta3);

        decrRefCount(purehot_bitmap1);
        /* other string object's refcount has become 0 during swap process. */
    }

    TEST("bitmap - save RDB_TYPE_STRING") {
        server.swap_rdb_bitmap_encode_enabled = 0;     

        sds f1, f2, f3, subval1, subval2, subval3, rdb_subval1, rdb_subval2, rdb_subval3, coldraw, warmraw, hotraw, warm_str1, hot_bitmap;

        robj *save_key1, *save_warm_bitmap1, *save_hot_bitmap0, *save_hot_bitmap1, *rdb_key1, *rdb_key2, *rdb_key3, *value1, *value2, *value3;

        f1 = bitmapEncodeSubkeyIdx(0), f2 = bitmapEncodeSubkeyIdx(1), f3 = bitmapEncodeSubkeyIdx(2);
        save_key1 = createStringObject("save_key1",9);

        subval1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        subval1[0] = '1';

        robj subval1_obj;
        initStaticStringObject(subval1_obj, subval1);

        subval2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);

        robj subval2_obj;
        initStaticStringObject(subval2_obj, subval2);

        subval3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        robj subval3_obj;
        initStaticStringObject(subval3_obj, subval3);

        rdb_subval1 = bitmapEncodeSubval(&subval1_obj);
        rdb_subval2 = bitmapEncodeSubval(&subval2_obj);
        rdb_subval3 = bitmapEncodeSubval(&subval3_obj);

        rio rdbcold, rdbwarm, rdbhot;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save cold, 3 subkeys */
        dbDelete(db, save_key1);
        rioInitWithBuffer(&rdbcold,sdsempty());

        test_assert(rdbKeySaveWarmColdInit(saveData, db, (decodedResult*)decoded_meta) == 0);

        test_assert(rdbKeySaveStart(saveData, &rdbcold) == 0);

        decoded_data->subkey = f1, decoded_data->rdbraw = sdsnewlen(rdb_subval1 + 1, sdslen(rdb_subval1) - 1);
        decoded_data->version = saveData->object_meta->version;
        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0 && saveData->saved == 1);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        decoded_data->subkey = f2, decoded_data->rdbraw = sdsnewlen(rdb_subval2 + 1, sdslen(rdb_subval2) - 1);
        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0 && saveData->saved == 2);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        decoded_data->subkey = f3, decoded_data->rdbraw = sdsnewlen(rdb_subval3 + 1, sdslen(rdb_subval3) - 1);
        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0 && saveData->saved == 3);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        test_assert(rdbKeySaveEnd(saveData,&rdbcold,0) == 0);

        rdbKeySaveDataDeinit(saveData);
        coldraw = rdbcold.io.buffer.ptr;

        /* rdbSave - save warm */

        dbDelete(db, save_key1);
        rioInitWithBuffer(&rdbwarm,sdsempty());

        /* 2 subkeys are hot */
        warm_str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2);
        warm_str1[0] = '1';
        save_warm_bitmap1 = createStringObject(warm_str1, sdslen(warm_str1));
        save_warm_bitmap1->type = OBJ_STRING;

        bitmapMeta *warm_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, part {0, 2} both in redis and rocks, {1} in rocksDb. */
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta, 0, 0, STATUS_HOT);

        warm_bitmap_meta->pure_cold_subkeys_num = 1;
        warm_bitmap_meta->size = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *warm_object_meta = createBitmapObjectMeta(0, warm_bitmap_meta);

        dbAdd(db, save_key1, save_warm_bitmap1);
        dbAddMeta(db, save_key1, warm_object_meta);

        test_assert(rdbKeySaveWarmColdInit(saveData, db, (decodedResult*)decoded_meta) == 0);
        test_assert(rdbKeySaveStart(saveData, &rdbwarm) == 0);

        decoded_data->version = saveData->object_meta->version;
        decoded_data->subkey = f2, decoded_data->rdbraw = sdsnewlen(rdb_subval2 + 1, sdslen(rdb_subval2) - 1);

        test_assert(rdbKeySave(saveData,&rdbwarm,decoded_data) == 0);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        test_assert(rdbKeySaveEnd(saveData,&rdbwarm,0) == 0);

        rdbKeySaveDataDeinit(saveData);
        warmraw = rdbwarm.io.buffer.ptr;

        /* rdbSave - save hot */
        dbDelete(db, save_key1);
        rioInitWithBuffer(&rdbhot,sdsempty());

        hot_bitmap = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        hot_bitmap[0] = '1';

        save_hot_bitmap0 = createStringObject(hot_bitmap, sdslen(hot_bitmap));

        save_hot_bitmap1 = createStringObject(hot_bitmap, sdslen(hot_bitmap));

        /* 3 subkeys are hot */
        save_hot_bitmap1->type = OBJ_STRING;

        bitmapMeta *hot_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, part {0,1,2} both in redis and rocks. */
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 1, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 0, STATUS_HOT);

        hot_bitmap_meta->pure_cold_subkeys_num = 0;
        hot_bitmap_meta->size = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *hot_object_meta = createBitmapObjectMeta(0, hot_bitmap_meta);

        dbAdd(db, save_key1, save_hot_bitmap1);
        dbAddMeta(db, save_key1, hot_object_meta);

        test_assert(rdbKeySaveHotExtensionInit(saveData, db, save_key1->ptr) == 0);
        test_assert(rdbKeySaveHotExtension(saveData, &rdbhot) == 0);
        rdbKeySaveDataDeinit(saveData);

        dbDelete(db, save_key1);
        hotraw = rdbhot.io.buffer.ptr;

        size_t coldrawlen = sdslen(coldraw);
        size_t warmrawlen = sdslen(warmraw);

        test_assert(coldrawlen == warmrawlen);
        test_assert(!sdscmp(warmraw,coldraw));

        /* load warm */

        rioInitWithBuffer(&rdbwarm,rdbwarm.io.buffer.ptr);
        /* LFU */
        uint8_t byte1;
        test_assert(rdbLoadType(&rdbwarm) == RDB_OPCODE_FREQ);
        test_assert(rioRead(&rdbwarm,&byte1,1));
        /* rdbtype */
        test_assert(rdbLoadType(&rdbwarm) == RDB_TYPE_STRING);
        /* key */
        rdb_key1 = rdbLoadStringObject(&rdbwarm);
        test_assert(equalStringObjects(rdb_key1, save_key1));

        int error1 = 0;
        value1 = rdbLoadObject(RDB_TYPE_STRING, &rdbwarm, rdb_key1->ptr, &error1, 0);
        test_assert(value1 != NULL);

        test_assert(equalStringObjects(value1, save_hot_bitmap0));

        /* load cold */
        
        rioInitWithBuffer(&rdbcold,rdbcold.io.buffer.ptr);
        /* LFU */
        uint8_t byte2;
        test_assert(rdbLoadType(&rdbcold) == RDB_OPCODE_FREQ);
        test_assert(rioRead(&rdbcold,&byte2,1));
        /* rdbtype */
        test_assert(rdbLoadType(&rdbcold) == RDB_TYPE_STRING);
        /* key */
        rdb_key2 = rdbLoadStringObject(&rdbcold);
        test_assert(equalStringObjects(rdb_key2, save_key1));

        int error2 = 0;
        value2 = rdbLoadObject(RDB_TYPE_STRING, &rdbcold, rdb_key2->ptr, &error2, 0);
        test_assert(value2 != NULL);

        test_assert(equalStringObjects(value2, save_hot_bitmap0));

        /* load hot */
        
        rioInitWithBuffer(&rdbhot,rdbhot.io.buffer.ptr);
        /* LFU */
        uint8_t byte3;
        test_assert(rdbLoadType(&rdbhot) == RDB_OPCODE_FREQ);
        test_assert(rioRead(&rdbhot,&byte3,1));
        /* rdbtype */
        test_assert(rdbLoadType(&rdbhot) == RDB_TYPE_STRING);
        /* key */
        rdb_key3 = rdbLoadStringObject(&rdbhot);
        test_assert(equalStringObjects(rdb_key3, save_key1));

        int error3 = 0;
        value3 = rdbLoadObject(RDB_TYPE_STRING, &rdbhot, rdb_key3->ptr, &error3, 0);
        test_assert(value3 != NULL);

        test_assert(equalStringObjects(value3, save_hot_bitmap0));

        sdsfree(f1),sdsfree(f2),sdsfree(f3),sdsfree(subval1),sdsfree(subval2),sdsfree(subval3),sdsfree(rdb_subval1),sdsfree(rdb_subval2),sdsfree(rdb_subval3),sdsfree(coldraw),sdsfree(warmraw),sdsfree(hotraw),sdsfree(warm_str1),sdsfree(hot_bitmap);

        f1 = NULL, f2 = NULL, f3 = NULL;
        subval1 = NULL,subval2 = NULL,subval3 = NULL;
        rdb_subval1 = NULL, rdb_subval2 = NULL,rdb_subval3 = NULL,
        coldraw = NULL, warmraw = NULL, warm_str1 = NULL, hotraw = NULL, hot_bitmap = NULL;

        decrRefCount(save_key1);
        decrRefCount(rdb_key1);
        decrRefCount(rdb_key2);
        decrRefCount(rdb_key3);
        decrRefCount(save_hot_bitmap0);

        decrRefCount(value1);
        decrRefCount(value2);
        decrRefCount(value3);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);

        server.swap_rdb_bitmap_encode_enabled = 1;  
    }

    TEST("bitmap - save & load cold RDB_TYPE_BITMAP") {
        
        sds f1, f2, f3, subval1, subval2, subval3, rdb_subval1, rdb_subval2, rdb_subval3, coldraw;

        robj *save_key1;

        uint64_t V = server.swap_key_version = 0; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);

        f1 = bitmapEncodeSubkeyIdx(0), f2 = bitmapEncodeSubkeyIdx(1), f3 = bitmapEncodeSubkeyIdx(2);

        sds raw_f1 = bitmapEncodeSubkey(db, save_key1->ptr, V, f1);
        sds raw_f2 = bitmapEncodeSubkey(db, save_key1->ptr, V, f2);
        sds raw_f3 = bitmapEncodeSubkey(db, save_key1->ptr, V, f3);

        subval1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        subval1[0] = '1';

        robj subval1_obj;
        initStaticStringObject(subval1_obj, subval1);

        subval2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);

        robj subval2_obj;
        initStaticStringObject(subval2_obj, subval2);

        subval3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        robj subval3_obj;
        initStaticStringObject(subval3_obj, subval3);

        rdb_subval1 = bitmapEncodeSubval(&subval1_obj);
        rdb_subval2 = bitmapEncodeSubval(&subval2_obj);
        rdb_subval3 = bitmapEncodeSubval(&subval3_obj);

        rio rdbcold,rdbwarm;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save cold, 3 subkeys */
        rioInitWithBuffer(&rdbcold,sdsempty());
        test_assert(rdbKeySaveWarmColdInit(saveData, db, (decodedResult*)decoded_meta) == 0);

        test_assert(rdbKeySaveStart(saveData, &rdbcold) == 0);

        decoded_data->subkey = f1, decoded_data->rdbraw = sdsnewlen(rdb_subval1 + 1, sdslen(rdb_subval1) - 1);
        decoded_data->version = saveData->object_meta->version;
        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0 && saveData->saved == 1);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        decoded_data->subkey = f2, decoded_data->rdbraw = sdsnewlen(rdb_subval2 + 1, sdslen(rdb_subval2) - 1);
        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0 && saveData->saved == 2);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;


        decoded_data->subkey = f3, decoded_data->rdbraw = sdsnewlen(rdb_subval3 + 1, sdslen(rdb_subval3) - 1);
        test_assert(rdbKeySave(saveData,&rdbcold,decoded_data) == 0 && saveData->saved == 3);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        test_assert(rdbKeySaveEnd(saveData,&rdbwarm,0) == 0);

        rdbKeySaveDataDeinit(saveData);

        coldraw = rdbcold.io.buffer.ptr;
        rioInitWithBuffer(&rdbcold,coldraw);

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbcold)) == RDB_OPCODE_FREQ);
        rioRead(&rdbcold, &byte, 1);
        test_assert((type = rdbLoadType(&rdbcold)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbcold,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbcold,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        cont = rdbKeyLoad(load,&rdbcold,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval1) && !sdscmp(subkey, raw_f1));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbcold,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval2) && !sdscmp(subkey, raw_f2));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbcold,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 0 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval3) && !sdscmp(subkey, raw_f3));

        sdsfree(subkey), sdsfree(subraw);

        test_assert(load->loaded_fields == 3);
        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);


        sdsfree(f1),sdsfree(f2),sdsfree(f3),sdsfree(subval1),sdsfree(subval2),sdsfree(subval3),sdsfree(rdb_subval1),sdsfree(rdb_subval2),sdsfree(rdb_subval3),sdsfree(coldraw);
        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);
        sdsfree(key);

        sdsfree(raw_f1);
        sdsfree(raw_f2);
        sdsfree(raw_f3);

        f1 = NULL, f2 = NULL, f3 = NULL;
        subval1 = NULL,subval2 = NULL,subval3 = NULL;
        rdb_subval1 = NULL, rdb_subval2 = NULL,rdb_subval3 = NULL,
        coldraw = NULL;
        raw_f1 = NULL, raw_f2 = NULL, raw_f3 = NULL,
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);
    }

    TEST("bitmap - save & load warm RDB_TYPE_BITMAP (hot subkey only in redis)") {
        
        sds f1, f2, f3, subval1, subval2, subval3, rdb_subval1, rdb_subval2, rdb_subval3, warmraw, warm_str1;

        robj *save_key1, *save_warm_bitmap1;

        uint64_t V = server.swap_key_version = 1; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);

        f1 = bitmapEncodeSubkeyIdx(0);
        f2 = bitmapEncodeSubkeyIdx(1);
        f3 = bitmapEncodeSubkeyIdx(2);

        sds raw_f2 = bitmapEncodeSubkey(db, save_key1->ptr, V, f2);
        sds raw_f3 = bitmapEncodeSubkey(db, save_key1->ptr, V, f3);
        sds raw_f1 = bitmapEncodeSubkey(db, save_key1->ptr, V, f1);

        subval1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        subval1[0] = '1';

        robj subval1_obj;
        initStaticStringObject(subval1_obj, subval1);

        subval2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);

        robj subval2_obj;
        initStaticStringObject(subval2_obj, subval2);

        subval3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        robj subval3_obj;
        initStaticStringObject(subval3_obj, subval3);

        rdb_subval1 = bitmapEncodeSubval(&subval1_obj);
        rdb_subval2 = bitmapEncodeSubval(&subval2_obj);
        rdb_subval3 = bitmapEncodeSubval(&subval3_obj);

        rio rdbwarm;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save warm */

        dbDelete(db, save_key1);
        rioInitWithBuffer(&rdbwarm,sdsempty());

        /* 2 subkeys are hot */
        warm_str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2);
        warm_str1[0] = '1';
        save_warm_bitmap1 = createStringObject(warm_str1, sdslen(warm_str1));
        save_warm_bitmap1->type = OBJ_STRING;

        bitmapMeta *warm_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, part {0, 2} only in redis, {1} in rocksDb. */
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta, 0, 0, STATUS_HOT);

        warm_bitmap_meta->pure_cold_subkeys_num = 1;
        warm_bitmap_meta->size = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *warm_object_meta = createBitmapObjectMeta(0, warm_bitmap_meta);

        dbAdd(db, save_key1, save_warm_bitmap1);
        dbAddMeta(db, save_key1, warm_object_meta);

        test_assert(rdbKeySaveWarmColdInit(saveData, db, (decodedResult*)decoded_meta) == 0);
        test_assert(rdbKeySaveStart(saveData, &rdbwarm) == 0);

        decoded_data->version = saveData->object_meta->version;
        decoded_data->subkey = f2, decoded_data->rdbraw = sdsnewlen(rdb_subval2 + 1, sdslen(rdb_subval2) - 1);

        test_assert(rdbKeySave(saveData,&rdbwarm,decoded_data) == 0);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        test_assert(rdbKeySaveEnd(saveData,&rdbwarm,0) == 0);

        rdbKeySaveDataDeinit(saveData);

        dbDelete(db, save_key1);

        warmraw = rdbwarm.io.buffer.ptr;
        rioInitWithBuffer(&rdbwarm,warmraw);

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbwarm)) == RDB_OPCODE_FREQ);
        rioRead(&rdbwarm, &byte, 1);
        test_assert((type = rdbLoadType(&rdbwarm)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbwarm,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbwarm,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        cont = rdbKeyLoad(load,&rdbwarm,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval1));
        test_assert(!sdscmp(subkey, raw_f1));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbwarm,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval2) && !sdscmp(subkey, raw_f2));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbwarm,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 0 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval3) && !sdscmp(subkey, raw_f3));

        sdsfree(subkey), sdsfree(subraw);

        test_assert(load->loaded_fields == 3);
        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);

        sdsfree(f1),sdsfree(f2),sdsfree(f3);
        sdsfree(subval1),sdsfree(subval2),sdsfree(subval3);
        sdsfree(rdb_subval1);
        sdsfree(rdb_subval2);
        sdsfree(rdb_subval3);
        sdsfree(warmraw),sdsfree(warm_str1);

        sdsfree(raw_f1),sdsfree(raw_f2),sdsfree(raw_f3);

        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);
        sdsfree(key);

        f1 = NULL, f2 = NULL, f3 = NULL;
        subval1 = NULL, subval2 = NULL, subval3 = NULL;
        rdb_subval1 = NULL, rdb_subval2 = NULL,rdb_subval3 = NULL,
        warmraw = NULL, warm_str1 = NULL;
        raw_f1 = NULL, raw_f2 = NULL, raw_f3 = NULL,
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);
    }

    TEST("bitmap - save & load warm RDB_TYPE_BITMAP (hot subkey both in redis and rocks)") {
        
        sds f1, f2, f3, subval1, subval2, subval3, rdb_subval1, rdb_subval2, rdb_subval3, warmraw, warm_str1;

        robj *save_key1, *save_warm_bitmap1;

        uint64_t V = server.swap_key_version = 1; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);

        f1 = bitmapEncodeSubkeyIdx(0);
        f2 = bitmapEncodeSubkeyIdx(1);
        f3 = bitmapEncodeSubkeyIdx(2);

        sds raw_f2 = bitmapEncodeSubkey(db, save_key1->ptr, V, f2);
        sds raw_f3 = bitmapEncodeSubkey(db, save_key1->ptr, V, f3);
        sds raw_f1 = bitmapEncodeSubkey(db, save_key1->ptr, V, f1);

        subval1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        subval1[0] = '1';

        robj subval1_obj;
        initStaticStringObject(subval1_obj, subval1);

        subval2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);

        robj subval2_obj;
        initStaticStringObject(subval2_obj, subval2);

        subval3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        robj subval3_obj;
        initStaticStringObject(subval3_obj, subval3);

        rdb_subval1 = bitmapEncodeSubval(&subval1_obj);
        rdb_subval2 = bitmapEncodeSubval(&subval2_obj);
        rdb_subval3 = bitmapEncodeSubval(&subval3_obj);

        rio rdbwarm;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save warm */

        dbDelete(db, save_key1);
        rioInitWithBuffer(&rdbwarm,sdsempty());

        /* 2 subkeys are hot */
        warm_str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE + BITMAP_SUBKEY_SIZE / 2);
        warm_str1[0] = '1';
        save_warm_bitmap1 = createStringObject(warm_str1, sdslen(warm_str1));
        save_warm_bitmap1->type = OBJ_STRING;

        bitmapMeta *warm_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, part {0, 2} both in redis and rocks, {1} in rocksDb. */
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(warm_bitmap_meta, 0, 0, STATUS_HOT);

        warm_bitmap_meta->pure_cold_subkeys_num = 1;
        warm_bitmap_meta->size = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *warm_object_meta = createBitmapObjectMeta(0, warm_bitmap_meta);

        dbAdd(db, save_key1, save_warm_bitmap1);
        dbAddMeta(db, save_key1, warm_object_meta);

        test_assert(rdbKeySaveWarmColdInit(saveData, db, (decodedResult*)decoded_meta) == 0);
        test_assert(rdbKeySaveStart(saveData, &rdbwarm) == 0);

        decoded_data->version = saveData->object_meta->version;
        decoded_data->subkey = f1, decoded_data->rdbraw = sdsnewlen(rdb_subval1 + 1, sdslen(rdb_subval1) - 1);

        test_assert(rdbKeySave(saveData,&rdbwarm,decoded_data) == 0);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        decoded_data->version = saveData->object_meta->version;
        decoded_data->subkey = f2, decoded_data->rdbraw = sdsnewlen(rdb_subval2 + 1, sdslen(rdb_subval2) - 1);

        test_assert(rdbKeySave(saveData,&rdbwarm,decoded_data) == 0);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        decoded_data->version = saveData->object_meta->version;
        decoded_data->subkey = f3, decoded_data->rdbraw = sdsnewlen(rdb_subval3 + 1, sdslen(rdb_subval3) - 1);

        test_assert(rdbKeySave(saveData,&rdbwarm,decoded_data) == 0);

        sdsfree(decoded_data->rdbraw);
        decoded_data->rdbraw = NULL;
        decoded_data->subkey = NULL;

        test_assert(rdbKeySaveEnd(saveData,&rdbwarm,0) == 0);

        rdbKeySaveDataDeinit(saveData);

        dbDelete(db, save_key1);

        warmraw = rdbwarm.io.buffer.ptr;
        rioInitWithBuffer(&rdbwarm,warmraw);

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbwarm)) == RDB_OPCODE_FREQ);
        rioRead(&rdbwarm, &byte, 1);
        test_assert((type = rdbLoadType(&rdbwarm)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbwarm,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbwarm,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        cont = rdbKeyLoad(load,&rdbwarm,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval1));
        test_assert(!sdscmp(subkey, raw_f1));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbwarm,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval2) && !sdscmp(subkey, raw_f2));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbwarm,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 0 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval3) && !sdscmp(subkey, raw_f3));

        sdsfree(subkey), sdsfree(subraw);

        test_assert(load->loaded_fields == 3);
        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);

        sdsfree(f1),sdsfree(f2),sdsfree(f3);
        sdsfree(subval1),sdsfree(subval2),sdsfree(subval3);
        sdsfree(rdb_subval1);
        sdsfree(rdb_subval2);
        sdsfree(rdb_subval3);
        sdsfree(warmraw),sdsfree(warm_str1);

        sdsfree(raw_f1),sdsfree(raw_f2),sdsfree(raw_f3);

        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);
        sdsfree(key);

        f1 = NULL, f2 = NULL, f3 = NULL;
        subval1 = NULL, subval2 = NULL, subval3 = NULL;
        rdb_subval1 = NULL, rdb_subval2 = NULL,rdb_subval3 = NULL,
        warmraw = NULL, warm_str1 = NULL;
        raw_f1 = NULL, raw_f2 = NULL, raw_f3 = NULL,
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);
    }

    TEST("bitmap - save & load hot RDB_TYPE_BITMAP") {
        
        sds f1, f2, f3, subval1, subval2, subval3, rdb_subval1, rdb_subval2, rdb_subval3, hotraw, hot_str1;

        robj *save_key1, *save_hot_bitmap1;

        uint64_t V = server.swap_key_version = 1; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);

        f1 = bitmapEncodeSubkeyIdx(0);
        f2 = bitmapEncodeSubkeyIdx(1);
        f3 = bitmapEncodeSubkeyIdx(2);

        sds raw_f1 = bitmapEncodeSubkey(db, save_key1->ptr, V, f1);
        sds raw_f2 = bitmapEncodeSubkey(db, save_key1->ptr, V, f2);
        sds raw_f3 = bitmapEncodeSubkey(db, save_key1->ptr, V, f3);

        subval1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        subval1[0] = '1';

        robj subval1_obj;
        initStaticStringObject(subval1_obj, subval1);

        subval2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);

        robj subval2_obj;
        initStaticStringObject(subval2_obj, subval2);

        subval3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        robj subval3_obj;
        initStaticStringObject(subval3_obj, subval3);

        rdb_subval1 = bitmapEncodeSubval(&subval1_obj);
        rdb_subval2 = bitmapEncodeSubval(&subval2_obj);
        rdb_subval3 = bitmapEncodeSubval(&subval3_obj);

        rio rdbhot;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save hot */

        dbDelete(db, save_key1);
        rioInitWithBuffer(&rdbhot,sdsempty());

        /* 3 subkeys are hot */
        hot_str1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        hot_str1[0] = '1';
        save_hot_bitmap1 = createStringObject(hot_str1, sdslen(hot_str1));
        save_hot_bitmap1->type = OBJ_STRING;

        bitmapMeta *hot_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, {0, 1, 2} both in redis and rocks. */
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 1, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 0, STATUS_HOT);

        hot_bitmap_meta->pure_cold_subkeys_num = 0;
        hot_bitmap_meta->size = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *hot_object_meta = createBitmapObjectMeta(0, hot_bitmap_meta);

        dbAdd(db, save_key1, save_hot_bitmap1);
        dbAddMeta(db, save_key1, hot_object_meta);

        test_assert(rdbKeySaveHotExtensionInit(saveData, db, save_key1->ptr) == 0);
        test_assert(rdbKeySaveHotExtension(saveData, &rdbhot) == 0);

        rdbKeySaveDataDeinit(saveData);

        hotraw = rdbhot.io.buffer.ptr;
        rioInitWithBuffer(&rdbhot,hotraw);

        dbDelete(db, save_key1);

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_OPCODE_FREQ);
        rioRead(&rdbhot, &byte, 1);
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbhot,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbhot,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval1));
        test_assert(!sdscmp(subkey, raw_f1));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval2) && !sdscmp(subkey, raw_f2));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 0 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval3) && !sdscmp(subkey, raw_f3));

        sdsfree(subkey), sdsfree(subraw);

        test_assert(load->loaded_fields == 3);
        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);

        sdsfree(f1),sdsfree(f2),sdsfree(f3);
        sdsfree(subval1),sdsfree(subval2),sdsfree(subval3);
        sdsfree(rdb_subval1);
        sdsfree(rdb_subval2);
        sdsfree(rdb_subval3);
        sdsfree(hotraw),sdsfree(hot_str1);

        sdsfree(raw_f1),sdsfree(raw_f2),sdsfree(raw_f3);

        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);
        sdsfree(key);

        f1 = NULL, f2 = NULL, f3 = NULL;
        subval1 = NULL, subval2 = NULL, subval3 = NULL;
        rdb_subval1 = NULL, rdb_subval2 = NULL,rdb_subval3 = NULL,
        hotraw = NULL, hot_str1 = NULL;
        raw_f1 = NULL, raw_f2 = NULL, raw_f3 = NULL,
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);
    }

    TEST("bitmap - save & load pure hot RDB_TYPE_BITMAP") {
        sds f1, f2, f3, subval1, subval2, subval3, rdb_subval1, rdb_subval2, rdb_subval3, hotraw, hot_bitmap;

        robj *save_key1, *save_hot_bitmap1;

        uint64_t V = server.swap_key_version = 0; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);
        dbDelete(db, save_key1);

        f1 = bitmapEncodeSubkeyIdx(0), f2 = bitmapEncodeSubkeyIdx(1), f3 = bitmapEncodeSubkeyIdx(2);

        sds raw_f1 = bitmapEncodeSubkey(db, save_key1->ptr, V, f1);
        sds raw_f2 = bitmapEncodeSubkey(db, save_key1->ptr, V, f2);
        sds raw_f3 = bitmapEncodeSubkey(db, save_key1->ptr, V, f3);

        long l1 = sdslen(raw_f1);
        long l2 = sdslen(raw_f2);
        test_assert(l1 != 0);
        test_assert(l1 == l2);

        subval1 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);
        subval1[0] = '1';

        robj subval1_obj;
        initStaticStringObject(subval1_obj, subval1);

        subval2 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE);

        robj subval2_obj;
        initStaticStringObject(subval2_obj, subval2);

        subval3 = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE / 2);

        robj subval3_obj;
        initStaticStringObject(subval3_obj, subval3);

        rdb_subval1 = bitmapEncodeSubval(&subval1_obj);
        rdb_subval2 = bitmapEncodeSubval(&subval2_obj);
        rdb_subval3 = bitmapEncodeSubval(&subval3_obj);

        rio rdbhot;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save hot */
        hot_bitmap = sdsnewlen(NULL, BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2);
        hot_bitmap[0] = '1';

        save_hot_bitmap1 = createStringObject(hot_bitmap, sdslen(hot_bitmap));

        /* 3 subkeys are hot */
        save_hot_bitmap1->type = OBJ_STRING;

        bitmapMeta *hot_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, part {0,1,2} both in redis and rocks. */
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 1, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 0, STATUS_HOT);

        hot_bitmap_meta->pure_cold_subkeys_num = 0;
        hot_bitmap_meta->size = BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2;

        objectMeta *hot_object_meta = createBitmapObjectMeta(0, hot_bitmap_meta);

        dbAdd(db, save_key1, save_hot_bitmap1);
        dbAddMeta(db, save_key1, hot_object_meta);

        rioInitWithBuffer(&rdbhot,sdsempty());

        test_assert(rdbKeySaveHotExtensionInit(saveData, db, save_key1->ptr) == 0);
        test_assert(rdbKeySaveHotExtension(saveData, &rdbhot) == 0);

        rdbKeySaveDataDeinit(saveData);

        hotraw = rdbhot.io.buffer.ptr;
        rioInitWithBuffer(&rdbhot,hotraw);

        dbDelete(db, save_key1);

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_OPCODE_FREQ);
        rioRead(&rdbhot, &byte, 1);
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbhot,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(BITMAP_SUBKEY_SIZE * 2 + BITMAP_SUBKEY_SIZE / 2),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbhot,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval1));

        l1 = sdslen(raw_f1);
        test_assert(l1 != 0);

        test_assert(!sdscmp(subkey, raw_f1));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval2) && !sdscmp(subkey, raw_f2));

        sdsfree(subkey), sdsfree(subraw);

        cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 0 && err == 0);
        test_assert(!sdscmp(subraw, rdb_subval3) && !sdscmp(subkey, raw_f3));

        sdsfree(subkey), sdsfree(subraw);

        test_assert(load->loaded_fields == 3);
        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);

        sdsfree(f1),sdsfree(f2),sdsfree(f3);
        sdsfree(subval1), sdsfree(subval2), sdsfree(subval3);
        sdsfree(rdb_subval1);
        sdsfree(rdb_subval2);
        sdsfree(rdb_subval3);
        sdsfree(hotraw),sdsfree(hot_bitmap);

        sdsfree(raw_f1),sdsfree(raw_f2),sdsfree(raw_f3);

        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);
        sdsfree(key);


        f1 = NULL, f2 = NULL, f3 = NULL;
        subval1 = NULL, subval2 = NULL, subval3 = NULL;
        rdb_subval1 = NULL, rdb_subval2 = NULL,rdb_subval3 = NULL,
        hotraw = NULL, hot_bitmap = NULL;
        raw_f1 = NULL, raw_f2 = NULL, raw_f3 = NULL,
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);
    }


    TEST("bitmap - save as 4KB & load as 2KB") {

        server.swap_bitmap_subkey_size = 4096;

        sds hotraw, hot_bitmap;

        robj *save_key1, *save_hot_bitmap1;

        uint64_t V = server.swap_key_version = 0; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);
        dbDelete(db, save_key1);

        rio rdbhot;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(4096 * 2 + 2048);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save hot */
        hot_bitmap = sdsnewlen(NULL, 4096 * 2 + 2048);
        hot_bitmap[0] = '1';

        save_hot_bitmap1 = createStringObject(hot_bitmap, sdslen(hot_bitmap));

        /* 3 subkeys are hot */
        save_hot_bitmap1->type = OBJ_STRING;

        bitmapMeta *hot_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 3, part {0,1,2} both in redis and rocks. */
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 1, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 0, STATUS_HOT);

        hot_bitmap_meta->pure_cold_subkeys_num = 0;
        hot_bitmap_meta->size = 4096 * 2 + 2048;

        objectMeta *hot_object_meta = createBitmapObjectMeta(0, hot_bitmap_meta);

        dbAdd(db, save_key1, save_hot_bitmap1);
        dbAddMeta(db, save_key1, hot_object_meta);

        rioInitWithBuffer(&rdbhot,sdsempty());

        test_assert(rdbKeySaveHotExtensionInit(saveData, db, save_key1->ptr) == 0);
        test_assert(rdbKeySaveHotExtension(saveData, &rdbhot) == 0);

        rdbKeySaveDataDeinit(saveData);

        hotraw = rdbhot.io.buffer.ptr;
        rioInitWithBuffer(&rdbhot,hotraw);

        dbDelete(db, save_key1);

        server.swap_bitmap_subkey_size = 2048;

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_OPCODE_FREQ);
        rioRead(&rdbhot, &byte, 1);
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbhot,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(2048 * 5),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbhot,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        sds new_subval1 = sdsnewlen(NULL, 2048);
        new_subval1[0] = '1';

        robj new_subval1_obj;
        initStaticStringObject(new_subval1_obj, new_subval1);

        sds rdb_new_subval1 = bitmapEncodeSubval(&new_subval1_obj);

        sds new_subval2 = sdsnewlen(NULL, 2048);
        robj new_subval2_obj;
        initStaticStringObject(new_subval2_obj, new_subval2);

        sds rdb_new_subval2 = bitmapEncodeSubval(&new_subval2_obj);

        do {

            subkey = NULL, subraw = NULL;

            cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
            test_assert(cf == DATA_CF && err == 0);

            if (subkey != NULL) {
                sds tmp_key = bitmapEncodeSubkeyIdx(load->loaded_fields - 1);
                sds raw_key = bitmapEncodeSubkey(db, save_key1->ptr, V, tmp_key);

                if (load->loaded_fields == 1) {
                    test_assert(!sdscmp(subraw, rdb_new_subval1) && !sdscmp(subkey, raw_key));
                } else if (load->loaded_fields != 1) {
                    test_assert(!sdscmp(subraw, rdb_new_subval2) && !sdscmp(subkey, raw_key));
                }

                sdsfree(raw_key);
                raw_key = NULL;

                sdsfree(tmp_key);
                tmp_key = NULL;

            }

            sdsfree(subkey), sdsfree(subraw);
            subkey = NULL, subraw = NULL;

        } while(cont == 1);

        sdsfree(new_subval1);
        sdsfree(new_subval2);
        new_subval1 = NULL;
        new_subval2 = NULL;

        sdsfree(rdb_new_subval1);
        sdsfree(rdb_new_subval2);
        rdb_new_subval1 = NULL;
        rdb_new_subval2 = NULL;

        test_assert(load->total_fields == 5);

        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);

        sdsfree(hotraw),sdsfree(hot_bitmap);

        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);

        sdsfree(key);
        key = NULL;

        hotraw = NULL, hot_bitmap = NULL;
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);

        server.swap_bitmap_subkey_size = 4096;
    }

    TEST("bitmap - save as 2KB & load as 4KB") {
        
        server.swap_bitmap_subkey_size = 2048;

        sds hotraw, hot_bitmap;

        robj *save_key1, *save_hot_bitmap1;

        uint64_t V = server.swap_key_version = 0; /* reset to zero so that save & load with same version(0) */

        save_key1 = createStringObject("save_key1",9);
        dbDelete(db, save_key1);

        rio rdbhot;
        rdbKeySaveData _saveData; rdbKeySaveData *saveData = &_saveData;

        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decodedData _decoded_data, *decoded_data = &_decoded_data;
        decoded_meta->dbid = db->id;
        decoded_meta->key = sdsnewlen("save_key1",9);
        decoded_meta->cf = META_CF;
        decoded_meta->version = 0;
        decoded_meta->swap_type = SWAP_TYPE_BITMAP;
        decoded_meta->expire = -1;
        decoded_meta->extend = encodeBitmapSize(2048 * 11);

        decoded_data->dbid = db->id;
        decoded_data->key = sdsnewlen("save_key1",9);
        decoded_data->cf = DATA_CF;
        decoded_data->version = 0;
        decoded_data->rdbtype = RDB_TYPE_STRING;

        /* rdbSave - save hot */
        hot_bitmap = sdsnewlen(NULL, 2048 * 11);
        hot_bitmap[0] = '1';

        save_hot_bitmap1 = createStringObject(hot_bitmap, sdslen(hot_bitmap));
        save_hot_bitmap1->type = OBJ_STRING;

        bitmapMeta *hot_bitmap_meta = bitmapMetaCreate();

        /* subkeys sum = 11, both hot. */
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 2, 2, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 1, 1, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 0, 0, STATUS_HOT);

        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 3, 3, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 4, 4, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 5, 5, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 6, 6, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 7, 7, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 8, 8, STATUS_HOT);

        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 9, 9, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 10, 10, STATUS_HOT);
        bitmapMetaSetSubkeyStatus(hot_bitmap_meta, 11, 11, STATUS_HOT);

        hot_bitmap_meta->pure_cold_subkeys_num = 0;
        hot_bitmap_meta->size = 2048 * 11;

        objectMeta *hot_object_meta = createBitmapObjectMeta(0, hot_bitmap_meta);

        dbAdd(db, save_key1, save_hot_bitmap1);
        dbAddMeta(db, save_key1, hot_object_meta);

        rioInitWithBuffer(&rdbhot,sdsempty());

        test_assert(rdbKeySaveHotExtensionInit(saveData, db, save_key1->ptr) == 0);
        test_assert(rdbKeySaveHotExtension(saveData, &rdbhot) == 0);

        rdbKeySaveDataDeinit(saveData);

        hotraw = rdbhot.io.buffer.ptr;
        rioInitWithBuffer(&rdbhot,hotraw);

        dbDelete(db, save_key1);

        server.swap_bitmap_subkey_size = 4096;

        int type;
        uint8_t byte;
        
        /* consume rdb header */
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_OPCODE_FREQ);
        rioRead(&rdbhot, &byte, 1);
        test_assert((type = rdbLoadType(&rdbhot)) == RDB_TYPE_BITMAP);
        sds key = rdbGenericLoadStringObject(&rdbhot,RDB_LOAD_SDS,NULL);
        test_assert(!sdscmp(key, save_key1->ptr));

        /* consume object */
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load, type, db, key, -1, NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf, err;

        sds expected_metakey = rocksEncodeMetaKey(db, save_key1->ptr),
            expected_metaextend = encodeBitmapSize(4096 * 5 + 2048),
            expected_metaval = rocksEncodeMetaVal(SWAP_TYPE_BITMAP, -1, V, expected_metaextend);

        bitmapLoadStart(load,&rdbhot,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,expected_metakey));
        test_assert(!sdscmp(metaval,expected_metaval));

        sds new_subval1 = sdsnewlen(NULL, 4096);
        new_subval1[0] = '1';

        robj new_subval1_obj;
        initStaticStringObject(new_subval1_obj, new_subval1);

        sds rdb_new_subval1 = bitmapEncodeSubval(&new_subval1_obj);

        sds new_subval2 = sdsnewlen(NULL, 4096);
        robj new_subval2_obj;
        initStaticStringObject(new_subval2_obj, new_subval2);

        sds rdb_new_subval2 = bitmapEncodeSubval(&new_subval2_obj);

        sds new_subval3 = sdsnewlen(NULL, 2048);
        robj new_subval3_obj;
        initStaticStringObject(new_subval3_obj, new_subval3);

        sds rdb_new_subval3 = bitmapEncodeSubval(&new_subval3_obj);

        do {

            subkey = NULL, subraw = NULL;

            cont = rdbKeyLoad(load,&rdbhot,&cf,&subkey,&subraw,&err);
            test_assert(cf == DATA_CF && err == 0);

            if (subkey != NULL) {
                sds tmp_key = bitmapEncodeSubkeyIdx(load->loaded_fields - 1);
                sds raw_key = bitmapEncodeSubkey(db, save_key1->ptr, V, tmp_key);

                if (load->loaded_fields == 1) {
                    test_assert(!sdscmp(subraw, rdb_new_subval1) && !sdscmp(subkey, raw_key));
                } else if (load->loaded_fields == 6) {
                    test_assert(!sdscmp(subraw, rdb_new_subval3) && !sdscmp(subkey, raw_key));
                } else {
                    test_assert(!sdscmp(subraw, rdb_new_subval2) && !sdscmp(subkey, raw_key));
                } 

                sdsfree(raw_key);
                raw_key = NULL;

                sdsfree(tmp_key);
                tmp_key = NULL;

            }

            sdsfree(subkey), sdsfree(subraw);
            subkey = NULL, subraw = NULL;

        } while(cont == 1);

        sdsfree(new_subval1);
        sdsfree(new_subval2);
        sdsfree(new_subval3);
        new_subval1 = NULL;
        new_subval2 = NULL;
        new_subval3 = NULL;

        sdsfree(rdb_new_subval1);
        sdsfree(rdb_new_subval2);
        sdsfree(rdb_new_subval3);
        rdb_new_subval1 = NULL;
        rdb_new_subval2 = NULL;
        rdb_new_subval3 = NULL;

        test_assert(load->total_fields == 6);

        test_assert(load->swap_type == SWAP_TYPE_BITMAP);
        rdbKeyLoadDataDeinit(load);

        sdsfree(hotraw),sdsfree(hot_bitmap);

        sdsfree(metakey),sdsfree(metaval);
        sdsfree(expected_metakey),sdsfree(expected_metaextend),sdsfree(expected_metaval);

        sdsfree(key);
        key = NULL;

        hotraw = NULL, hot_bitmap = NULL;
        metakey = NULL, metaval = NULL;
        expected_metakey = NULL, expected_metaextend = NULL, expected_metaval = NULL;
        key = NULL;

        subkey = NULL, subraw = NULL;

        decrRefCount(save_key1);

        decodedResultDeinit((decodedResult*)decoded_meta);
        decodedResultDeinit((decodedResult*)decoded_data);
    }

    server.swap_evict_step_max_subkeys = originEvictStepMaxSubkey;
    server.swap_evict_step_max_memory = originEvictStepMaxMemory;

    return error;
}

#endif
