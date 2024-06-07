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
#include "ctrip_roaring_malloc.h"
#include "ctrip_roaring_bitmap.h"
#include <assert.h>

#define CONTAINER_BITS 12  /* default save the lower 12 bits in the container, no more than 16 */
#define BUCKET_MAX_BITS 8
#define BITMAP_CONTAINER_CAPACITY (1 << CONTAINER_BITS) /* bits num */
#define BITMAP_CONTAINER_SIZE (1 << (CONTAINER_BITS - 3)) /* bit num -> byte num = size */
#define ARRAY_CONTAINER_CAPACITY ((BITMAP_CONTAINER_SIZE >> 1) - 1)   /* bits num, array container use uint16_t save index */
#define CONTAINER_CAPACITY BITMAP_CONTAINER_CAPACITY /* bits num */
#define CONTAINER_MASK ((1 << CONTAINER_BITS) - 1)
#define ARRAY_CONTAINER_EXPAND_SPEED 2
#define MOD_8_MASK 0x7
#define BITS_NUM_IN_BYTE 8

/* There are 3 kinds of container:
 *   arrray Container: sorted array of uint16_t (bit index)
 *   bitmap container: raw bitmap
 *   full container: when all bits set, containerInside is NULL
 */

#define CONTAINER_TYPE_ARRAY 0
#define CONTAINER_TYPE_BITMAP 1
#define CONTAINER_TYPE_FULL 2

typedef uint16_t arrayContainer;
typedef uint8_t bitmapContainer;

typedef struct roaringContainer {
    uint16_t elements_num;
    unsigned type:2;
    union {
        struct {
            unsigned padding:14;
            bitmapContainer *bitmap;
        } b;
        struct {
            unsigned capacity:14;
            arrayContainer *array;
        } a;
        struct {
            unsigned padding:14;
            void *none;
        } f;
    };
} roaringContainer;

struct roaringBitmap_t {
    uint8_t buckets_num;
    uint8_t* buckets;
    roaringContainer** containers;
};

static uint8_t bitsNumTable[256] =
        {
                0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
        };

/* utils api */

static inline uint8_t binarySearchLocUint8(const uint8_t *arr, uint8_t arr_size, uint8_t target)
{
    uint8_t left_idx = 0;
    uint8_t right_idx = arr_size;
    while (left_idx < right_idx) {
        uint8_t mid = left_idx + ((right_idx - left_idx) >> 1);
        if (arr[mid] == target) {
            return mid;
        } else if (arr[mid] > target) {
            right_idx = mid;
        } else {
            left_idx = mid + 1;
        }
    }
    return left_idx; /* if no target, loc should be here */
}

static inline uint16_t binarySearchLocUint16(const uint16_t *arr, uint16_t arr_size, uint16_t target)
{
    uint16_t left_idx = 0;
    uint16_t right_idx = arr_size;
    while (left_idx < right_idx) {
        uint16_t mid = left_idx + ((right_idx - left_idx) >> 1);
        if (arr[mid] == target) {
            return mid;
        } else if (arr[mid] > target) {
            right_idx = mid;
        } else {
            left_idx = mid + 1;
        }
    }
    return left_idx;  /* if no target, loc should be here */
}

static inline void bitmapSetbit(uint8_t *bitmap, uint16_t val)
{
    const uint8_t old_word = bitmap[val >> 3]; /* find the byte */
    const int bit_index = val & MOD_8_MASK; /* find the bit index in byte */
    const uint8_t new_word = old_word | (1 << bit_index);
    bitmap[val >> 3] = new_word;
}

static inline uint8_t bitmapCheckBitStatus(const uint8_t *bitmap, uint16_t val)
{
    const uint8_t old_word = bitmap[val >> 3]; /* find the byte */
    const int bit_index = val & MOD_8_MASK; /* find the bit index in byte */
    if ((old_word & (1 << bit_index)) != 0) {
        return 1;
    }
    return 0;
}

/* count the num of  bit 1 in four bytes */
static inline uint32_t countUint32Bits(uint32_t n)
{
    n = n - ((n >> 1) & 0x55555555);
    n = (n & 0x33333333) + ((n >> 2) & 0x33333333);
    n = (n + (n >> 4)) & 0x0F0F0F0F;
    return (n * 0x01010101) >> 24;
}

static uint32_t bitmapCountBits(uint8_t *bmp, uint32_t start_idx, uint32_t end_idx)
{
    uint8_t *p = bmp + start_idx;
    uint32_t bits_num = 0;
    uint32_t bytes_num = end_idx - start_idx + 1;
    while (bytes_num & 3) {
        bits_num += bitsNumTable[*p++];
        bytes_num--;
    }

    /* left bytes_num is 4 * n */
    while (bytes_num) {
        bits_num += countUint32Bits(*(uint32_t *)p);
        p += 4;
        bytes_num -= 4;
    }
    return bits_num;
}

static inline void clearContainer(roaringContainer *container)
{
    if (container->type == CONTAINER_TYPE_BITMAP) {
        roaring_free(container->b.bitmap);
        container->b.bitmap = NULL;
    } else if (container->type == CONTAINER_TYPE_ARRAY) {
        roaring_free(container->a.array);
        container->a.array = NULL;
        container->a.capacity = 0;
    } else {
        container->f.none = NULL;
    }
    container->elements_num = 0;
    container->type = CONTAINER_TYPE_ARRAY;
}

static inline void expandArrIfNeed(roaringContainer *container, uint16_t new_num)
{
    if (container->a.capacity >= new_num) {
        return;
    }

    uint32_t new_capacity = new_num * ARRAY_CONTAINER_EXPAND_SPEED;

    container->a.capacity = MIN(new_capacity, ARRAY_CONTAINER_CAPACITY);
    container->a.array = roaring_realloc(container->a.array, sizeof(arrayContainer) * container->a.capacity);
}

static inline void shrinkArrIfNeed(roaringContainer *container)
{
    if (container->elements_num == 0 || container->a.capacity <= container->elements_num * ARRAY_CONTAINER_EXPAND_SPEED) {
        return;
    }
    uint32_t new_capacity = container->elements_num * ARRAY_CONTAINER_EXPAND_SPEED;
    arrayContainer *new_arr = roaring_malloc(new_capacity * sizeof(arrayContainer));
    memcpy(new_arr, container->a.array, container->elements_num * sizeof(arrayContainer));

    roaring_free(container->a.array);
    container->a.array = new_arr;
    container->a.capacity = new_capacity;
}

 static inline void transArrayToBitmapContainer(roaringContainer *container)
{
    arrayContainer *oldArr = container->a.array;
    bitmapContainer *new_bmp = roaring_calloc(BITMAP_CONTAINER_SIZE);
    for (int i = 0; i < container->elements_num; i++) {
        bitmapSetbit(new_bmp, oldArr[i]);
    }
    roaring_free(container->a.array);
    container->b.bitmap = new_bmp;
    container->type = CONTAINER_TYPE_BITMAP;
}

static inline void transBitmapToArrayContainer(roaringContainer *container)
{
    bitmapContainer *bmp = container->b.bitmap;

    arrayContainer *new_arr = roaring_calloc(container->elements_num * sizeof(arrayContainer));
    uint32_t cursor = 0;
    for (uint32_t i = 0; i < BITMAP_CONTAINER_CAPACITY; i++) {
        if (bitmapCheckBitStatus(bmp, i)) {
            new_arr[cursor++] = i;
        }
    }
    roaring_free(container->b.bitmap);
    container->a.capacity = container->elements_num;
    container->a.array = new_arr;
    container->type = CONTAINER_TYPE_ARRAY;
}

static inline void transToFullContainer(roaringContainer *container)
{
    clearContainer(container);
    container->type = CONTAINER_TYPE_FULL;
    container->f.none = NULL;
    container->elements_num = CONTAINER_CAPACITY;
}

/* container set api */

static void bitmapContainerSetSingleBit(roaringContainer *container, uint16_t val)
{
    if (container->elements_num <= ARRAY_CONTAINER_CAPACITY) {
        transArrayToBitmapContainer(container);
    }

    bitmapContainer *bitmap = container->b.bitmap;

    const uint8_t old_word = bitmap[val >> 3];
    const int bit_index = val & MOD_8_MASK;
    const uint8_t new_word = old_word | (1 << bit_index);
    bitmap[val >> 3] = new_word;
    container->elements_num += (new_word ^ old_word) >> bit_index;
}

static void bitmapContainerSetBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    if (min_val == max_val) {
        bitmapContainerSetSingleBit(container, min_val);
        return;
    }

    if (container->elements_num <= ARRAY_CONTAINER_CAPACITY) {
        transArrayToBitmapContainer(container);
    }

    bitmapContainer *bitmap = container->b.bitmap;

    /* assuming min_val is first bit in byte, max_val is last bit in byte */
    uint16_t first_full_byte_idx = min_val >> 3;
    uint16_t last_full_byte_idx = max_val >> 3;

    /* min_val max_val at the same byte */
    if (first_full_byte_idx == last_full_byte_idx) {
        uint8_t *byte = bitmap + first_full_byte_idx;
        uint8_t add_bits_num = max_val - min_val + 1;
        uint8_t bit_idx = min_val & MOD_8_MASK;

        uint8_t old_bits_num = bitsNumTable[*byte & (((1 << add_bits_num) - 1) << bit_idx)]; /* n bits in the mid */
        *byte |= ((1 << add_bits_num) - 1) << bit_idx;
        container->elements_num += (add_bits_num - old_bits_num);
        return;
    }

    /* min_val max_val at different bytes
    if min_val is not first bit in byte */
    if (min_val > first_full_byte_idx << 3) {
        uint8_t *first_byte = bitmap + first_full_byte_idx;
        uint8_t add_bits_num = ((first_full_byte_idx + 1) << 3) - min_val;  /* upper n bits */
        uint8_t old_bits_num = bitsNumTable[*first_byte & ~((1 << (8 - add_bits_num)) - 1)];

        *first_byte |= ~((1 << (8 - add_bits_num)) - 1);
        container->elements_num += (add_bits_num - old_bits_num);
        first_full_byte_idx++;
    }

    /* max_val is not last bit in byte */
    if (max_val < ((last_full_byte_idx + 1) << 3) - 1) {
        uint8_t *last_byte = bitmap + last_full_byte_idx;
        uint8_t add_bits_num = max_val - (last_full_byte_idx << 3) + 1;
        uint8_t old_bits_num = bitsNumTable[*last_byte & ((1 << add_bits_num) - 1)];  /* lower n bits */

        *last_byte |= (1 << add_bits_num) - 1;
        container->elements_num += (add_bits_num - old_bits_num);
        last_full_byte_idx--;
    }

    if (first_full_byte_idx <= last_full_byte_idx) {
        uint32_t old_bit_num = bitmapCountBits(bitmap, first_full_byte_idx, last_full_byte_idx);
        memset(bitmap + first_full_byte_idx, 0xffU, last_full_byte_idx - first_full_byte_idx + 1);
        uint32_t new_bit_num = (last_full_byte_idx - first_full_byte_idx + 1) << 3;
        container->elements_num += (new_bit_num - old_bit_num);
    }
}

static void arrayContainerRebuildInterval(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    uint32_t new_elements_num = max_val - min_val + 1;

    if (new_elements_num > ARRAY_CONTAINER_CAPACITY) {
        clearContainer(container);
        bitmapContainerSetBit(container, min_val, max_val);
        return;
    }

    expandArrIfNeed(container, new_elements_num);

    arrayContainer *arr = container->a.array;
    for (uint32_t i = 0; i < new_elements_num; i++) {
        arr[i] = min_val + i;
    }
    container->elements_num = new_elements_num;
}

static void arrayContainerInsertInterval(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
     /* insert [min_val, max_val], if min_val or max_val alreadly exists, we will rewrite */
    arrayContainer *arr = container->a.array;

    uint16_t left_loc = binarySearchLocUint16(arr, container->elements_num, min_val); /* left_loc is not in perserved */
    uint16_t right_loc = binarySearchLocUint16(arr, container->elements_num, max_val + 1); /* right_loc is perserved */

    if (left_loc != container->elements_num && right_loc != 0 &&
        arr[left_loc] == min_val && arr[right_loc - 1] == max_val && right_loc - left_loc - 1 == max_val - min_val) {
        /* whole interval alreadly exists */
        return;
    }

    uint32_t left_perserved_num = left_loc;
    uint32_t right_perserved_num = container->elements_num - right_loc;
    uint32_t insert_num = max_val - min_val + 1;
    uint32_t new_num = left_perserved_num + insert_num + right_perserved_num; /* is impossible zero */

    if (new_num > ARRAY_CONTAINER_CAPACITY) {
        bitmapContainerSetBit(container, min_val, max_val);
        return;
    }

    expandArrIfNeed(container, new_num);
    arrayContainer *new_arr = container->a.array;

    if (right_perserved_num) {
        memmove(new_arr + left_perserved_num + insert_num, new_arr + left_perserved_num, sizeof(arrayContainer) * right_perserved_num);
    }
    for (uint32_t i = 0; i < insert_num; i++) {
        new_arr[left_perserved_num + i] = min_val + i;
    }

    container->elements_num = new_num;
}

static void arrayContainerSetSingleBit(roaringContainer *container, uint16_t val)
{
    arrayContainer *arr = container->a.array;

    uint16_t loc = binarySearchLocUint16(arr, container->elements_num, val);
    if (loc < container->elements_num && arr[loc] == val) {
        return;
    }
    if (container->elements_num + 1 > ARRAY_CONTAINER_CAPACITY) {
        bitmapContainerSetSingleBit(container, val);
        return;
    }

    expandArrIfNeed(container, container->elements_num + 1);
    arr = container->a.array;

    /* append mode */
    if (container->elements_num == 0 || arr[container->elements_num - 1] < val) {
        arr[container->elements_num++] = val;
    } else {
        /* insert mode */
        if (loc != container->elements_num) {
            memmove(arr + loc + 1, arr + loc, (container->elements_num - loc) * sizeof(arrayContainer));
        }
        arr[loc] = val;
        container->elements_num++;
    }
}

static void arrayContainerSetBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    if (min_val == max_val) {
        arrayContainerSetSingleBit(container, min_val);
        return;
    }
    if (container->elements_num == 0) {
        arrayContainerRebuildInterval(container, min_val, max_val);
        return;
    }
    arrayContainerInsertInterval(container, min_val, max_val);
}

static void containerSetBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    if (max_val - min_val + 1 == CONTAINER_CAPACITY) {
        transToFullContainer(container);
        return;
    }
    if (container->type == CONTAINER_TYPE_FULL) {
        return;
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        bitmapContainerSetBit(container, min_val, max_val);
    } else {
        arrayContainerSetBit(container, min_val, max_val);
    }
    if (container->elements_num == CONTAINER_CAPACITY) {
        transToFullContainer(container);
    }
}

 /* container clear api */

static void fullContainerClearBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    clearContainer(container);
    if (min_val != 0) {
        containerSetBit(container, 0, min_val - 1);
    }
    if (max_val != CONTAINER_CAPACITY - 1) {
        containerSetBit(container, max_val + 1, CONTAINER_CAPACITY - 1);
    }
}

static void bitmapContainerClearSingleBit(roaringContainer *container, uint16_t val)
{
    bitmapContainer *bitmap = container->b.bitmap;

    const uint8_t old_word = bitmap[val >> 3];
    const int bit_index = val & MOD_8_MASK;
    const uint8_t new_word = old_word & ~(1 << bit_index);
    bitmap[val >> 3] = new_word;
    container->elements_num -= (new_word ^ old_word) >> bit_index;
    if (container->elements_num <= ARRAY_CONTAINER_CAPACITY) {
        transBitmapToArrayContainer(container);
    }
}

static void bitmapContainerClearBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    if (min_val == max_val) {
        bitmapContainerClearSingleBit(container, min_val);
        return;
    }
    bitmapContainer *bitmap = container->b.bitmap;

    /* assuming min_val is first bit in byte, max_val is last bit in byte */
    uint16_t first_full_byte_idx = min_val >> 3;
    uint16_t last_full_byte_idx = max_val >> 3;

    if (first_full_byte_idx == last_full_byte_idx) {
        uint8_t *byte = bitmap + first_full_byte_idx;
        uint32_t clear_bit_num = max_val - min_val + 1;
        uint8_t bit_idx = min_val & MOD_8_MASK;

        uint32_t old_bit_nums = bitsNumTable[*byte & (((1 << clear_bit_num) - 1) << bit_idx)]; /* mid n bits */
        *byte &= ~(((1 << clear_bit_num) - 1) << bit_idx);
        container->elements_num -= old_bit_nums;
        return;
    }

    /* min_val max_val at different bytes */
    /* if min_val is not first bit in byte */
    if (min_val > first_full_byte_idx << 3) {
        uint8_t *first_byte = bitmap + (min_val >> 3);
        uint8_t clear_bits_num = ((first_full_byte_idx + 1) << 3) - min_val; /* clear the upper n bits */
        uint8_t old_bits_num = bitsNumTable[*first_byte & ~((1 << (8 - clear_bits_num)) - 1)];

        *first_byte &= (1 << (8 - clear_bits_num)) - 1;
        container->elements_num -= old_bits_num;

        first_full_byte_idx++;
    }

    /* max_val is not last bit in byte*/
    if (max_val < ((last_full_byte_idx + 1) << 3) - 1) {
        uint8_t *last_byte = bitmap + (max_val >> 3);
        uint8_t clear_bits_num = max_val - (last_full_byte_idx << 3) + 1; /* clear the lower bits */

        uint8_t old_bits_num = bitsNumTable[*last_byte & ((1 << clear_bits_num) - 1)];
        *last_byte &= ~((1 << clear_bits_num) - 1);
        container->elements_num -= old_bits_num;
        last_full_byte_idx--;
    }

    if (first_full_byte_idx <= last_full_byte_idx) {
        uint32_t old_bit_num = bitmapCountBits(bitmap, first_full_byte_idx, last_full_byte_idx);
        memset(bitmap + first_full_byte_idx, 0, last_full_byte_idx - first_full_byte_idx + 1);
        container->elements_num -= old_bit_num;
    }
    if (container->elements_num <= ARRAY_CONTAINER_CAPACITY) {
        transBitmapToArrayContainer(container);
    }
}

static void arrayContainerClearInterval(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    arrayContainer *arr = container->a.array;

    uint16_t right_loc = binarySearchLocUint16(arr, container->elements_num, max_val + 1); /* right_loc is perserved */
    uint16_t left_loc = binarySearchLocUint16(arr, container->elements_num, min_val); /* left_loc is not perserved */

    /* left_perserved_num , right_perserved_num are impossible both zero */
    uint32_t left_perserved_num = left_loc;
    uint32_t right_perserved_num = container->elements_num - right_loc;

    if (right_perserved_num != 0) {
        memmove(arr + left_perserved_num, arr + right_loc, sizeof(arrayContainer) * right_perserved_num);
    }
    container->elements_num = left_perserved_num + right_perserved_num;
    shrinkArrIfNeed(container);
}

static void arrayContainerClearSingleBit(roaringContainer *container, uint16_t val)
{
    arrayContainer *arr = container->a.array;

    uint16_t loc = binarySearchLocUint16(arr, container->elements_num, val);
    if (loc == container->elements_num || arr[loc] != val) {
        return;
    }

    uint32_t back_preserved_num = container->elements_num - loc - 1;
    if (back_preserved_num != 0) {
        memmove(arr + loc, arr + loc + 1, back_preserved_num * sizeof(arrayContainer));
    }
    container->elements_num--;
    shrinkArrIfNeed(container);
}

static void arrayContainerClearBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
     if (min_val == max_val) {
         arrayContainerClearSingleBit(container, min_val);
         return;
     }
    arrayContainer *arr = container->a.array;
    uint16_t first_val = arr[0];
    uint16_t last_val = arr[container->elements_num - 1];

    if (min_val <= first_val && max_val >= last_val) {
        container->elements_num = 0;
    } else if (max_val < first_val || min_val > last_val) {
        return;
    } else {
        /* just clear the part of the array */
        arrayContainerClearInterval(container, min_val, max_val);
    }
}

static void containerClearBit(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    if (container == NULL || container->elements_num == 0) {
        return;
    }
    uint32_t clear_num = max_val - min_val + 1;

    if (clear_num == CONTAINER_CAPACITY) {
        clearContainer(container);
        return;
    }
    if (container->type == CONTAINER_TYPE_FULL) {
        fullContainerClearBit(container, min_val, max_val);
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        bitmapContainerClearBit(container, min_val, max_val);
    } else {
        arrayContainerClearBit(container, min_val, max_val);
    }
}

 /* container get api */

static uint32_t bitmapContainerGetBitNum(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    bitmapContainer *bitmap = container->b.bitmap;

    if (min_val == max_val) {
        return bitmapCheckBitStatus(bitmap, min_val);
    }
    /* assuming min_val is first bit in byte, max_val is last bit in byte */
    uint16_t first_full_byte_idx = min_val >> 3;
    uint16_t last_full_byte_idx = max_val >> 3;

    uint32_t bits_num = 0;

    if (first_full_byte_idx == last_full_byte_idx) {
        uint8_t *byte = bitmap + first_full_byte_idx;
        uint32_t get_bit_num = max_val - min_val + 1;
        uint8_t bit_idx = min_val & MOD_8_MASK;

        return bitsNumTable[*byte & (((1 << get_bit_num) - 1) << bit_idx)]; /* n bits mid of the byte */
    }

    /* min_val max_val at different bytes */
    /* if min_val is not first bit in byte */
    if (min_val > first_full_byte_idx << 3) {
        uint8_t *first_byte = bitmap + first_full_byte_idx;
        uint8_t check_bits_num = ((first_full_byte_idx + 1) << 3) - min_val; /* the upper n bits */

        bits_num += bitsNumTable[*first_byte & ~((1 << (8 - check_bits_num)) - 1)];
        first_full_byte_idx++;
    }

    /* max_val is not last bit in byte */
    if (max_val < ((last_full_byte_idx + 1) << 3) - 1) {
        uint8_t *last_byte = bitmap + last_full_byte_idx;
        uint8_t check_bits_num = max_val - (last_full_byte_idx << 3) + 1; /* the lower n bits */

        bits_num += bitsNumTable[*last_byte & ((1 << check_bits_num) - 1)];
        last_full_byte_idx--;
    }
    if (first_full_byte_idx <= last_full_byte_idx) {
        bits_num += bitmapCountBits(bitmap, first_full_byte_idx, last_full_byte_idx);
    }

    return bits_num;
}

static uint8_t arrayContainerGetSingleBit(roaringContainer *container, uint16_t val) {
    arrayContainer *arr = container->a.array;

    uint16_t loc = binarySearchLocUint16(arr, container->elements_num, val);
    if (loc < container->elements_num && arr[loc] == val) {
        return 1;
    }
    return 0;
}

static uint32_t arrayContainerGetBitNum(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
     if (min_val == max_val) {
         return arrayContainerGetSingleBit(container, min_val);
     }
    arrayContainer *arr = container->a.array;
    uint16_t first_val = arr[0];
    uint16_t last_val = arr[container->elements_num - 1];

    if (max_val < first_val || min_val > last_val) {
        return 0;
    }
    uint16_t left_loc = binarySearchLocUint16(arr, container->elements_num, min_val);
    uint16_t right_loc = binarySearchLocUint16(arr, container->elements_num, max_val);

    /* max_val not exist */
    if (right_loc == container->elements_num || arr[right_loc] != max_val) {
        right_loc--;
    }
    if (right_loc >= left_loc) {
        return right_loc - left_loc + 1;
    }
    return 0;
}

static uint32_t containerGetBitNum(roaringContainer *container, uint16_t min_val, uint16_t max_val)
{
    if (container == NULL || container->elements_num == 0) {
        return 0;
    }
    if (max_val - min_val + 1 == CONTAINER_CAPACITY) {
        return container->elements_num;
    }

    if (container->type == CONTAINER_TYPE_FULL) {
        return max_val - min_val + 1;
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        return bitmapContainerGetBitNum(container, min_val, max_val);
    } else {
        return arrayContainerGetBitNum(container, min_val, max_val);
    }
}

/* rbm operate container api */

static void rbmDeleteBucket(roaringBitmap* rbm, uint8_t bucket_idx)
{
    uint32_t loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, bucket_idx);

    if (loc == rbm->buckets_num || rbm->buckets[loc] != bucket_idx) {
        return;
    }

    clearContainer(rbm->containers[loc]);
    roaring_free(rbm->containers[loc]);

    uint32_t left_perserved_num = loc;
    uint32_t right_perserved_num = rbm->buckets_num - loc - 1;
    uint32_t new_num = left_perserved_num + right_perserved_num;

    uint8_t *new_keys = roaring_malloc(sizeof(uint8_t) * new_num);
    roaringContainer **new_containers = roaring_malloc(sizeof(roaringContainer *) * new_num);

    if (left_perserved_num) {
        memcpy(new_keys, rbm->buckets, sizeof(uint8_t) * left_perserved_num);
        memcpy(new_containers, rbm->containers, sizeof(roaringContainer *) * left_perserved_num);
    }
    if (right_perserved_num) {
        memcpy(new_keys + left_perserved_num, rbm->buckets + left_perserved_num + 1, sizeof(uint8_t) * right_perserved_num);
        memcpy(new_containers + left_perserved_num, rbm->containers + left_perserved_num + 1, sizeof(roaringContainer *) * right_perserved_num);
    }

    roaring_free(rbm->buckets);
    roaring_free(rbm->containers);
    rbm->buckets = new_keys;
    rbm->containers = new_containers;
    rbm->buckets_num = new_num;
}

/* cursor is the physical idx of buckets, bucket_index will be saved in keys */
static void rbmInsertBucket(roaringBitmap* rbm, uint32_t cursor, uint8_t bucket_index)
{
    rbm->buckets = roaring_realloc(rbm->buckets, (rbm->buckets_num + 1) * sizeof(uint8_t));
    rbm->containers = roaring_realloc(rbm->containers, (rbm->buckets_num + 1) * sizeof(roaringContainer *));

    uint8_t left_buckets_num = cursor;

    if (rbm->buckets_num - left_buckets_num != 0) {
        memmove(rbm->buckets + left_buckets_num + 1, rbm->buckets + left_buckets_num, (rbm->buckets_num - left_buckets_num) * sizeof(uint8_t));
        memmove(rbm->containers + left_buckets_num + 1, rbm->containers + left_buckets_num, (rbm->buckets_num - left_buckets_num) *
                                                                                        sizeof(roaringContainer *));
    }

    rbm->buckets[left_buckets_num] = bucket_index;
    rbm->containers[left_buckets_num] = roaring_calloc(sizeof(roaringContainer));
    rbm->buckets_num++;
}

static roaringContainer *rbmGetContainerIfNoInsert(roaringBitmap* rbm, uint8_t bucket_index)
{
    uint8_t idx = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, bucket_index);

    if (idx < rbm->buckets_num && rbm->buckets[idx] == bucket_index) {
        return rbm->containers[idx];
    }
    rbmInsertBucket(rbm, idx, bucket_index);
    return rbm->containers[idx];
}

static roaringContainer *rbmGetContainer(roaringBitmap* rbm, uint8_t bucket_index)
{
    if (rbm->buckets_num == 0) {
        return NULL;
    }
    uint8_t idx = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, bucket_index);

    if (idx == rbm->buckets_num || rbm->buckets[idx] != bucket_index) {
        return NULL;
    }
    return rbm->containers[idx];
}

/* bucket set get clear api */

static void bucketClearBit(roaringBitmap* rbm, uint8_t bucket_idx, uint16_t min_val, uint16_t max_val)
{
    roaringContainer *container = rbmGetContainer(rbm, bucket_idx);
    if (container == NULL) {
        return;
    }
    containerClearBit(container, min_val, max_val);
    if (container->elements_num == 0) {
        rbmDeleteBucket(rbm, bucket_idx);
    }
}

static void bucketSetBit(roaringBitmap* rbm, uint8_t bucket_idx, uint16_t min_val, uint16_t max_val)
{
    containerSetBit(rbmGetContainerIfNoInsert(rbm, bucket_idx), min_val, max_val);
}

static uint32_t bucketGetBitNum(roaringBitmap* rbm, uint8_t bucket_idx, uint16_t min_val, uint16_t max_val)
{
    return containerGetBitNum(rbmGetContainer(rbm, bucket_idx), min_val, max_val);
}

static void rbmSetBucketsFull(roaringBitmap* rbm, uint8_t min_bucket, uint8_t max_bucket)
{
    uint32_t left_loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, min_bucket); /*  in set interval */
    uint32_t right_loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, max_bucket + 1); /* out of set interval */

    /* the buckets alreaddy exist */
    if (left_loc != rbm->buckets_num && right_loc != 0 && rbm->buckets[left_loc] == min_bucket &&
        rbm->buckets[right_loc - 1] == max_bucket && right_loc - 1 - left_loc == (uint32_t)(max_bucket - min_bucket)) {
        for (uint32_t i = left_loc; i < right_loc; i++) {
            transToFullContainer(rbm->containers[i]);
        }
        return;
    }

    for (uint32_t i = left_loc; i < right_loc; i++) {
        clearContainer(rbm->containers[i]);
        roaring_free(rbm->containers[i]);
    }

    uint32_t left_perserved_num = left_loc;
    uint32_t right_perserved_num = rbm->buckets_num - right_loc;
    uint32_t insert_num = max_bucket - min_bucket + 1;
    uint32_t new_num = left_perserved_num + insert_num + right_perserved_num;

    rbm->buckets = roaring_realloc(rbm->buckets, sizeof(uint8_t) * new_num);
    rbm->containers = roaring_realloc(rbm->containers, sizeof(roaringContainer *) * new_num);

    if (right_perserved_num) {
        memmove(rbm->buckets + left_perserved_num + insert_num, rbm->buckets + right_loc, sizeof(uint8_t) * right_perserved_num);
        memmove(rbm->containers + left_perserved_num + insert_num, rbm->containers + right_loc, sizeof(roaringContainer *) * right_perserved_num);
    }

    for (uint32_t i = left_perserved_num, cursor = 0; i < left_perserved_num + insert_num; i++, cursor++) {
        rbm->buckets[i] = min_bucket + cursor;
        rbm->containers[i] = roaring_calloc(sizeof(roaringContainer));
        transToFullContainer(rbm->containers[i]);
    }

    rbm->buckets_num = new_num;
}

static void rbmSetBucketsEmpty(roaringBitmap* rbm, uint8_t min_bucket, uint8_t max_bucket)
{
    uint32_t left_loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, min_bucket); /* in the interval to be deleted */
    uint32_t right_loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, max_bucket + 1); /* out of the interval to be deleted */

    /* interval to be deleted not exist */
    if (left_loc == right_loc) {
        return;
    }

    for (uint32_t i = left_loc; i < right_loc; i++) {
        clearContainer(rbm->containers[i]);
        roaring_free(rbm->containers[i]);
    }

    uint32_t left_perserved_num = left_loc;
    uint32_t right_perserved_num = rbm->buckets_num - right_loc;
    uint32_t new_num = left_perserved_num + right_perserved_num;

    uint8_t *new_keys = roaring_malloc(sizeof(uint8_t) * new_num);
    roaringContainer **new_containers = roaring_malloc(sizeof(roaringContainer *) * new_num);

    if (left_perserved_num) {
        memcpy(new_keys, rbm->buckets, sizeof(uint8_t) * left_perserved_num);
        memcpy(new_containers, rbm->containers, sizeof(roaringContainer *) * left_perserved_num);
    }
    if (right_perserved_num) {
        memcpy(new_keys + left_perserved_num, rbm->buckets + right_loc, sizeof(uint8_t) * right_perserved_num);
        memcpy(new_containers + left_perserved_num, rbm->containers + right_loc, sizeof(roaringContainer *) * right_perserved_num);
    }

    roaring_free(rbm->buckets);
    roaring_free(rbm->containers);
    rbm->buckets = new_keys;
    rbm->containers = new_containers;
    rbm->buckets_num = new_num;
}

static uint32_t rbmGetSingleBucketBitNum(roaringBitmap* rbm, uint8_t bucket_index)
{
    roaringContainer *container = rbmGetContainer(rbm, bucket_index);
    if (container == NULL) {
        return 0;
    }
    return container->elements_num;
}

static uint32_t rbmGetBucketsBitNum(roaringBitmap* rbm, uint8_t min_bucket, uint8_t max_bucket)
{
    assert(min_bucket <= max_bucket);

    if (min_bucket == max_bucket) {
        return rbmGetSingleBucketBitNum(rbm, min_bucket);
    }

    uint8_t left_loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, min_bucket);
    uint8_t right_loc = binarySearchLocUint8(rbm->buckets, rbm->buckets_num, max_bucket);

    uint32_t bits_num = 0;

    for (uint8_t i = left_loc; i <= right_loc; i++) {
        if (i == rbm->buckets_num || rbm->buckets[i] > max_bucket) {
            break;
        }
        bits_num += rbm->containers[i]->elements_num;
    }
    return bits_num;
}

/* rbm export api */

roaringBitmap* rbmCreate(void)
{
    roaringBitmap *bitmap = roaring_malloc(sizeof(roaringBitmap));
    bitmap->buckets_num = 0;
    bitmap->containers = NULL;
    bitmap->buckets = NULL;
    return bitmap;
}

void rbmDestory(roaringBitmap* rbm)
{
    if (rbm == NULL) {
        return;
    }
    roaring_free(rbm->buckets);
    for (int i = 0; i < rbm->buckets_num; i++) {
        clearContainer(rbm->containers[i]);
        roaring_free(rbm->containers[i]);
    }
    roaring_free(rbm->containers);
    roaring_free(rbm);
}

void rbmSetBitRange(roaringBitmap* rbm, uint32_t min_bit, uint32_t max_bit)
{
    serverAssert(rbm != NULL && min_bit <= max_bit);

    uint32_t first_bucket_idx = min_bit >> CONTAINER_BITS;
    uint32_t last_bucket_idx = max_bit >> CONTAINER_BITS;

    assert(last_bucket_idx < (1 << BUCKET_MAX_BITS));

    uint16_t min_val = min_bit & CONTAINER_MASK;
    uint16_t max_val = max_bit & CONTAINER_MASK;

    /* min_bit max_bit in the same Container */
    if (first_bucket_idx == last_bucket_idx) {
        bucketSetBit(rbm, first_bucket_idx, min_val, max_val);
        return;
    }

    /* assuming the min_bit is first bit in the container, max_bit is last bit in the container */
    uint32_t first_whole_bucket = first_bucket_idx;
    uint32_t last_whole_bucket = last_bucket_idx;

    /* min_bit is not first bit in the container */
    if ((min_bit & CONTAINER_MASK) != 0) {
        min_val = min_bit & CONTAINER_MASK;
        max_val = ((first_bucket_idx + 1) * CONTAINER_CAPACITY - 1) & CONTAINER_MASK;
        bucketSetBit(rbm, first_bucket_idx, min_val, max_val);
        first_whole_bucket++;
    }
    /* max_bit is not last bit in the container */
    if (((max_bit + 1) & CONTAINER_MASK) != 0) {
        min_val = (last_bucket_idx * CONTAINER_CAPACITY) & CONTAINER_MASK;
        max_val = max_bit & CONTAINER_MASK;
        bucketSetBit(rbm, last_bucket_idx, min_val, max_val);
        last_whole_bucket--;
    }

    if (first_whole_bucket <= last_whole_bucket) {
        rbmSetBucketsFull(rbm, first_whole_bucket, last_whole_bucket);
    }
}

uint32_t rbmGetBitRange(roaringBitmap* rbm, uint32_t min_bit, uint32_t max_bit)
{
    serverAssert(rbm != NULL && min_bit <= max_bit);
    uint32_t first_bucket_idx = min_bit >> CONTAINER_BITS;
    uint32_t last_bucket_idx = max_bit >> CONTAINER_BITS;

    assert(last_bucket_idx < (1 << BUCKET_MAX_BITS));

    uint16_t min_val = min_bit & CONTAINER_MASK;
    uint16_t max_val = max_bit & CONTAINER_MASK;

    uint32_t bits_num = 0;
    /* min_bit max_bit in the same Container */
    if (first_bucket_idx == last_bucket_idx) {
        return bucketGetBitNum(rbm, first_bucket_idx, min_val, max_val);
    }

    /* process  container of min_bit */
    max_val = ((first_bucket_idx + 1) * CONTAINER_CAPACITY - 1) & CONTAINER_MASK;
    bits_num += bucketGetBitNum(rbm, first_bucket_idx, min_val, max_val);

    /* process  container of max_bit */
    min_val = (last_bucket_idx * CONTAINER_CAPACITY) & CONTAINER_MASK;
    max_val = max_bit & CONTAINER_MASK;
    bits_num += bucketGetBitNum(rbm, last_bucket_idx, min_val, max_val);

    if (first_bucket_idx + 1 < last_bucket_idx) {
        bits_num += rbmGetBucketsBitNum(rbm, first_bucket_idx + 1, last_bucket_idx - 1);
    }

    return bits_num;
}

void rbmClearBitRange(roaringBitmap* rbm, uint32_t min_bit, uint32_t max_bit)
{
    serverAssert(rbm != NULL && min_bit <= max_bit);
    uint32_t first_bucket_idx = min_bit >> CONTAINER_BITS;
    uint32_t last_bucket_idx = max_bit >> CONTAINER_BITS;

    assert(last_bucket_idx < (1 << BUCKET_MAX_BITS));

    uint16_t min_val = min_bit & CONTAINER_MASK;
    uint16_t max_val = max_bit & CONTAINER_MASK;

    /* min_bit max_bit in the same Container */
    if (first_bucket_idx == last_bucket_idx) {
        bucketClearBit(rbm, first_bucket_idx, min_val, max_val);
        return;
    }

    /* assuming the min_bit is first bit in the container, max_bit is last bit in the container */
    uint32_t first_whole_bucket = first_bucket_idx;
    uint32_t last_whole_bucket = last_bucket_idx;

    /* min_bit is not first bit in the container */
    if ((min_bit & CONTAINER_MASK) != 0) {
        /* 处理startBit 所属Container */
        min_val = min_bit & CONTAINER_MASK;
        max_val = ((first_bucket_idx + 1) * CONTAINER_CAPACITY - 1) & CONTAINER_MASK;
        bucketClearBit(rbm, first_bucket_idx, min_val, max_val);
        first_whole_bucket++;
    }
    /* max_bit is not last bit in the container */
    if (((max_bit + 1) & CONTAINER_MASK) != 0) {
        min_val = (last_bucket_idx * CONTAINER_CAPACITY) & CONTAINER_MASK;
        max_val = max_bit & CONTAINER_MASK;
        bucketClearBit(rbm, last_bucket_idx, min_val, max_val);
        last_whole_bucket--;
    }

    if (first_whole_bucket <= last_whole_bucket) {
        rbmSetBucketsEmpty(rbm, first_whole_bucket, last_whole_bucket);
    }
}

static inline void containersDup(roaringContainer **dest_containers, roaringContainer **src_containers, uint32_t num)
{
    for (uint32_t i = 0; i < num; i++) {
        dest_containers[i] = roaring_calloc(sizeof(roaringContainer));
        dest_containers[i]->elements_num = src_containers[i]->elements_num;
        dest_containers[i]->type = src_containers[i]->type;
        if (dest_containers[i]->type == CONTAINER_TYPE_BITMAP) {
            dest_containers[i]->b.bitmap = roaring_malloc(BITMAP_CONTAINER_SIZE);
            memcpy(dest_containers[i]->b.bitmap, src_containers[i]->b.bitmap, BITMAP_CONTAINER_SIZE);
        } else if (dest_containers[i]->type == CONTAINER_TYPE_ARRAY) {
            dest_containers[i]->a.capacity = src_containers[i]->a.capacity;
            dest_containers[i]->a.array = roaring_malloc(dest_containers[i]->a.capacity * sizeof(arrayContainer));
            memcpy(dest_containers[i]->a.array, src_containers[i]->a.array, dest_containers[i]->a.capacity * sizeof(arrayContainer));
        }
    }
}

void rbmdup(roaringBitmap* dest_rbm, roaringBitmap* src_rbm)
{
    serverAssert(dest_rbm != NULL && src_rbm != NULL);
    serverAssert(dest_rbm->buckets == NULL && dest_rbm->containers == NULL);
    dest_rbm->buckets_num = src_rbm->buckets_num;
    dest_rbm->buckets = roaring_malloc(dest_rbm->buckets_num * sizeof(uint8_t));
    memcpy(dest_rbm->buckets, src_rbm->buckets, sizeof(uint8_t) * dest_rbm->buckets_num);
    dest_rbm->containers = roaring_calloc(dest_rbm->buckets_num * sizeof(roaringContainer *));
    containersDup(dest_rbm->containers, src_rbm->containers, dest_rbm->buckets_num);
}

static inline int containersAreEqual(roaringContainer **dest_containers, roaringContainer **src_containers, uint32_t num)
{
    for (uint32_t i = 0; i < num; i++) {
        if (dest_containers[i]->elements_num != src_containers[i]->elements_num || dest_containers[i]->type != src_containers[i]->type) {
            return 0;
        }
        if (dest_containers[i]->type == CONTAINER_TYPE_BITMAP) {
            if (0 != memcmp(dest_containers[i]->b.bitmap, src_containers[i]->b.bitmap, BITMAP_CONTAINER_SIZE)) {
                return 0;
            }
        } else if (dest_containers[i]->type == CONTAINER_TYPE_ARRAY) {
            if (0 != memcmp(dest_containers[i]->a.array, src_containers[i]->a.array, dest_containers[i]->elements_num * sizeof(arrayContainer))) {
                return 0;
            }
        }
    }
    return 1;
}

int rbmIsEqual(roaringBitmap* dest_rbm, roaringBitmap* src_rbm)
{
    serverAssert(dest_rbm != NULL && src_rbm != NULL);
    if (dest_rbm->buckets_num != src_rbm->buckets_num || 0 != memcmp(dest_rbm->buckets, src_rbm->buckets, dest_rbm->buckets_num *
            sizeof(uint8_t))) {
        return 0;
    }
    return containersAreEqual(dest_rbm->containers, src_rbm->containers, dest_rbm->buckets_num);
}

static inline uint32_t arrayContainerLocateSetBitPos(roaringContainer *container, uint8_t bit_idx_prefix, uint32_t *idx_arr_cursor, uint32_t bits_num)
{
    uint32_t left_num = bits_num;
    uint32_t idx_prefix = bit_idx_prefix;
    for (int i = 0; i < container->elements_num && left_num != 0; i++) {
        uint32_t bit_idx = (idx_prefix << CONTAINER_BITS) + container->a.array[i];
        *idx_arr_cursor = bit_idx;
        idx_arr_cursor++;
        left_num--;
    }
    return bits_num - left_num;
}

static inline uint32_t bitmapContainerLocateSetBitPos(roaringContainer *container, uint8_t bit_idx_prefix, uint32_t *idx_arr_cursor, uint32_t bits_num)
{
    uint32_t idx_prefix = bit_idx_prefix;
    uint32_t left_bytes = BITMAP_CONTAINER_SIZE;
    uint32_t byte_pos = 0;
    uint32_t left_num = bits_num;
    while (left_bytes > 0 && left_num) {
        bitmapContainer *cursor = container->b.bitmap + byte_pos;
        if (left_bytes > sizeof(uint64_t) && *((uint64_t *)(cursor)) == 0) {
            byte_pos += sizeof(uint64_t);
            left_bytes -= sizeof(uint64_t);
            continue;
        } else if (left_bytes > sizeof(uint32_t) && *((uint32_t *)(cursor)) == 0) {
            byte_pos += sizeof(uint32_t);
            left_bytes -= sizeof(uint32_t);
            continue;
        } else if (left_bytes > sizeof(uint16_t) && *((uint16_t *)(cursor)) == 0) {
            byte_pos += sizeof(uint16_t);
            left_bytes -= sizeof(uint16_t);
            continue;
        } else if (*((uint8_t *)(cursor)) == 0) {
            byte_pos += sizeof(uint8_t);
            left_bytes -= sizeof(uint8_t);
            continue;
        }

        const uint8_t word = *cursor;
        for (int i = 0; i < 8 && left_num != 0; i++) {
            if (word & (1 << i)) {
                uint32_t bit_idx = i + byte_pos * BITS_NUM_IN_BYTE + (idx_prefix << CONTAINER_BITS);
                *idx_arr_cursor = bit_idx;
                idx_arr_cursor++;
                left_num--;
            }
        }
        byte_pos += sizeof(uint8_t);
        left_bytes -= sizeof(uint8_t);
    }
    return bits_num - left_num;
}

static inline uint32_t fullContainerLocateSetBitPos(uint8_t bit_idx_prefix, uint32_t *idx_arr_cursor, uint32_t bits_num) {
    uint32_t idx_prefix = bit_idx_prefix;
    uint32_t left_num = bits_num;
    for (int i = 0; i < CONTAINER_CAPACITY && left_num != 0; i++, left_num--) {
        uint32_t bit_idx = (idx_prefix << CONTAINER_BITS) + i;
        *idx_arr_cursor = bit_idx;
        idx_arr_cursor++;
    }
    return bits_num - left_num;
}

static inline uint32_t bucketLocateSetBitPos(roaringBitmap *rbm, uint8_t bucketPhyIdx, uint32_t *idx_arr_cursor, uint32_t bits_num)
{
    uint8_t bit_idx_prefix = rbm->buckets[bucketPhyIdx];
    roaringContainer *container = rbm->containers[bucketPhyIdx];
    if (container == NULL) {
        return 0;
    }
    if (container->type == CONTAINER_TYPE_ARRAY) {
        return arrayContainerLocateSetBitPos(container, bit_idx_prefix, idx_arr_cursor, bits_num);
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        return bitmapContainerLocateSetBitPos(container, bit_idx_prefix, idx_arr_cursor, bits_num);
    } else {
        return fullContainerLocateSetBitPos(bit_idx_prefix, idx_arr_cursor, bits_num);
    }
}

uint32_t rbmLocateSetBitPos(roaringBitmap* rbm, uint32_t bits_num, uint32_t *idx_arr)
{
    serverAssert(rbm != NULL || bits_num != 0 || idx_arr != NULL);
    uint32_t *idx_arr_cursor = idx_arr;
    uint32_t left_bits_num = bits_num;
    for (int i = 0; i < rbm->buckets_num; i++) {
        uint32_t real_bits_num = bucketLocateSetBitPos(rbm, i, idx_arr_cursor, left_bits_num);
        left_bits_num -= real_bits_num;
        idx_arr_cursor += real_bits_num;
        if (left_bits_num == 0) {
            return bits_num;
        }
    }
    return bits_num - left_bits_num;
}

/* append to encoded if not NULL, update cursor anyway. */
static inline int rbmEncodeAppend_(char *encoded, size_t len, const void *p, size_t l, size_t *pcursor) {
    if (encoded) {
        if (*pcursor + l > len) return -1;
        memcpy(encoded + *pcursor,p,l);
    }
    *pcursor += l;
    return 0;
}

/* encode rbm to encoded if not NULL, return encoded length anyway. */
static ssize_t rbmEncode_(const roaringBitmap* rbm, char* encoded, size_t len) {
    size_t cursor = 0;

    if (rbmEncodeAppend_(encoded,len,&rbm->buckets_num,sizeof(rbm->buckets_num),&cursor)) goto err;
    if (rbmEncodeAppend_(encoded,len,rbm->buckets,sizeof(uint8_t)*rbm->buckets_num,&cursor)) goto err;

    for(int i = 0; i < rbm->buckets_num; i++) {
        uint16_t elements_num = htons(rbm->containers[i]->elements_num);
        if (rbmEncodeAppend_(encoded,len,&elements_num,sizeof(elements_num),&cursor)) goto err;

        uint8_t type = rbm->containers[i]->type;
        if (rbmEncodeAppend_(encoded,len,&type,sizeof(type),&cursor)) goto err;

        if (rbm->containers[i]->type == CONTAINER_TYPE_ARRAY) {
            size_t arrayLen = sizeof(arrayContainer) * rbm->containers[i]->elements_num;
            if (encoded) {
                if (cursor + arrayLen > len) goto err;
                for(int j=0; j<rbm->containers[i]->elements_num; j++) {
                    arrayContainer ele = htons(rbm->containers[i]->a.array[j]);
                    if (rbmEncodeAppend_(encoded,len,&ele,sizeof(ele),&cursor)) goto err;
                }
            } else {
                cursor += arrayLen;
            }
        } else if (rbm->containers[i]->type == CONTAINER_TYPE_BITMAP) {
            if (rbmEncodeAppend_(encoded,len,rbm->containers[i]->b.bitmap,BITMAP_CONTAINER_SIZE,&cursor)) goto err;
        } else if (rbm->containers[i]->type == CONTAINER_TYPE_FULL) {
            continue;
        } else {
            goto err;
        }
    }
    return cursor;
err:
    return -1;
}

char *rbmEncode(roaringBitmap* rbm, size_t *plen) {
    ssize_t len = 0;

    serverAssert(rbm && plen);

    if ((len = rbmEncode_(rbm, NULL, 0)) < 0) {
        *plen = 0;
        return NULL;
    }

    char* encoded = roaring_malloc(len);
    assert(rbmEncode_(rbm, encoded, len) != -1);

    *plen = len;
    return encoded;
}

roaringBitmap* rbmDecode(const char *buf, size_t len) {
    const char *cursor = buf;
    serverAssert(cursor != NULL && len > 0);
    roaringBitmap* rbm = rbmCreate();

    if (len < sizeof(uint8_t)) goto err;
    memcpy(&rbm->buckets_num,cursor,sizeof(uint8_t));
    cursor += sizeof(uint8_t), len -= sizeof(uint8_t);

    size_t buckets_len = rbm->buckets_num * sizeof(uint8_t);
    if (len < buckets_len) goto err;
    rbm->buckets = roaring_malloc(buckets_len);
    memcpy(rbm->buckets,cursor,buckets_len);
    cursor += buckets_len, len -= buckets_len;

    rbm->containers = roaring_calloc(rbm->buckets_num * sizeof(roaringContainer *));

    for (int i = 0; i< rbm->buckets_num; i++) {
        rbm->containers[i] = roaring_calloc(sizeof(roaringContainer));

        uint16_t elements_num = 0;
        if (len < sizeof(elements_num)) goto err;
        memcpy(&elements_num,cursor,sizeof(elements_num));
        elements_num = ntohs(elements_num);
        rbm->containers[i]->elements_num = elements_num;
        cursor += sizeof(elements_num), len -= sizeof(elements_num);

        uint8_t type = 0;
        if (len < sizeof(type)) goto err;
        memcpy(&type,cursor,sizeof(type));
        cursor += sizeof(type), len -= sizeof(type);
        rbm->containers[i]->type = type;

        if (type == CONTAINER_TYPE_ARRAY) {
            size_t array_size = sizeof(arrayContainer) * rbm->containers[i]->elements_num;
            if (len < array_size) goto err;
            rbm->containers[i]->a.array = roaring_malloc(array_size);
            rbm->containers[i]->a.capacity = rbm->containers[i]->elements_num;
            for(int j=0; j<rbm->containers[i]->elements_num; j++) {
                arrayContainer* array = (arrayContainer*)cursor;
                cursor += sizeof(arrayContainer);
                uint16_t value = *array;
                value = ntohs(value);
                rbm->containers[i]->a.array[j] = value;
            }
            len -= array_size;
        } else if (type == CONTAINER_TYPE_BITMAP) {
            if (len < BITMAP_CONTAINER_SIZE) goto err;
            rbm->containers[i]->b.bitmap = roaring_malloc(BITMAP_CONTAINER_SIZE);
            memcpy(rbm->containers[i]->b.bitmap, cursor, BITMAP_CONTAINER_SIZE);
            cursor += BITMAP_CONTAINER_SIZE, len -= BITMAP_CONTAINER_SIZE;
        } else if (type == CONTAINER_TYPE_FULL) {
            rbm->containers[i]->f.none = NULL;
        } else {
            goto err;
        }
    }

    if (len != 0) goto err;

    return rbm;

err:
    rbmDestory(rbm);
    return NULL;
}

#ifdef REDIS_TEST

int testAssertDecode(roaringBitmap* rbm, int* _error) {
    size_t len = 0;
    int error = *_error;
    char* encoded = NULL;
    roaringBitmap* decoded = NULL;
    encoded = rbmEncode(rbm, &len);
    decoded = rbmDecode(encoded, len);
    test_assert(rbmIsEqual(decoded, rbm) == 1);
    rbmDestory(decoded);
    roaring_free(encoded);
    *_error = error;
    return 0;
}

int roaringBitmapTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

        TEST("roaring-bitmap: set get") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* normal test */
            /* [0, 8] */
            rbmSetBitRange(rbm, 0, 8);

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            /* boundry test */
            bitNum = rbmGetBitRange(rbm, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 8, 200);
            test_assert(bitNum == 1);

            /* array container  */
            /* normal test */
            /* [0, 8]  [10, 200]*/
            rbmSetBitRange(rbm, 10, 200);
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 200);

            bitNum = rbmGetBitRange(rbm, 9, 9);  /* inside boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* across boundry */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 201, 201);  /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 196, 205); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 205); /* across set boundry */
            test_assert(bitNum == 191);

            rbmSetBitRange(rbm, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm, 200, 300);  /* boundry set */

            /* [0, 8]  [10, 300]*/
            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 300);

            /* Bitmap container  */
            /* [0, 8]  [10, 1000]*/
            rbmSetBitRange(rbm, 200, 1000);
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 1000);

            bitNum = rbmGetBitRange(rbm, 1001, 1001);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 996, 1005); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 1005); /* across set boundry */
            test_assert(bitNum == 991);

            rbmSetBitRange(rbm, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 1000);

            /* across container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2); /* container level test */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm, 4096 - 5, 4096 + 5); /* across container test */
            test_assert(bitNum == 11);

            /* across full Container set get */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]*/

            rbmSetBitRange(rbm, 4096 * 2 + 1000, 4096 * 2 + 2000);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            /* across empty container set get */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]  [4096 * 3 + 1000, 4096 * 3 + 2000]*/
            rbmSetBitRange(rbm, 4096 * 3 + 1000, 4096 * 3 + 2000); /* fill empty */

            bitNum = rbmGetBitRange(rbm, 4096 * 3, 4096 * 4 - 1); /* container level test */
            test_assert(bitNum == 1001);

            /* whole roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 128 - 1); /* container level test */
            test_assert(bitNum == 6294);

            rbmDestory(rbm);
        }

        TEST("roaring-bitmap: set get clear") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* normal test */
            rbmSetBitRange(rbm, 4, 8);   /* [4, 8] */

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 4, 4);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 8, 8);
            test_assert(bitNum == 1);

            /* boundry test */
            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 8, 200);
            test_assert(bitNum == 1);

            rbmClearBitRange(rbm, 6, 8);    /* [4, 5] */

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 2);

            bitNum = rbmGetBitRange(rbm, 3, 3);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 6, 6);
            test_assert(bitNum == 0);

            rbmClearBitRange(rbm, 0, 2);    /* [4, 5] */

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 2);


            /* array container  */
            /* normal test */
            rbmSetBitRange(rbm, 10, 200);  /* [4, 5]   [10 ,200] */
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 193);

            rbmClearBitRange(rbm, 0, 9);    /* [10 ,200] */

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* across boundry */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* beyond boundry */
            test_assert(bitNum == 0);

            rbmClearBitRange(rbm, 10, 99);    /* [100 ,200] */

            bitNum = rbmGetBitRange(rbm, 191, 210);  /* across boundry */
            test_assert(bitNum == 10);

            rbmClearBitRange(rbm, 191, 200);    /* [100 , 190] */

            bitNum = rbmGetBitRange(rbm, 181, 210);  /* across boundry */
            test_assert(bitNum == 10);

            rbmClearBitRange(rbm, 151, 159);    /*  [100 ,150]， [160, 190] */

            bitNum = rbmGetBitRange(rbm, 141, 169);  /* across boundry */
            test_assert(bitNum == 20);


            /* Bitmap container  */
            rbmSetBitRange(rbm, 200, 1000);   /*  100 ~150， 160 ~ 190 , 200 ~ 1000 */ /* array container to Bitmapcontainer */
            bitNum = rbmGetBitRange(rbm, 0, 1000);   /* between array container max capacity， Bitmap Container  max capacity */
            test_assert(bitNum == 883);

            bitNum = rbmGetBitRange(rbm, 1001, 1001); /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 165, 165);   /* inside boundry */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 160, 160);   /* inside boundry */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 220, 220);   /* inside boundry */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 159, 159);   /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 160, 220); /* across boundry */
            test_assert(bitNum == 52);

            /*  100 ~150， 160 ~ 190 , 301 ~ 1000 */
            rbmClearBitRange(rbm, 200, 300);    /* boundry outside clear */

            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 782);

            rbmClearBitRange(rbm, 501, 2000);  /* boundry clear */
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 282);

            rbmClearBitRange(rbm, 0, 129);   /* boundry clear */
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 252);

            rbmClearBitRange(rbm, 171, 180);  /* boundry inside Clear */
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 242);

            rbmSetBitRange(rbm, 501, 1000);
            rbmSetBitRange(rbm, 100, 129);
            rbmSetBitRange(rbm, 171, 180);

            /* across container */
            rbmSetBitRange(rbm, 200, 4096 * 2 + 1);   /*  100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 + 1 */
            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2 + 1);
            test_assert(bitNum == 8076);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1);
            test_assert(bitNum == 1);

              /*  100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 */
            rbmClearBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1); /* boundry clear */

            bitNum = rbmGetBitRange(rbm, 4096 * 2, 4096 * 2);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2);
            test_assert(bitNum == 8075);

               /*  100 ~150， 160 ~ 190 , 200 ~ 4096 */
            rbmClearBitRange(rbm, 4096 + 1, 4096 * 2); /* across  full Container clear */  /*  full Container clear boundry， trans to bitmap container */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2 + 1);
            test_assert(bitNum == 3979);

            bitNum = rbmGetBitRange(rbm, 4090, 4100);   /* across boundry */
            test_assert(bitNum == 7);

              /*  100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 - 1 */
            rbmSetBitRange(rbm, 4096 + 1, 4096 * 2 - 1); /*  full container */

            /*  100 ~150， 160 ~ 190 , 200 ~ 4096 + 9 ,  4096 + 4095 ~ 4096 + 4095 */
            rbmClearBitRange(rbm, 4096 + 10, 4096 + 4094); /* full container Clear， trans to  array container */

            bitNum = rbmGetBitRange(rbm, 4096 + 1, 4096 + 4095);
            test_assert(bitNum == 10);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 4);   /* container test */
            test_assert(bitNum == 3989);

            rbmClearBitRange(rbm, 4096 * 3, 4096 * 4 - 1000); /* clear empty container */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 4);   /* container test */
            test_assert(bitNum == 3989);

            rbmDestory(rbm);
        }

        TEST("roaring bitmap: set get clear operation in upper container location test") {
            roaringBitmap* rbm = rbmCreate();

            uint32_t bitNum = rbmGetBitRange(rbm, 0, 131071);  /*maxbit*/
            test_assert(bitNum == 0);

            /* setbit*/
            rbmSetBitRange(rbm, 131071, 131071);        /*  [131071, 131071] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);   /* boundry */
            test_assert(bitNum == 1);

            /* batch  setbit*/
            rbmSetBitRange(rbm, 131071 - 4096 - 1, 131070);    /*  [131071 - 4096 - 1, 131071] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 4098);

            rbmSetBitRange(rbm, 131071 - 4096 * 2, 131071 - 4096 - 2);   /*  [131071 - 4096 * 2, 131071] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);  /* batch getbit */
            test_assert(bitNum == 4096 * 2 + 1);

            bitNum = rbmGetBitRange(rbm, 130000, 130000); /* getbit */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 * 2, 131071 - 4096 * 2);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 * 2 - 1, 131071 - 4096 * 2 - 1);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 131071, 131071);   /* boundry */
            test_assert(bitNum == 1);

            /*  Clearbit */
            rbmClearBitRange(rbm, 131071, 131071);    /*  [131071 - 4096 * 2, 131070] */

            bitNum = rbmGetBitRange(rbm, 131071, 131071);  /*maxbit*/
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 4096 * 2);

            /* batch Clear, container of Bitmap to array */
            rbmClearBitRange(rbm, 131071 - 4096 - 4000, 131071 - 4096 - 1);   /*  [131071 - 4096 * 2, 131071 - 4096 - 4001], [131071 - 4096, 131070] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 4192);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 - 4001, 131071 - 4096 - 4001);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 - 4000, 131071 - 4096 - 4000);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096, 131071 - 4096);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 - 1, 131071 - 4096 -1);
            test_assert(bitNum == 0);

            rbmDestory(rbm);
        }

        TEST("roaring bitmap: full container test") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            rbmSetBitRange(rbm, 0, 100);  /* from low pos batch set */
            bitNum = rbmGetBitRange(rbm, 0, 4096);
            test_assert(bitNum == 101);

            rbmSetBitRange(rbm, 101, 101);  /* point set */
            bitNum = rbmGetBitRange(rbm, 0, 4096);
            test_assert(bitNum == 102);

            rbmSetBitRange(rbm, 102, 4000);  /* array container to Bitmap container */
            bitNum = rbmGetBitRange(rbm, 0, 4096);
            test_assert(bitNum == 4001);

            rbmSetBitRange(rbm, 4096, 4096 * 4 + 4090); /* 3 个 full container */

            /* full container get */
            bitNum = rbmGetBitRange(rbm, 4096 * 2, 4096 * 5 - 1);
            test_assert(bitNum == 4096 * 2 + 4091);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 + 100);
            test_assert(bitNum == 102);

            /* full container set */
            rbmSetBitRange(rbm, 4096, 4096 + 100);
            bitNum = rbmGetBitRange(rbm, 4096, 4096 * 2 - 1);
            test_assert(bitNum == 4096);

            rbmSetBitRange(rbm, 4096, 4096 * 2 - 1);
            bitNum = rbmGetBitRange(rbm, 4096, 4096 * 2 - 1);
            test_assert(bitNum == 4096);

            rbmClearBitRange(rbm, 4096, 4096); /*  point  Clear first full container */

            bitNum = rbmGetBitRange(rbm, 4096, 4096);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 4096 + 1, 4096 + 1);
            test_assert(bitNum == 1);

            rbmClearBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1); /*  point  Clear second full container */

            bitNum = rbmGetBitRange(rbm, 4096 * 2, 4096 * 2);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 2, 4096 * 2 + 2);
            test_assert(bitNum == 1);

            rbmClearBitRange(rbm, 4096 * 3 + 101, 4096 * 3 + 4000); /* batch  Clear third full container */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 + 100, 4096 * 3 + 100);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 4096 * 3 + 4000, 4096 * 3 + 4000);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 4 + 4090);
            test_assert(bitNum == 4001 + 4096 * 2 - 2 + 196 + 4091);

            rbmSetBitRange(rbm, 4096 * 4 + 4091, 4096 * 5 - 1); /* fourth full Container */

            rbmSetBitRange(rbm, 4096 * 4, 4096 * 4);
            bitNum = rbmGetBitRange(rbm, 4096 * 4, 4096 * 5 - 1);
            test_assert(bitNum == 4096);

            bitNum = rbmGetBitRange(rbm, 4096 * 4 + 100, 4096 * 4 + 200);
            test_assert(bitNum == 101);

            rbmDestory(rbm);
        }

        TEST("roaring bitmap: empty container") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            rbmSetBitRange(rbm, 100, 4095);
            rbmSetBitRange(rbm, 4096, 4096 + 100);
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);

            rbmClearBitRange(rbm, 4096, 4096 + 100); /* second container is empty */

            /* empty container get */
            bitNum = rbmGetBitRange(rbm, 4096, 4096 + 200);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 4096, 4096);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 4096 * 3 - 1);
            test_assert(bitNum == 3996 + 4096);

            /* empty container clear */
            rbmClearBitRange(rbm, 4096 + 100, 4096 + 200);

            bitNum = rbmGetBitRange(rbm, 4096, 4096 + 1000);
            test_assert(bitNum == 0);

            rbmClearBitRange(rbm, 4096 + 100, 4096 + 100);
            bitNum = rbmGetBitRange(rbm, 4096, 4096 + 1000);
            test_assert(bitNum == 0);

            /* empty container set */
            rbmSetBitRange(rbm, 4096 + 100, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 4096, 4096 + 1000);
            test_assert(bitNum == 1);

            rbmClearBitRange(rbm, 4096 + 100, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 4096, 4096 + 1000);
            test_assert(bitNum == 0);

            rbmSetBitRange(rbm, 4096 + 101, 4096 + 200);
            bitNum = rbmGetBitRange(rbm, 4096, 4096 + 1000);
            test_assert(bitNum == 100);

            rbmDestory(rbm);
        }

        TEST("roaring bitmap: insert delete bucket test") {
            roaringBitmap* rbm = rbmCreate();

            uint32_t bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 0);

            /*  point  set insert 3 Buckets */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 2);
            rbmSetBitRange(rbm, 4096 * 3, 4096 * 3);
            rbmSetBitRange(rbm, 4096 * 5 - 1, 4096 * 5 - 1);

            test_assert(rbm->buckets_num == 3);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 3);

            /* range set insert 3 Buckets */
            rbmSetBitRange(rbm, 4096 * 6, 4096 * 6 + 99);
            rbmSetBitRange(rbm, 4096 * 7, 4096 * 7 + 99);
            rbmSetBitRange(rbm, 4096 * 8, 4096 * 8 + 99);

            test_assert(rbm->buckets_num == 6);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 303);

            /*  set full container trigger inserting 3 Buckets */
            rbmSetBitRange(rbm, 4096 * 9, 4096 * 10 - 1);
            rbmSetBitRange(rbm, 4096 * 10, 4096 * 11 - 1);
            rbmSetBitRange(rbm, 4096 * 11, 4096 * 12 - 1);

            test_assert(rbm->buckets_num == 9);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 303 + 4096 * 3);

            /*  point del Bucket */
            rbmClearBitRange(rbm, 4096 * 9, 4096 * 10 - 1);
            rbmClearBitRange(rbm, 4096 * 6, 4096 * 6 + 99);
            rbmClearBitRange(rbm, 4096 * 2, 4096 * 2);

            test_assert(rbm->buckets_num == 6);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 202 + 4096 * 2);

            /* batch del Bucket */
            rbmClearBitRange(rbm, 4096 * 5, 4096 * 12 - 1);
            test_assert(rbm->buckets_num == 2);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 2);

            rbmDestory(rbm);
        }

        TEST("roaring-bitmap: set get clear getbitpos") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* normal test */
            rbmSetBitRange(rbm, 4, 8);   /* [4, 8] */

            uint32_t *idx_arr = zmalloc(sizeof(uint32_t) * CONTAINER_CAPACITY);

            bitNum = rbmLocateSetBitPos(rbm, 6, idx_arr);
            test_assert(bitNum == 5);
            test_assert(idx_arr[0] == 4);
            test_assert(idx_arr[1] == 5);
            test_assert(idx_arr[2] == 6);
            test_assert(idx_arr[3] == 7);
            test_assert(idx_arr[4] == 8);

            rbmClearBitRange(rbm, 6, 8);    /* [4, 5] */
            bitNum = rbmLocateSetBitPos(rbm, 6, idx_arr);
            test_assert(bitNum == 2);
            test_assert(idx_arr[0] == 4);
            test_assert(idx_arr[1] == 5);

            /* array container  */
            /* normal test */
            rbmSetBitRange(rbm, 10, 200);  /* [4, 5]   [10 ,200] */
            bitNum = rbmLocateSetBitPos(rbm, 100, idx_arr);
            test_assert(bitNum == 100);
            test_assert(idx_arr[0] == 4);
            test_assert(idx_arr[99] == 107);

            bitNum = rbmLocateSetBitPos(rbm, 200, idx_arr);
            test_assert(bitNum == 193);
            test_assert(idx_arr[0] == 4);
            test_assert(idx_arr[192] == 200);

            rbmClearBitRange(rbm, 0, 9);    /* [10 ,200] */

            rbmClearBitRange(rbm, 10, 99);    /* [100 ,200] */

            rbmClearBitRange(rbm, 191, 200);    /* [100 , 190] */

            rbmClearBitRange(rbm, 151, 159);    /*  [100 ,150]， [160, 190] */

            /* Bitmap container  */
            rbmSetBitRange(rbm, 200, 1000);   /*  100 ~ 150， 160 ~ 190 , 200 ~ 1000 */ /* array container to Bitmapcontainer */

            bitNum = rbmLocateSetBitPos(rbm, 1000, idx_arr);
            test_assert(bitNum == 883);
            test_assert(idx_arr[0] == 100);
            test_assert(idx_arr[882] == 1000);

            bitNum = rbmLocateSetBitPos(rbm, 800, idx_arr);
            test_assert(bitNum == 800);
            test_assert(idx_arr[0] == 100);
            test_assert(idx_arr[799] == 917);

            /*  100 ~150， 160 ~ 190 , 301 ~ 1000 */
            rbmClearBitRange(rbm, 200, 300);    /* boundry outside clear */


            rbmClearBitRange(rbm, 501, 2000);  /* boundry clear */

            rbmClearBitRange(rbm, 0, 129);   /* boundry clear */

            rbmClearBitRange(rbm, 171, 180);  /* boundry inside Clear */

            rbmSetBitRange(rbm, 501, 1000);
            rbmSetBitRange(rbm, 100, 129);
            rbmSetBitRange(rbm, 171, 180);

            /* across Container bitmap, full , array */
            rbmSetBitRange(rbm, 200, 4096 * 2 + 1);   /*  100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 + 1 */

            bitNum = rbmLocateSetBitPos(rbm, 4096 * 2 + 1, idx_arr);
            test_assert(bitNum == 8076);
            test_assert(idx_arr[0] == 100);
            test_assert(idx_arr[8075] == 4096 * 2 + 1);

              /*  100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 */
            rbmClearBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1); /* boundry clear */

             /*  100 ~150， 160 ~ 190 , 200 ~ 4096 */
            rbmClearBitRange(rbm, 4096 + 1, 4096 * 2); /* across  full Container clear */  /*  full Container clear boundry， trans to bitmap container */

              /*  100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 - 1 */
            rbmSetBitRange(rbm, 4096 + 1, 4096 * 2 - 1); /*  full container */

            /*  full container, mid of container */
            bitNum = rbmLocateSetBitPos(rbm, 4096, idx_arr);
            test_assert(bitNum == 4096);
            test_assert(idx_arr[0] == 100);
            test_assert(idx_arr[4095] == 4213);

            /*  full container, end of container */
            bitNum = rbmLocateSetBitPos(rbm, 8074, idx_arr);
            test_assert(bitNum == 8074);
            test_assert(idx_arr[0] == 100);
            test_assert(idx_arr[8073] == 4096 * 2 - 1);

            zfree(idx_arr);
            rbmDestory(rbm);
        }

        TEST("roaring-bitmap: set get duplicate test") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* normal test */
            /* [0, 8] */
            rbmSetBitRange(rbm, 0, 8);

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            /* boundry test */
            bitNum = rbmGetBitRange(rbm, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 8, 200);
            test_assert(bitNum == 1);

            roaringBitmap* rbm1 = rbmCreate();
            rbmdup(rbm1, rbm);

            bitNum = rbmGetBitRange(rbm1, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm1, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm1, 9, 9);
            test_assert(bitNum == 0);

            /* boundry test */
            bitNum = rbmGetBitRange(rbm1, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm1, 8, 200);
            test_assert(bitNum == 1);

            rbmDestory(rbm1);

            /* array container  */
            /* normal test */
            /* [0, 8]  [10, 200]*/
            rbmSetBitRange(rbm, 10, 200);
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 200);

            bitNum = rbmGetBitRange(rbm, 9, 9);  /* inside boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* across boundry */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 201, 201);  /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 196, 205); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 205); /* across set boundry */
            test_assert(bitNum == 191);

            roaringBitmap* rbm2 = rbmCreate();
            rbmdup(rbm2, rbm);

            bitNum = rbmGetBitRange(rbm2, 9, 9);  /* inside boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm2, 100, 300);  /* across boundry */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm2, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm2, 201, 400); /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 201, 201);  /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 196, 205); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm2, 9, 205); /* across set boundry */
            test_assert(bitNum == 191);

            /* non-in-place modification rbm */
            rbmSetBitRange(rbm, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm, 200, 300);  /* boundry set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 300);

            /* non-in-place modification rbm2 */
            rbmSetBitRange(rbm2, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container level test */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm2, 200, 300);  /* boundry set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container level test */
            test_assert(bitNum == 300);

            rbmDestory(rbm2);

            /* Bitmap container  */
            /* [0, 8]  [10, 1000] */
            rbmSetBitRange(rbm, 200, 1000);

            roaringBitmap* rbm3 = rbmCreate();
            rbmdup(rbm3, rbm);

            bitNum = rbmGetBitRange(rbm3, 0, 1000);
            test_assert(bitNum == 1000);

            bitNum = rbmGetBitRange(rbm3, 1001, 1001);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm3, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm3, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm3, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm3, 996, 1005); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm3, 9, 1005); /* across set boundry */
            test_assert(bitNum == 991);

            /* non-in-place modificationrbm */
            rbmSetBitRange(rbm, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 1000);

            /* non-in-place modificationrbm3 */
            rbmSetBitRange(rbm3, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm3, 0, 4095); /* container level test */
            test_assert(bitNum == 1000);

            /* rbm across container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2); /* container level test */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm, 4096 - 5, 4096 + 5); /* across container test */
            test_assert(bitNum == 11);

            /* rbm3 across container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm3, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm3, 0, 4096 * 2); /* container level test */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm3, 4096 - 5, 4096 + 5); /* across container test */
            test_assert(bitNum == 11);

            rbmDestory(rbm3);

            /* across full Container set get */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1] */

            rbmSetBitRange(rbm, 4096 * 2 + 1000, 4096 * 2 + 2000);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            roaringBitmap* rbm4 = rbmCreate();
            rbmdup(rbm4, rbm);

            bitNum = rbmGetBitRange(rbm4, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            /* across empty container set get */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* across empty container set get */
            bitNum = rbmGetBitRange(rbm4, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            rbmDestory(rbm4);

            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]  [4096 * 3 + 1000, 4096 * 3 + 2000]*/
            rbmSetBitRange(rbm, 4096 * 3 + 1000, 4096 * 3 + 2000); /* fill empty */

            bitNum = rbmGetBitRange(rbm, 4096 * 3, 4096 * 4 - 1); /* container level test */
            test_assert(bitNum == 1001);

            /* whole roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 128 - 1); /* container level test */
            test_assert(bitNum == 6294);

            roaringBitmap* rbm5 = rbmCreate();
            rbmdup(rbm5, rbm);

            bitNum = rbmGetBitRange(rbm5, 4096 * 3, 4096 * 4 - 1); /* container level test */
            test_assert(bitNum == 1001);

            /* whole roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm5, 0, 4096 * 128 - 1); /* container level test */
            test_assert(bitNum == 6294);

            rbmDestory(rbm);
            rbmDestory(rbm5);
        }

        TEST("roaring-bitmap: set get duplicate isEqual test") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* normal test */
            /* [0, 8] */
            rbmSetBitRange(rbm, 0, 8);

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            /* boundry test */
            bitNum = rbmGetBitRange(rbm, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 8, 200);
            test_assert(bitNum == 1);

            roaringBitmap* rbm1 = rbmCreate();
            rbmdup(rbm1, rbm);

            bitNum = rbmGetBitRange(rbm1, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm1, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm1, 9, 9);
            test_assert(bitNum == 0);

            /* boundry test */
            bitNum = rbmGetBitRange(rbm1, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm1, 8, 200);
            test_assert(bitNum == 1);

            test_assert(1 == rbmIsEqual(rbm1, rbm));

            rbmSetBitRange(rbm1, 9, 9);

            test_assert(0 == rbmIsEqual(rbm1, rbm));
            rbmDestory(rbm1);

            /* array container  */
            /* normal test */
            /* [0, 8]  [10, 200]*/
            rbmSetBitRange(rbm, 10, 200);
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 200);

            bitNum = rbmGetBitRange(rbm, 9, 9);  /* inside boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* across boundry */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 201, 201);  /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 196, 205); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 205); /* across set boundry */
            test_assert(bitNum == 191);

            roaringBitmap* rbm2 = rbmCreate();
            rbmdup(rbm2, rbm);

            bitNum = rbmGetBitRange(rbm2, 9, 9);  /* inside boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm2, 100, 300);  /* across boundry */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm2, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm2, 201, 400); /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 201, 201);  /* beyond boundry */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 196, 205); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm2, 9, 205); /* across set boundry */
            test_assert(bitNum == 191);

            test_assert(1 == rbmIsEqual(rbm2, rbm));

            /* non-in-place modification rbm */
            rbmSetBitRange(rbm, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm, 200, 300);  /* boundry set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 300);

            test_assert(0 == rbmIsEqual(rbm2, rbm));

            /* non-in-place modification rbm2 */
            rbmSetBitRange(rbm2, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container level test */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm2, 200, 300);  /* boundry set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container level test */
            test_assert(bitNum == 300);

            test_assert(1 == rbmIsEqual(rbm2, rbm));
            rbmDestory(rbm2);

            /* Bitmap container  */
            /* [0, 8]  [10, 1000] */
            rbmSetBitRange(rbm, 200, 1000);

            roaringBitmap* rbm3 = rbmCreate();
            rbmdup(rbm3, rbm);

            bitNum = rbmGetBitRange(rbm3, 0, 1000);
            test_assert(bitNum == 1000);

            bitNum = rbmGetBitRange(rbm3, 1001, 1001);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm3, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm3, 100, 150);  /* inside boundry */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm3, 8, 100);  /* across boundry */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm3, 996, 1005); /* across boundry */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm3, 9, 1005); /* across set boundry */
            test_assert(bitNum == 991);

            test_assert(1 == rbmIsEqual(rbm3, rbm));

            /* non-in-place modificationrbm */
            rbmSetBitRange(rbm, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container level test */
            test_assert(bitNum == 1000);

            test_assert(1 == rbmIsEqual(rbm3, rbm));

            /* non-in-place modificationrbm3 */
            rbmSetBitRange(rbm3, 150, 160);  /* repeat set */

            bitNum = rbmGetBitRange(rbm3, 0, 4095); /* container level test */
            test_assert(bitNum == 1000);

            test_assert(1 == rbmIsEqual(rbm3, rbm));

            /* rbm across container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2); /* container level test */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm, 4096 - 5, 4096 + 5); /* across container test */
            test_assert(bitNum == 11);

            test_assert(0 == rbmIsEqual(rbm3, rbm));

            /* rbm3 across container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm3, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm3, 0, 4096 * 2); /* container level test */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm3, 4096 - 5, 4096 + 5); /* across container test */
            test_assert(bitNum == 11);

            test_assert(1 == rbmIsEqual(rbm3, rbm));
            rbmDestory(rbm3);

            /* across full Container set get */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1] */

            rbmSetBitRange(rbm, 4096 * 2 + 1000, 4096 * 2 + 2000);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            roaringBitmap* rbm4 = rbmCreate();
            rbmdup(rbm4, rbm);

            bitNum = rbmGetBitRange(rbm4, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            /* across empty container set get */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* across empty container set get */
            bitNum = rbmGetBitRange(rbm4, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]  [4096 * 3 + 1000, 4096 * 3 + 2000]*/
            rbmSetBitRange(rbm, 4096 * 3 + 1000, 4096 * 3 + 2000); /* fill empty */

            test_assert(0 == rbmIsEqual(rbm4, rbm));

            bitNum = rbmGetBitRange(rbm, 4096 * 3, 4096 * 4 - 1); /* container level test */
            test_assert(bitNum == 1001);

            /* whole roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 128 - 1); /* container level test */
            test_assert(bitNum == 6294);

            rbmDestory(rbm4);

            roaringBitmap* rbm5 = rbmCreate();
            rbmdup(rbm5, rbm);

            bitNum = rbmGetBitRange(rbm5, 4096 * 3, 4096 * 4 - 1); /* container level test */
            test_assert(bitNum == 1001);

            /* whole roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm5, 0, 4096 * 128 - 1); /* container level test */
            test_assert(bitNum == 6294);

            test_assert(1 == rbmIsEqual(rbm5, rbm));

            rbmDestory(rbm);
            rbmDestory(rbm5);
        }

            /* first is  array， second is Bitmap，  third is  empty,  fourth is full */
        TEST("roaring bitmap: container save 14bits") {

            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            rbmSetBitRange(rbm, 101, 200);
            rbmSetBitRange(rbm, 16384 + 101, 16384 + 16383);
            rbmSetBitRange(rbm, 16384 * 3, 16384 * 4 - 2);

            rbmSetBitRange(rbm, 100, 100);
            rbmSetBitRange(rbm, 16384 + 100, 16384 + 100);
            rbmSetBitRange(rbm, 16384 * 4 - 1, 16384 * 4 - 1);



            bitNum = rbmGetBitRange(rbm, 10, 120);
            test_assert(bitNum == 21);

            bitNum = rbmGetBitRange(rbm, 16384, 16384 + 199);
            test_assert(bitNum == 100);

            bitNum = rbmGetBitRange(rbm, 16384 * 2, 16384 * 2 + 10);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 16384 * 3, 16384 * 3 + 100);
            test_assert(bitNum == 101);

            rbmClearBitRange(rbm, 100, 100);

            rbmClearBitRange(rbm, 16384 + 100, 16384 + 100);

            rbmClearBitRange(rbm, 16384 * 2 + 300, 16384 * 2 + 400);
            rbmClearBitRange(rbm, 16384 * 3 + 50, 16384 * 3 + 100);


            bitNum = rbmGetBitRange(rbm, 10, 120);
            test_assert(bitNum == 20);

            bitNum = rbmGetBitRange(rbm, 16384, 16384 + 199);
            test_assert(bitNum == 99);

            bitNum = rbmGetBitRange(rbm, 16384 * 2, 16384 * 2 + 10);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 16384 * 3, 16384 * 3 + 100);
            test_assert(bitNum == 50);
            rbmDestory(rbm);
        }

        /* first is array， second is Bitmap，  third is  empty,  fourth is full */
        
        TEST("roaring bitmap: container save 10 bits") {

            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            rbmSetBitRange(rbm, 100, 199);
            rbmSetBitRange(rbm, 1024, 1024 + 922);
            rbmSetBitRange(rbm, 1024 * 3, 1024 * 4 - 2);

            rbmSetBitRange(rbm, 200, 200);
            rbmSetBitRange(rbm, 1024 + 923, 1024 + 923);
            rbmSetBitRange(rbm, 1024 * 4 - 1, 1024 * 4 - 1);


            bitNum = rbmGetBitRange(rbm, 180, 300);
            test_assert(bitNum == 21);

            bitNum = rbmGetBitRange(rbm, 1024 + 823, 1024 + 1023);
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 1024 * 2 + 500, 1024 * 2 + 600);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 1024 * 3 + 923, 1024 * 3 + 1023);
            test_assert(bitNum == 101);



            rbmClearBitRange(rbm, 200, 200);

            rbmClearBitRange(rbm, 1024 + 923, 1024 + 923);

            rbmClearBitRange(rbm, 1024 * 2 + 500, 1024 * 2 + 700);
            rbmClearBitRange(rbm, 1024 * 3 + 1023, 1024 * 3 + 1023);


            bitNum = rbmGetBitRange(rbm, 180, 300);
            test_assert(bitNum == 20);

            bitNum = rbmGetBitRange(rbm, 1024 + 823, 1024 + 1023);
            test_assert(bitNum == 100);

            bitNum = rbmGetBitRange(rbm, 1024 * 2 + 500, 1024 * 2 + 600);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 1024 * 3 + 923, 1024 * 3 + 1023);
            test_assert(bitNum == 100);
            rbmDestory(rbm);

        }

        /* 500W QPS: [set]: 1/3 [get]: 2/3  total time = 1410622us  */

        TEST("roaring bitmap: perf") {
            size_t querytimes = 500000;

            long long start = ustime();
            roaringBitmap* rbm = rbmCreate();
            uint32_t maxBitNum = 131072;

            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;

                uint32_t bitNum = rbmGetBitRange(rbm, bit_idx, bit_idx);
                UNUSED(bitNum);
                rbmSetBitRange(rbm, bit_idx, bit_idx);
                bitNum = rbmGetBitRange(rbm, 0, bit_idx);

                if (bit_idx == maxBitNum - 1) {
                    rbmClearBitRange(rbm, 0, bit_idx / 2);
                }

            }
            printf("[bitmap set get]: %lld\n", ustime() - start);

            rbmDestory(rbm);
        }

        /* Performance test, TIME/OP (ns)：
            [bitmap single set]: 48
            [bitmap single get]: 32
            [bitmap range get]: 118
            [bitmap single clear]: 13
            [bitmap range set]: 179
            [bitmap range clear]: 74 */

        TEST("roaring bitmap: single api perf") {
            size_t querytimes = 500000;

            long long start = ustime();
            roaringBitmap* rbm = rbmCreate();
            uint32_t maxBitNum = 131072;

            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;
                rbmSetBitRange(rbm, bit_idx, bit_idx);
            }
            printf("[bitmap single set]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;

                uint32_t bitNum = rbmGetBitRange(rbm, bit_idx, bit_idx);
                UNUSED(bitNum);
            }
            printf("[bitmap single get]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;

                uint32_t bitNum = rbmGetBitRange(rbm, 0, bit_idx);
                UNUSED(bitNum);
            }
            printf("[bitmap range get]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;

                rbmClearBitRange(rbm, bit_idx, bit_idx);
            }
            printf("[bitmap single clear]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;
                rbmSetBitRange(rbm, 0, bit_idx);
            }
            printf("[bitmap range set]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bit_idx = i % maxBitNum;

                rbmClearBitRange(rbm, 0, bit_idx);
            }
            printf("[bitmap range clear]: %lld\n", (ustime() - start) / 500);
            rbmDestory(rbm);
        }

        TEST("roaring bitmap: serialize and deserialize") {
            roaringBitmap* rbm = rbmCreate();
            size_t len = 0;
            char* encoded = NULL;
            roaringBitmap* decoded = NULL;

            rbmSetBitRange(rbm, 100, 4095);
            rbmSetBitRange(rbm, 4096, 4096 + 100);
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);

            test_assert(rbm->containers[0]->type == CONTAINER_TYPE_BITMAP);
            test_assert(rbm->containers[1]->type == CONTAINER_TYPE_ARRAY);
            test_assert(rbm->containers[2]->type == CONTAINER_TYPE_FULL);

            testAssertDecode(rbm, &error);

            encoded = rbmEncode(rbm, &len);
            len -= 1;
            decoded = rbmDecode(encoded, len);
            test_assert(decoded == NULL);
            len += 2;
            decoded = rbmDecode(encoded, len);
            test_assert(decoded == NULL);
            len = 0;
            roaring_free(encoded);
            encoded = NULL;

            uint8_t CONTAINER_TYPE_ERROR = 3;
            rbm->containers[0]->type = CONTAINER_TYPE_ERROR;
            encoded = rbmEncode(rbm, &len);
            test_assert(encoded == NULL);
            test_assert(len == 0);
            rbmDestory(decoded);

            rbm->containers[1]->type = CONTAINER_TYPE_ERROR;
            encoded = rbmEncode(rbm, &len);
            test_assert(encoded == NULL);
            test_assert(len == 0);
            rbmDestory(decoded);

            rbm->containers[2]->type = CONTAINER_TYPE_ERROR;
            encoded = rbmEncode(rbm, &len);
            test_assert(encoded == NULL);
            test_assert(len == 0);
            rbmDestory(decoded);

            rbm->containers[0]->type = CONTAINER_TYPE_BITMAP;
            rbm->containers[1]->type = CONTAINER_TYPE_ARRAY;
            rbm->containers[2]->type = CONTAINER_TYPE_FULL;

            rbmDestory(rbm);
            rbm = NULL;
        }

        return error;
}

#endif
