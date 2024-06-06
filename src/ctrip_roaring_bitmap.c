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
    uint16_t elementsNum;
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
    uint8_t bucketsNum;
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

static inline uint8_t binarySearchLocUint8(const uint8_t *arr, uint8_t arrSize, uint8_t target)
{
    uint8_t leftIdx = 0;
    uint8_t rightIdx = arrSize;
    while (leftIdx < rightIdx) {
        uint8_t mid = leftIdx + ((rightIdx - leftIdx) >> 1);
        if (arr[mid] == target) {
            return mid;
        } else if (arr[mid] > target) {
            rightIdx = mid;
        } else {
            leftIdx = mid + 1;
        }
    }
    return leftIdx; /* if no target, loc should be here */
}

static inline uint16_t binarySearchLocUint16(const uint16_t *arr, uint16_t arrSize, uint16_t target)
{
    uint16_t leftIdx = 0;
    uint16_t rightIdx = arrSize;
    while (leftIdx < rightIdx) {
        uint16_t mid = leftIdx + ((rightIdx - leftIdx) >> 1);
        if (arr[mid] == target) {
            return mid;
        } else if (arr[mid] > target) {
            rightIdx = mid;
        } else {
            leftIdx = mid + 1;
        }
    }
    return leftIdx;  /* if no target, loc should be here */
}

static inline void bitmapSetbit(uint8_t *bitmap, uint16_t val)
{
    const uint8_t old_word = bitmap[val >> 3]; /* find the byte */
    const int bitIndex = val & MOD_8_MASK; /* find the bit index in byte */
    const uint8_t new_word = old_word | (1 << bitIndex);
    bitmap[val >> 3] = new_word;
}

static inline uint8_t bitmapCheckBitStatus(const uint8_t *bitmap, uint16_t val)
{
    const uint8_t old_word = bitmap[val >> 3]; /* find the byte */
    const int bitIndex = val & MOD_8_MASK; /* find the bit index in byte */
    if ((old_word & (1 << bitIndex)) != 0) {
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

static uint32_t bitmapCountBits(uint8_t *bmp, uint32_t startIdx, uint32_t endIdx)
{
    uint8_t *p = bmp + startIdx;
    uint32_t bitsNum = 0;
    uint32_t bytesNum = endIdx - startIdx + 1;
    while (bytesNum & 3) {
        bitsNum += bitsNumTable[*p++];
        bytesNum--;
    }

    /* left bytesNum is 4 * n */
    while (bytesNum) {
        bitsNum += countUint32Bits(*(uint32_t *)p);
        p += 4;
        bytesNum -= 4;
    }
    return bitsNum;
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
    container->elementsNum = 0;
    container->type = CONTAINER_TYPE_ARRAY;
}

static inline void expandArrIfNeed(roaringContainer *container, uint16_t newNum)
{
    if (container->a.capacity >= newNum) {
        return;
    }

    uint32_t newCapacity = newNum * ARRAY_CONTAINER_EXPAND_SPEED;

    container->a.capacity = MIN(newCapacity, ARRAY_CONTAINER_CAPACITY);
    container->a.array = roaring_realloc(container->a.array, sizeof(arrayContainer) * container->a.capacity);
}

static inline void shrinkArrIfNeed(roaringContainer *container)
{
    if (container->elementsNum == 0 || container->a.capacity <= container->elementsNum * ARRAY_CONTAINER_EXPAND_SPEED) {
        return;
    }
    uint32_t newCapacity = container->elementsNum * ARRAY_CONTAINER_EXPAND_SPEED;
    arrayContainer *newArr = roaring_malloc(newCapacity * sizeof(arrayContainer));
    memcpy(newArr, container->a.array, container->elementsNum * sizeof(arrayContainer));

    roaring_free(container->a.array);
    container->a.array = newArr;
    container->a.capacity = newCapacity;
}

 static inline void transArrayToBitmapContainer(roaringContainer *container)
{
    arrayContainer *oldArr = container->a.array;
    bitmapContainer *newBmp = roaring_calloc(BITMAP_CONTAINER_SIZE);
    for (int i = 0; i < container->elementsNum; i++) {
        bitmapSetbit(newBmp, oldArr[i]);
    }
    roaring_free(container->a.array);
    container->b.bitmap = newBmp;
    container->type = CONTAINER_TYPE_BITMAP;
}

static inline void transBitmapToArrayContainer(roaringContainer *container)
{
    bitmapContainer *bmp = container->b.bitmap;

    arrayContainer *newArr = roaring_calloc(container->elementsNum * sizeof(arrayContainer));
    uint32_t cursor = 0;
    for (uint32_t i = 0; i < BITMAP_CONTAINER_CAPACITY; i++) {
        if (bitmapCheckBitStatus(bmp, i)) {
            newArr[cursor++] = i;
        }
    }
    roaring_free(container->b.bitmap);
    container->a.capacity = container->elementsNum;
    container->a.array = newArr;
    container->type = CONTAINER_TYPE_ARRAY;
}

static inline void transToFullContainer(roaringContainer *container)
{
    clearContainer(container);
    container->type = CONTAINER_TYPE_FULL;
    container->f.none = NULL;
    container->elementsNum = CONTAINER_CAPACITY;
}

/* container set api */

static void bitmapContainerSetSingleBit(roaringContainer *container, uint16_t val)
{
    if (container->elementsNum <= ARRAY_CONTAINER_CAPACITY) {
        transArrayToBitmapContainer(container);
    }

    bitmapContainer *bitmap = container->b.bitmap;

    const uint8_t old_word = bitmap[val >> 3];
    const int bitIndex = val & MOD_8_MASK;
    const uint8_t new_word = old_word | (1 << bitIndex);
    bitmap[val >> 3] = new_word;
    container->elementsNum += (new_word ^ old_word) >> bitIndex;
}

static void bitmapContainerSetBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    if (minVal == maxVal) {
        bitmapContainerSetSingleBit(container, minVal);
        return;
    }

    if (container->elementsNum <= ARRAY_CONTAINER_CAPACITY) {
        transArrayToBitmapContainer(container);
    }

    bitmapContainer *bitmap = container->b.bitmap;

    /* assuming minVal is first bit in byte, maxVal is last bit in byte */
    uint16_t firstFullByteIdx = minVal >> 3;
    uint16_t lastFullByteIdx = maxVal >> 3;

    /* minVal maxVal at the same byte */
    if (firstFullByteIdx == lastFullByteIdx) {
        uint8_t *byte = bitmap + firstFullByteIdx;
        uint8_t addBitsNum = maxVal - minVal + 1;
        uint8_t bitIdx = minVal & MOD_8_MASK;

        uint8_t oldBitsNum = bitsNumTable[*byte & (((1 << addBitsNum) - 1) << bitIdx)]; /* n bits in the mid */
        *byte |= ((1 << addBitsNum) - 1) << bitIdx;
        container->elementsNum += (addBitsNum - oldBitsNum);
        return;
    }

    /* minVal maxVal at different bytes
    if minVal is not first bit in byte */
    if (minVal > firstFullByteIdx << 3) {
        uint8_t *firstByte = bitmap + firstFullByteIdx;
        uint8_t addBitsNum = ((firstFullByteIdx + 1) << 3) - minVal;  /* upper n bits */
        uint8_t oldBitsNum = bitsNumTable[*firstByte & ~((1 << (8 - addBitsNum)) - 1)];

        *firstByte |= ~((1 << (8 - addBitsNum)) - 1);
        container->elementsNum += (addBitsNum - oldBitsNum);
        firstFullByteIdx++;
    }

    /* maxVal is not last bit in byte */
    if (maxVal < ((lastFullByteIdx + 1) << 3) - 1) {
        uint8_t *lastByte = bitmap + lastFullByteIdx;
        uint8_t addBitsNum = maxVal - (lastFullByteIdx << 3) + 1;
        uint8_t oldBitsNum = bitsNumTable[*lastByte & ((1 << addBitsNum) - 1)];  /* lower n bits */

        *lastByte |= (1 << addBitsNum) - 1;
        container->elementsNum += (addBitsNum - oldBitsNum);
        lastFullByteIdx--;
    }

    if (firstFullByteIdx <= lastFullByteIdx) {
        uint32_t oldBitNum = bitmapCountBits(bitmap, firstFullByteIdx, lastFullByteIdx);
        memset(bitmap + firstFullByteIdx, 0xffU, lastFullByteIdx - firstFullByteIdx + 1);
        uint32_t newbitNum = (lastFullByteIdx - firstFullByteIdx + 1) << 3;
        container->elementsNum += (newbitNum - oldBitNum);
    }
}

static void arrayContainerRebuildInterval(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    uint32_t newElementsNum = maxVal - minVal + 1;

    if (newElementsNum > ARRAY_CONTAINER_CAPACITY) {
        clearContainer(container);
        bitmapContainerSetBit(container, minVal, maxVal);
        return;
    }

    expandArrIfNeed(container, newElementsNum);

    arrayContainer *arr = container->a.array;
    for (uint32_t i = 0; i < newElementsNum; i++) {
        arr[i] = minVal + i;
    }
    container->elementsNum = newElementsNum;
}

static void arrayContainerInsertInterval(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
     /* insert [minVal, maxVal], if minVal or maxVal alreadly exists, we will rewrite */
    arrayContainer *arr = container->a.array;

    uint16_t leftLoc = binarySearchLocUint16(arr, container->elementsNum, minVal); /* leftLoc is not in perserved */
    uint16_t rightLoc = binarySearchLocUint16(arr, container->elementsNum, maxVal + 1); /* rightLoc is perserved */

    if (leftLoc != container->elementsNum && rightLoc != 0 &&
        arr[leftLoc] == minVal && arr[rightLoc - 1] == maxVal && rightLoc - leftLoc - 1 == maxVal - minVal) {
        /* whole interval alreadly exists */
        return;
    }

    uint32_t leftPerservedNum = leftLoc;
    uint32_t rightPerservedNum = container->elementsNum - rightLoc;
    uint32_t insertNum = maxVal - minVal + 1;
    uint32_t newNum = leftPerservedNum + insertNum + rightPerservedNum; /* is impossible zero */

    if (newNum > ARRAY_CONTAINER_CAPACITY) {
        bitmapContainerSetBit(container, minVal, maxVal);
        return;
    }

    expandArrIfNeed(container, newNum);
    arrayContainer *newArr = container->a.array;

    if (rightPerservedNum) {
        memmove(newArr + leftPerservedNum + insertNum, newArr + leftPerservedNum, sizeof(arrayContainer) * rightPerservedNum);
    }
    for (uint32_t i = 0; i < insertNum; i++) {
        newArr[leftPerservedNum + i] = minVal + i;
    }

    container->elementsNum = newNum;
}

static void arrayContainerSetSingleBit(roaringContainer *container, uint16_t val)
{
    arrayContainer *arr = container->a.array;

    uint16_t loc = binarySearchLocUint16(arr, container->elementsNum, val);
    if (loc < container->elementsNum && arr[loc] == val) {
        return;
    }
    if (container->elementsNum + 1 > ARRAY_CONTAINER_CAPACITY) {
        bitmapContainerSetSingleBit(container, val);
        return;
    }

    expandArrIfNeed(container, container->elementsNum + 1);
    arr = container->a.array;

    /* append mode */
    if (container->elementsNum == 0 || arr[container->elementsNum - 1] < val) {
        arr[container->elementsNum++] = val;
    } else {
        /* insert mode */
        if (loc != container->elementsNum) {
            memmove(arr + loc + 1, arr + loc, (container->elementsNum - loc) * sizeof(arrayContainer));
        }
        arr[loc] = val;
        container->elementsNum++;
    }
}

static void arrayContainerSetBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    if (minVal == maxVal) {
        arrayContainerSetSingleBit(container, minVal);
        return;
    }
    if (container->elementsNum == 0) {
        arrayContainerRebuildInterval(container, minVal, maxVal);
        return;
    }
    arrayContainerInsertInterval(container, minVal, maxVal);
}

static void containerSetBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    if (maxVal - minVal + 1 == CONTAINER_CAPACITY) {
        transToFullContainer(container);
        return;
    }
    if (container->type == CONTAINER_TYPE_FULL) {
        return;
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        bitmapContainerSetBit(container, minVal, maxVal);
    } else {
        arrayContainerSetBit(container, minVal, maxVal);
    }
    if (container->elementsNum == CONTAINER_CAPACITY) {
        transToFullContainer(container);
    }
}

 /* container clear api */

static void fullContainerClearBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    clearContainer(container);
    if (minVal != 0) {
        containerSetBit(container, 0, minVal - 1);
    }
    if (maxVal != CONTAINER_CAPACITY - 1) {
        containerSetBit(container, maxVal + 1, CONTAINER_CAPACITY - 1);
    }
}

static void bitmapContainerClearSingleBit(roaringContainer *container, uint16_t val)
{
    bitmapContainer *bitmap = container->b.bitmap;

    const uint8_t old_word = bitmap[val >> 3];
    const int bitIndex = val & MOD_8_MASK;
    const uint8_t new_word = old_word & ~(1 << bitIndex);
    bitmap[val >> 3] = new_word;
    container->elementsNum -= (new_word ^ old_word) >> bitIndex;
    if (container->elementsNum <= ARRAY_CONTAINER_CAPACITY) {
        transBitmapToArrayContainer(container);
    }
}

static void bitmapContainerClearBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    if (minVal == maxVal) {
        bitmapContainerClearSingleBit(container, minVal);
        return;
    }
    bitmapContainer *bitmap = container->b.bitmap;

    /* assuming minVal is first bit in byte, maxVal is last bit in byte */
    uint16_t firstFullByteIdx = minVal >> 3;
    uint16_t lastFullByteIdx = maxVal >> 3;

    if (firstFullByteIdx == lastFullByteIdx) {
        uint8_t *byte = bitmap + firstFullByteIdx;
        uint32_t clearBitNum = maxVal - minVal + 1;
        uint8_t bitIdx = minVal & MOD_8_MASK;

        uint32_t oldBitNums = bitsNumTable[*byte & (((1 << clearBitNum) - 1) << bitIdx)]; /* mid n bits */
        *byte &= ~(((1 << clearBitNum) - 1) << bitIdx);
        container->elementsNum -= oldBitNums;
        return;
    }

    /* minVal maxVal at different bytes */
    /* if minVal is not first bit in byte */
    if (minVal > firstFullByteIdx << 3) {
        uint8_t *firstByte = bitmap + (minVal >> 3);
        uint8_t clearBitsNum = ((firstFullByteIdx + 1) << 3) - minVal; /* clear the upper n bits */
        uint8_t oldBitsNum = bitsNumTable[*firstByte & ~((1 << (8 - clearBitsNum)) - 1)];

        *firstByte &= (1 << (8 - clearBitsNum)) - 1;
        container->elementsNum -= oldBitsNum;

        firstFullByteIdx++;
    }

    /* maxVal is not last bit in byte*/
    if (maxVal < ((lastFullByteIdx + 1) << 3) - 1) {
        uint8_t *lastByte = bitmap + (maxVal >> 3);
        uint8_t clearBitsNum = maxVal - (lastFullByteIdx << 3) + 1; /* clear the lower bits */

        uint8_t oldBitsNum = bitsNumTable[*lastByte & ((1 << clearBitsNum) - 1)];
        *lastByte &= ~((1 << clearBitsNum) - 1);
        container->elementsNum -= oldBitsNum;
        lastFullByteIdx--;
    }

    if (firstFullByteIdx <= lastFullByteIdx) {
        uint32_t oldBitNum = bitmapCountBits(bitmap, firstFullByteIdx, lastFullByteIdx);
        memset(bitmap + firstFullByteIdx, 0, lastFullByteIdx - firstFullByteIdx + 1);
        container->elementsNum -= oldBitNum;
    }
    if (container->elementsNum <= ARRAY_CONTAINER_CAPACITY) {
        transBitmapToArrayContainer(container);
    }
}

static void arrayContainerClearInterval(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    arrayContainer *arr = container->a.array;

    uint16_t rightLoc = binarySearchLocUint16(arr, container->elementsNum, maxVal + 1); /* rightLoc is perserved */
    uint16_t leftLoc = binarySearchLocUint16(arr, container->elementsNum, minVal); /* leftLoc is not perserved */

    /* leftPerservedNum , rightperservedNum are impossible both zero */
    uint32_t leftPerservedNum = leftLoc;
    uint32_t rightperservedNum = container->elementsNum - rightLoc;

    if (rightperservedNum != 0) {
        memmove(arr + leftPerservedNum, arr + rightLoc, sizeof(arrayContainer) * rightperservedNum);
    }
    container->elementsNum = leftPerservedNum + rightperservedNum;
    shrinkArrIfNeed(container);
}

static void arrayContainerClearSingleBit(roaringContainer *container, uint16_t val)
{
    arrayContainer *arr = container->a.array;

    uint16_t loc = binarySearchLocUint16(arr, container->elementsNum, val);
    if (loc == container->elementsNum || arr[loc] != val) {
        return;
    }

    uint32_t backPreservedNum = container->elementsNum - loc - 1;
    if (backPreservedNum != 0) {
        memmove(arr + loc, arr + loc + 1, backPreservedNum * sizeof(arrayContainer));
    }
    container->elementsNum--;
    shrinkArrIfNeed(container);
}

static void arrayContainerClearBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
     if (minVal == maxVal) {
         arrayContainerClearSingleBit(container, minVal);
         return;
     }
    arrayContainer *arr = container->a.array;
    uint16_t firstVal = arr[0];
    uint16_t lastVal = arr[container->elementsNum - 1];

    if (minVal <= firstVal && maxVal >= lastVal) {
        container->elementsNum = 0;
    } else if (maxVal < firstVal || minVal > lastVal) {
        return;
    } else {
        /* just clear the part of the array */
        arrayContainerClearInterval(container, minVal, maxVal);
    }
}

static void containerClearBit(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    if (container == NULL || container->elementsNum == 0) {
        return;
    }
    uint32_t clearNum = maxVal - minVal + 1;

    if (clearNum == CONTAINER_CAPACITY) {
        clearContainer(container);
        return;
    }
    if (container->type == CONTAINER_TYPE_FULL) {
        fullContainerClearBit(container, minVal, maxVal);
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        bitmapContainerClearBit(container, minVal, maxVal);
    } else {
        arrayContainerClearBit(container, minVal, maxVal);
    }
}

 /* container get api */

static uint32_t bitmapContainerGetBitNum(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    bitmapContainer *bitmap = container->b.bitmap;

    if (minVal == maxVal) {
        return bitmapCheckBitStatus(bitmap, minVal);
    }
    /* assuming minVal is first bit in byte, maxVal is last bit in byte */
    uint16_t firstFullByteIdx = minVal >> 3;
    uint16_t lastFullByteIdx = maxVal >> 3;

    uint32_t bitsNum = 0;

    if (firstFullByteIdx == lastFullByteIdx) {
        uint8_t *byte = bitmap + firstFullByteIdx;
        uint32_t getBitNum = maxVal - minVal + 1;
        uint8_t bitIdx = minVal & MOD_8_MASK;

        return bitsNumTable[*byte & (((1 << getBitNum) - 1) << bitIdx)]; /* n bits mid of the byte */
    }

    /* minVal maxVal at different bytes */
    /* if minVal is not first bit in byte */
    if (minVal > firstFullByteIdx << 3) {
        uint8_t *firstByte = bitmap + firstFullByteIdx;
        uint8_t checkBitsNum = ((firstFullByteIdx + 1) << 3) - minVal; /* the upper n bits */

        bitsNum += bitsNumTable[*firstByte & ~((1 << (8 - checkBitsNum)) - 1)];
        firstFullByteIdx++;
    }

    /* maxVal is not last bit in byte */
    if (maxVal < ((lastFullByteIdx + 1) << 3) - 1) {
        uint8_t *lastByte = bitmap + lastFullByteIdx;
        uint8_t checkBitsNum = maxVal - (lastFullByteIdx << 3) + 1; /* the lower n bits */

        bitsNum += bitsNumTable[*lastByte & ((1 << checkBitsNum) - 1)];
        lastFullByteIdx--;
    }
    if (firstFullByteIdx <= lastFullByteIdx) {
        bitsNum += bitmapCountBits(bitmap, firstFullByteIdx, lastFullByteIdx);
    }

    return bitsNum;
}

static uint8_t arrayContainerGetSingleBit(roaringContainer *container, uint16_t val) {
    arrayContainer *arr = container->a.array;

    uint16_t loc = binarySearchLocUint16(arr, container->elementsNum, val);
    if (loc < container->elementsNum && arr[loc] == val) {
        return 1;
    }
    return 0;
}

static uint32_t arrayContainerGetBitNum(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
     if (minVal == maxVal) {
         return arrayContainerGetSingleBit(container, minVal);
     }
    arrayContainer *arr = container->a.array;
    uint16_t firstVal = arr[0];
    uint16_t lastVal = arr[container->elementsNum - 1];

    if (maxVal < firstVal || minVal > lastVal) {
        return 0;
    }
    uint16_t leftLoc = binarySearchLocUint16(arr, container->elementsNum, minVal);
    uint16_t rightLoc = binarySearchLocUint16(arr, container->elementsNum, maxVal);

    /* maxVal not exist */
    if (rightLoc == container->elementsNum || arr[rightLoc] != maxVal) {
        rightLoc--;
    }
    if (rightLoc >= leftLoc) {
        return rightLoc - leftLoc + 1;
    }
    return 0;
}

static uint32_t containerGetBitNum(roaringContainer *container, uint16_t minVal, uint16_t maxVal)
{
    if (container == NULL || container->elementsNum == 0) {
        return 0;
    }
    if (maxVal - minVal + 1 == CONTAINER_CAPACITY) {
        return container->elementsNum;
    }

    if (container->type == CONTAINER_TYPE_FULL) {
        return maxVal - minVal + 1;
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        return bitmapContainerGetBitNum(container, minVal, maxVal);
    } else {
        return arrayContainerGetBitNum(container, minVal, maxVal);
    }
}

/* rbm operate container api */

static void rbmDeleteBucket(roaringBitmap* rbm, uint8_t bucketIdx)
{
    uint32_t loc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, bucketIdx);

    if (loc == rbm->bucketsNum || rbm->buckets[loc] != bucketIdx) {
        return;
    }

    clearContainer(rbm->containers[loc]);
    roaring_free(rbm->containers[loc]);

    uint32_t leftPerservedNum = loc;
    uint32_t rightPerservedNum = rbm->bucketsNum - loc - 1;
    uint32_t newNum = leftPerservedNum + rightPerservedNum;

    uint8_t *newKeys = roaring_malloc(sizeof(uint8_t) * newNum);
    roaringContainer **newContainers = roaring_malloc(sizeof(roaringContainer *) * newNum);

    if (leftPerservedNum) {
        memcpy(newKeys, rbm->buckets, sizeof(uint8_t) * leftPerservedNum);
        memcpy(newContainers, rbm->containers, sizeof(roaringContainer *) * leftPerservedNum);
    }
    if (rightPerservedNum) {
        memcpy(newKeys + leftPerservedNum, rbm->buckets + leftPerservedNum + 1, sizeof(uint8_t) * rightPerservedNum);
        memcpy(newContainers + leftPerservedNum, rbm->containers + leftPerservedNum + 1, sizeof(roaringContainer *) * rightPerservedNum);
    }

    roaring_free(rbm->buckets);
    roaring_free(rbm->containers);
    rbm->buckets = newKeys;
    rbm->containers = newContainers;
    rbm->bucketsNum = newNum;
}

/* cursor is the physical idx of buckets, bucketIndex will be saved in keys */
static void rbmInsertBucket(roaringBitmap* rbm, uint32_t cursor, uint8_t bucketIndex)
{
    rbm->buckets = roaring_realloc(rbm->buckets, (rbm->bucketsNum + 1) * sizeof(uint8_t));
    rbm->containers = roaring_realloc(rbm->containers, (rbm->bucketsNum + 1) * sizeof(roaringContainer *));

    uint8_t leftBucketsNum = cursor;

    if (rbm->bucketsNum - leftBucketsNum != 0) {
        memmove(rbm->buckets + leftBucketsNum + 1, rbm->buckets + leftBucketsNum, (rbm->bucketsNum - leftBucketsNum) * sizeof(uint8_t));
        memmove(rbm->containers + leftBucketsNum + 1, rbm->containers + leftBucketsNum, (rbm->bucketsNum - leftBucketsNum) *
                                                                                        sizeof(roaringContainer *));
    }

    rbm->buckets[leftBucketsNum] = bucketIndex;
    rbm->containers[leftBucketsNum] = roaring_calloc(sizeof(roaringContainer));
    rbm->bucketsNum++;
}

static roaringContainer *rbmGetContainerIfNoInsert(roaringBitmap* rbm, uint8_t bucketIndex)
{
    uint8_t idx = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, bucketIndex);

    if (idx < rbm->bucketsNum && rbm->buckets[idx] == bucketIndex) {
        return rbm->containers[idx];
    }
    rbmInsertBucket(rbm, idx, bucketIndex);
    return rbm->containers[idx];
}

static roaringContainer *rbmGetContainer(roaringBitmap* rbm, uint8_t bucketIndex)
{
    if (rbm->bucketsNum == 0) {
        return NULL;
    }
    uint8_t idx = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, bucketIndex);

    if (idx == rbm->bucketsNum || rbm->buckets[idx] != bucketIndex) {
        return NULL;
    }
    return rbm->containers[idx];
}

/* bucket set get clear api */

static void bucketClearBit(roaringBitmap* rbm, uint8_t bucketIdx, uint16_t minVal, uint16_t maxVal)
{
    roaringContainer *container = rbmGetContainer(rbm, bucketIdx);
    if (container == NULL) {
        return;
    }
    containerClearBit(container, minVal, maxVal);
    if (container->elementsNum == 0) {
        rbmDeleteBucket(rbm, bucketIdx);
    }
}

static void bucketSetBit(roaringBitmap* rbm, uint8_t bucketIdx, uint16_t minVal, uint16_t maxVal)
{
    containerSetBit(rbmGetContainerIfNoInsert(rbm, bucketIdx), minVal, maxVal);
}

static uint32_t bucketGetBitNum(roaringBitmap* rbm, uint8_t bucketIdx, uint16_t minVal, uint16_t maxVal)
{
    return containerGetBitNum(rbmGetContainer(rbm, bucketIdx), minVal, maxVal);
}

static void rbmSetBucketsFull(roaringBitmap* rbm, uint8_t minBucket, uint8_t maxBucket)
{
    uint32_t leftLoc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, minBucket); /*  in set interval */
    uint32_t rightLoc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, maxBucket + 1); /* out of set interval */

    /* the buckets alreaddy exist */
    if (leftLoc != rbm->bucketsNum && rightLoc != 0 && rbm->buckets[leftLoc] == minBucket &&
        rbm->buckets[rightLoc - 1] == maxBucket && rightLoc - 1 - leftLoc == (uint32_t)(maxBucket - minBucket)) {
        for (uint32_t i = leftLoc; i < rightLoc; i++) {
            transToFullContainer(rbm->containers[i]);
        }
        return;
    }

    for (uint32_t i = leftLoc; i < rightLoc; i++) {
        clearContainer(rbm->containers[i]);
        roaring_free(rbm->containers[i]);
    }

    uint32_t leftPerservedNum = leftLoc;
    uint32_t rightPerservedNum = rbm->bucketsNum - rightLoc;
    uint32_t insertNum = maxBucket - minBucket + 1;
    uint32_t newNum = leftPerservedNum + insertNum + rightPerservedNum;

    rbm->buckets = roaring_realloc(rbm->buckets, sizeof(uint8_t) * newNum);
    rbm->containers = roaring_realloc(rbm->containers, sizeof(roaringContainer *) * newNum);

    if (rightPerservedNum) {
        memmove(rbm->buckets + leftPerservedNum + insertNum, rbm->buckets + rightLoc, sizeof(uint8_t) * rightPerservedNum);
        memmove(rbm->containers + leftPerservedNum + insertNum, rbm->containers + rightLoc, sizeof(roaringContainer *) * rightPerservedNum);
    }

    for (uint32_t i = leftPerservedNum, cursor = 0; i < leftPerservedNum + insertNum; i++, cursor++) {
        rbm->buckets[i] = minBucket + cursor;
        rbm->containers[i] = roaring_calloc(sizeof(roaringContainer));
        transToFullContainer(rbm->containers[i]);
    }

    rbm->bucketsNum = newNum;
}

static void rbmSetBucketsEmpty(roaringBitmap* rbm, uint8_t minBucket, uint8_t maxBucket)
{
    uint32_t leftLoc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, minBucket); /* in the interval to be deleted */
    uint32_t rightLoc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, maxBucket + 1); /* out of the interval to be deleted */

    /* interval to be deleted not exist */
    if (leftLoc == rightLoc) {
        return;
    }

    for (uint32_t i = leftLoc; i < rightLoc; i++) {
        clearContainer(rbm->containers[i]);
        roaring_free(rbm->containers[i]);
    }

    uint32_t leftPerservedNum = leftLoc;
    uint32_t rightPerservedNum = rbm->bucketsNum - rightLoc;
    uint32_t newNum = leftPerservedNum + rightPerservedNum;

    uint8_t *newKeys = roaring_malloc(sizeof(uint8_t) * newNum);
    roaringContainer **newContainers = roaring_malloc(sizeof(roaringContainer *) * newNum);

    if (leftPerservedNum) {
        memcpy(newKeys, rbm->buckets, sizeof(uint8_t) * leftPerservedNum);
        memcpy(newContainers, rbm->containers, sizeof(roaringContainer *) * leftPerservedNum);
    }
    if (rightPerservedNum) {
        memcpy(newKeys + leftPerservedNum, rbm->buckets + rightLoc, sizeof(uint8_t) * rightPerservedNum);
        memcpy(newContainers + leftPerservedNum, rbm->containers + rightLoc, sizeof(roaringContainer *) * rightPerservedNum);
    }

    roaring_free(rbm->buckets);
    roaring_free(rbm->containers);
    rbm->buckets = newKeys;
    rbm->containers = newContainers;
    rbm->bucketsNum = newNum;
}

static uint32_t rbmGetSingleBucketBitNum(roaringBitmap* rbm, uint8_t bucketIndex)
{
    roaringContainer *container = rbmGetContainer(rbm, bucketIndex);
    if (container == NULL) {
        return 0;
    }
    return container->elementsNum;
}

static uint32_t rbmGetBucketsBitNum(roaringBitmap* rbm, uint8_t minBucket, uint8_t maxBucket)
{
    assert(minBucket <= maxBucket);

    if (minBucket == maxBucket) {
        return rbmGetSingleBucketBitNum(rbm, minBucket);
    }

    uint8_t leftLoc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, minBucket);
    uint8_t rightLoc = binarySearchLocUint8(rbm->buckets, rbm->bucketsNum, maxBucket);

    uint32_t bitsNum = 0;

    for (uint8_t i = leftLoc; i <= rightLoc; i++) {
        if (i == rbm->bucketsNum || rbm->buckets[i] > maxBucket) {
            break;
        }
        bitsNum += rbm->containers[i]->elementsNum;
    }
    return bitsNum;
}

/* rbm export api */

roaringBitmap* rbmCreate(void)
{
    roaringBitmap *bitmap = roaring_malloc(sizeof(roaringBitmap));
    bitmap->bucketsNum = 0;
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
    for (int i = 0; i < rbm->bucketsNum; i++) {
        clearContainer(rbm->containers[i]);
        roaring_free(rbm->containers[i]);
    }
    roaring_free(rbm->containers);
    roaring_free(rbm);
}

void rbmSetBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit)
{
    serverAssert(rbm != NULL && minBit <= maxBit);

    uint32_t firstBucketIdx = minBit >> CONTAINER_BITS;
    uint32_t lastBucketIdx = maxBit >> CONTAINER_BITS;

    assert(lastBucketIdx < (1 << BUCKET_MAX_BITS));

    uint16_t minVal = minBit & CONTAINER_MASK;
    uint16_t maxVal = maxBit & CONTAINER_MASK;

    /* minBit maxBit in the same Container */
    if (firstBucketIdx == lastBucketIdx) {
        bucketSetBit(rbm, firstBucketIdx, minVal, maxVal);
        return;
    }

    /* assuming the minBit is first bit in the container, maxBit is last bit in the container */
    uint32_t firstWholeBucket = firstBucketIdx;
    uint32_t lastWholeBucket = lastBucketIdx;

    /* minBit is not first bit in the container */
    if ((minBit & CONTAINER_MASK) != 0) {
        minVal = minBit & CONTAINER_MASK;
        maxVal = ((firstBucketIdx + 1) * CONTAINER_CAPACITY - 1) & CONTAINER_MASK;
        bucketSetBit(rbm, firstBucketIdx, minVal, maxVal);
        firstWholeBucket++;
    }
    /* maxBit is not last bit in the container */
    if (((maxBit + 1) & CONTAINER_MASK) != 0) {
        minVal = (lastBucketIdx * CONTAINER_CAPACITY) & CONTAINER_MASK;
        maxVal = maxBit & CONTAINER_MASK;
        bucketSetBit(rbm, lastBucketIdx, minVal, maxVal);
        lastWholeBucket--;
    }

    if (firstWholeBucket <= lastWholeBucket) {
        rbmSetBucketsFull(rbm, firstWholeBucket, lastWholeBucket);
    }
}

uint32_t rbmGetBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit)
{
    serverAssert(rbm != NULL && minBit <= maxBit);
    uint32_t firstBucketIdx = minBit >> CONTAINER_BITS;
    uint32_t lastBucketIdx = maxBit >> CONTAINER_BITS;

    assert(lastBucketIdx < (1 << BUCKET_MAX_BITS));

    uint16_t minVal = minBit & CONTAINER_MASK;
    uint16_t maxVal = maxBit & CONTAINER_MASK;

    uint32_t bitsNum = 0;
    /* minBit maxBit in the same Container */
    if (firstBucketIdx == lastBucketIdx) {
        return bucketGetBitNum(rbm, firstBucketIdx, minVal, maxVal);
    }

    /* process  container of minBit */
    maxVal = ((firstBucketIdx + 1) * CONTAINER_CAPACITY - 1) & CONTAINER_MASK;
    bitsNum += bucketGetBitNum(rbm, firstBucketIdx, minVal, maxVal);

    /* process  container of maxBit */
    minVal = (lastBucketIdx * CONTAINER_CAPACITY) & CONTAINER_MASK;
    maxVal = maxBit & CONTAINER_MASK;
    bitsNum += bucketGetBitNum(rbm, lastBucketIdx, minVal, maxVal);

    if (firstBucketIdx + 1 < lastBucketIdx) {
        bitsNum += rbmGetBucketsBitNum(rbm, firstBucketIdx + 1, lastBucketIdx - 1);
    }

    return bitsNum;
}

void rbmClearBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit)
{
    serverAssert(rbm != NULL && minBit <= maxBit);
    uint32_t firstBucketIdx = minBit >> CONTAINER_BITS;
    uint32_t lastBucketIdx = maxBit >> CONTAINER_BITS;

    assert(lastBucketIdx < (1 << BUCKET_MAX_BITS));

    uint16_t minVal = minBit & CONTAINER_MASK;
    uint16_t maxVal = maxBit & CONTAINER_MASK;

    /* minBit maxBit in the same Container */
    if (firstBucketIdx == lastBucketIdx) {
        bucketClearBit(rbm, firstBucketIdx, minVal, maxVal);
        return;
    }

    /* assuming the minBit is first bit in the container, maxBit is last bit in the container */
    uint32_t firstWholeBucket = firstBucketIdx;
    uint32_t lastWholeBucket = lastBucketIdx;

    /* minBit is not first bit in the container */
    if ((minBit & CONTAINER_MASK) != 0) {
        /* 处理startBit 所属Container */
        minVal = minBit & CONTAINER_MASK;
        maxVal = ((firstBucketIdx + 1) * CONTAINER_CAPACITY - 1) & CONTAINER_MASK;
        bucketClearBit(rbm, firstBucketIdx, minVal, maxVal);
        firstWholeBucket++;
    }
    /* maxBit is not last bit in the container */
    if (((maxBit + 1) & CONTAINER_MASK) != 0) {
        minVal = (lastBucketIdx * CONTAINER_CAPACITY) & CONTAINER_MASK;
        maxVal = maxBit & CONTAINER_MASK;
        bucketClearBit(rbm, lastBucketIdx, minVal, maxVal);
        lastWholeBucket--;
    }

    if (firstWholeBucket <= lastWholeBucket) {
        rbmSetBucketsEmpty(rbm, firstWholeBucket, lastWholeBucket);
    }
}

static inline void containersDup(roaringContainer **destContainers, roaringContainer **srcContainers, uint32_t num)
{
    for (uint32_t i = 0; i < num; i++) {
        destContainers[i] = roaring_calloc(sizeof(roaringContainer));
        destContainers[i]->elementsNum = srcContainers[i]->elementsNum;
        destContainers[i]->type = srcContainers[i]->type;
        if (destContainers[i]->type == CONTAINER_TYPE_BITMAP) {
            destContainers[i]->b.bitmap = roaring_malloc(BITMAP_CONTAINER_SIZE);
            memcpy(destContainers[i]->b.bitmap, srcContainers[i]->b.bitmap, BITMAP_CONTAINER_SIZE);
        } else if (destContainers[i]->type == CONTAINER_TYPE_ARRAY) {
            destContainers[i]->a.capacity = srcContainers[i]->a.capacity;
            destContainers[i]->a.array = roaring_malloc(destContainers[i]->a.capacity * sizeof(arrayContainer));
            memcpy(destContainers[i]->a.array, srcContainers[i]->a.array, destContainers[i]->a.capacity * sizeof(arrayContainer));
        }
    }
}

void rbmdup(roaringBitmap* destRbm, roaringBitmap* srcRbm)
{
    serverAssert(destRbm != NULL && srcRbm != NULL);
    serverAssert(destRbm->buckets == NULL && destRbm->containers == NULL);
    destRbm->bucketsNum = srcRbm->bucketsNum;
    destRbm->buckets = roaring_malloc(destRbm->bucketsNum * sizeof(uint8_t));
    memcpy(destRbm->buckets, srcRbm->buckets, sizeof(uint8_t) * destRbm->bucketsNum);
    destRbm->containers = roaring_calloc(destRbm->bucketsNum * sizeof(roaringContainer *));
    containersDup(destRbm->containers, srcRbm->containers, destRbm->bucketsNum);
}

static inline int containersAreEqual(roaringContainer **destContainers, roaringContainer **srcContainers, uint32_t num)
{
    for (uint32_t i = 0; i < num; i++) {
        if (destContainers[i]->elementsNum != srcContainers[i]->elementsNum || destContainers[i]->type != srcContainers[i]->type) {
            return 0;
        }
        if (destContainers[i]->type == CONTAINER_TYPE_BITMAP) {
            if (0 != memcmp(destContainers[i]->b.bitmap, srcContainers[i]->b.bitmap, BITMAP_CONTAINER_SIZE)) {
                return 0;
            }
        } else if (destContainers[i]->type == CONTAINER_TYPE_ARRAY) {
            if (0 != memcmp(destContainers[i]->a.array, srcContainers[i]->a.array, destContainers[i]->elementsNum * sizeof(arrayContainer))) {
                return 0;
            }
        }
    }
    return 1;
}

int rbmIsEqual(roaringBitmap* destRbm, roaringBitmap* srcRbm)
{
    serverAssert(destRbm != NULL && srcRbm != NULL);
    if (destRbm->bucketsNum != srcRbm->bucketsNum || 0 != memcmp(destRbm->buckets, srcRbm->buckets, destRbm->bucketsNum *
            sizeof(uint8_t))) {
        return 0;
    }
    return containersAreEqual(destRbm->containers, srcRbm->containers, destRbm->bucketsNum);
}

static inline uint32_t arrayContainerLocateSetBitPos(roaringContainer *container, uint8_t bitIdxPrefix, uint32_t *idxArrCursor, uint32_t bitsNum)
{
    uint32_t leftNum = bitsNum;
    uint32_t idxPrefix = bitIdxPrefix;
    for (int i = 0; i < container->elementsNum && leftNum != 0; i++) {
        uint32_t bitIdx = (idxPrefix << CONTAINER_BITS) + container->a.array[i];
        *idxArrCursor = bitIdx;
        idxArrCursor++;
        leftNum--;
    }
    return bitsNum - leftNum;
}

static inline uint32_t bitmapContainerLocateSetBitPos(roaringContainer *container, uint8_t bitIdxPrefix, uint32_t *idxArrCursor, uint32_t bitsNum)
{
    uint32_t idxPrefix = bitIdxPrefix;
    uint32_t leftBytes = BITMAP_CONTAINER_SIZE;
    uint32_t bytePos = 0;
    uint32_t leftNum = bitsNum;
    while (leftBytes > 0 && leftNum) {
        bitmapContainer *cursor = container->b.bitmap + bytePos;
        if (leftBytes > sizeof(uint64_t) && *((uint64_t *)(cursor)) == 0) {
            bytePos += sizeof(uint64_t);
            leftBytes -= sizeof(uint64_t);
            continue;
        } else if (leftBytes > sizeof(uint32_t) && *((uint32_t *)(cursor)) == 0) {
            bytePos += sizeof(uint32_t);
            leftBytes -= sizeof(uint32_t);
            continue;
        } else if (leftBytes > sizeof(uint16_t) && *((uint16_t *)(cursor)) == 0) {
            bytePos += sizeof(uint16_t);
            leftBytes -= sizeof(uint16_t);
            continue;
        } else if (*((uint8_t *)(cursor)) == 0) {
            bytePos += sizeof(uint8_t);
            leftBytes -= sizeof(uint8_t);
            continue;
        }

        const uint8_t word = *cursor;
        for (int i = 0; i < 8 && leftNum != 0; i++) {
            if (word & (1 << i)) {
                uint32_t bitIdx = i + bytePos * BITS_NUM_IN_BYTE + (idxPrefix << CONTAINER_BITS);
                *idxArrCursor = bitIdx;
                idxArrCursor++;
                leftNum--;
            }
        }
        bytePos += sizeof(uint8_t);
        leftBytes -= sizeof(uint8_t);
    }
    return bitsNum - leftNum;
}

static inline uint32_t fullContainerLocateSetBitPos(uint8_t bitIdxPrefix, uint32_t *idxArrCursor, uint32_t bitsNum) {
    uint32_t idxPrefix = bitIdxPrefix;
    uint32_t leftNum = bitsNum;
    for (int i = 0; i < CONTAINER_CAPACITY && leftNum != 0; i++, leftNum--) {
        uint32_t bitIdx = (idxPrefix << CONTAINER_BITS) + i;
        *idxArrCursor = bitIdx;
        idxArrCursor++;
    }
    return bitsNum - leftNum;
}

static inline uint32_t bucketLocateSetBitPos(roaringBitmap *rbm, uint8_t bucketPhyIdx, uint32_t *idxArrCursor, uint32_t bitsNum)
{
    uint8_t bitIdxPrefix = rbm->buckets[bucketPhyIdx];
    roaringContainer *container = rbm->containers[bucketPhyIdx];
    if (container == NULL) {
        return 0;
    }
    if (container->type == CONTAINER_TYPE_ARRAY) {
        return arrayContainerLocateSetBitPos(container, bitIdxPrefix, idxArrCursor, bitsNum);
    } else if (container->type == CONTAINER_TYPE_BITMAP) {
        return bitmapContainerLocateSetBitPos(container, bitIdxPrefix, idxArrCursor, bitsNum);
    } else {
        return fullContainerLocateSetBitPos(bitIdxPrefix, idxArrCursor, bitsNum);
    }
}

uint32_t rbmLocateSetBitPos(roaringBitmap* rbm, uint32_t bitsNum, uint32_t *idxArr)
{
    serverAssert(rbm != NULL || bitsNum != 0 || idxArr != NULL);
    uint32_t *idxArrCursor = idxArr;
    uint32_t leftBitsNum = bitsNum;
    for (int i = 0; i < rbm->bucketsNum; i++) {
        uint32_t realBitsNum = bucketLocateSetBitPos(rbm, i, idxArrCursor, leftBitsNum);
        leftBitsNum -= realBitsNum;
        idxArrCursor += realBitsNum;
        if (leftBitsNum == 0) {
            return bitsNum;
        }
    }
    return bitsNum - leftBitsNum;
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

    if (rbmEncodeAppend_(encoded,len,&rbm->bucketsNum,sizeof(rbm->bucketsNum),&cursor)) goto err;
    if (rbmEncodeAppend_(encoded,len,rbm->buckets,sizeof(uint8_t)*rbm->bucketsNum,&cursor)) goto err;

    for(int i = 0; i < rbm->bucketsNum; i++) {
        uint16_t elementsNum = htons(rbm->containers[i]->elementsNum);
        if (rbmEncodeAppend_(encoded,len,&elementsNum,sizeof(elementsNum),&cursor)) goto err;

        uint8_t type = rbm->containers[i]->type;
        if (rbmEncodeAppend_(encoded,len,&type,sizeof(type),&cursor)) goto err;

        if (rbm->containers[i]->type == CONTAINER_TYPE_ARRAY) {
            size_t arrayLen = sizeof(arrayContainer) * rbm->containers[i]->elementsNum;
            if (encoded) {
                if (cursor + arrayLen > len) goto err;
                for(int j=0; j<rbm->containers[i]->elementsNum; j++) {
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
    memcpy(&rbm->bucketsNum,cursor,sizeof(uint8_t));
    cursor += sizeof(uint8_t), len -= sizeof(uint8_t);

    size_t bucketsLen = rbm->bucketsNum * sizeof(uint8_t);
    if (len < bucketsLen) goto err;
    rbm->buckets = roaring_malloc(bucketsLen);
    memcpy(rbm->buckets,cursor,bucketsLen);
    cursor += bucketsLen, len -= bucketsLen;

    rbm->containers = roaring_calloc(rbm->bucketsNum * sizeof(roaringContainer *));

    for (int i = 0; i< rbm->bucketsNum; i++) {
        rbm->containers[i] = roaring_calloc(sizeof(roaringContainer));

        uint16_t elementsNum = 0;
        if (len < sizeof(elementsNum)) goto err;
        memcpy(&elementsNum,cursor,sizeof(elementsNum));
        elementsNum = ntohs(elementsNum);
        rbm->containers[i]->elementsNum = elementsNum;
        cursor += sizeof(elementsNum), len -= sizeof(elementsNum);

        uint8_t type = 0;
        if (len < sizeof(type)) goto err;
        memcpy(&type,cursor,sizeof(type));
        cursor += sizeof(type), len -= sizeof(type);
        rbm->containers[i]->type = type;

        if (type == CONTAINER_TYPE_ARRAY) {
            size_t arraySize = sizeof(arrayContainer) * rbm->containers[i]->elementsNum;
            if (len < arraySize) goto err;
            rbm->containers[i]->a.array = roaring_malloc(arraySize);
            rbm->containers[i]->a.capacity = rbm->containers[i]->elementsNum;
            for(int j=0; j<rbm->containers[i]->elementsNum; j++) {
                arrayContainer* array = (arrayContainer*)cursor;
                cursor += sizeof(arrayContainer);
                uint16_t value = *array;
                value = ntohs(value);
                rbm->containers[i]->a.array[j] = value;
            }
            len -= arraySize;
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

            /* 个数元素 量级 */
            /* 正常测 */
            /* [0, 8] */
            rbmSetBitRange(rbm, 0, 8);

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            /* 边界测 */
            bitNum = rbmGetBitRange(rbm, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 8, 200);
            test_assert(bitNum == 1);

            /* array container 量级 */
            /* 正常测 */
            /* [0, 8]  [10, 200]*/
            rbmSetBitRange(rbm, 10, 200);
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 200);

            bitNum = rbmGetBitRange(rbm, 9, 9);  /* 界内 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* 跨边界 */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 201, 201);  /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 196, 205); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 205); /* 横跨 set范围 */
            test_assert(bitNum == 191);

            rbmSetBitRange(rbm, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm, 200, 300);  /* 边界set */

            /* [0, 8]  [10, 300]*/
            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 300);

            /* Bitmap container 量级 */
            /* [0, 8]  [10, 1000]*/
            rbmSetBitRange(rbm, 200, 1000);
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 1000);

            bitNum = rbmGetBitRange(rbm, 1001, 1001);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 996, 1005); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 1005); /* 横跨 set范围 */
            test_assert(bitNum == 991);

            rbmSetBitRange(rbm, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 1000);

            /* 跨 Container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2); /* container 级别验证 */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm, 4096 - 5, 4096 + 5); /* 区间跨container验证 */
            test_assert(bitNum == 11);

            /* 跨full Container set get */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]*/

            rbmSetBitRange(rbm, 4096 * 2 + 1000, 4096 * 2 + 2000);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            /* 跨empty container set get */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]  [4096 * 3 + 1000, 4096 * 3 + 2000]*/
            rbmSetBitRange(rbm, 4096 * 3 + 1000, 4096 * 3 + 2000); /* 填充empty */

            bitNum = rbmGetBitRange(rbm, 4096 * 3, 4096 * 4 - 1); /* container 级别验证 */
            test_assert(bitNum == 1001);

            /* 整个roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 128 - 1); /* container 级别验证 */
            test_assert(bitNum == 6294);

            rbmDestory(rbm);
        }

        TEST("roaring-bitmap: set get clear") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* 个数 量级 */
            /* 正常测 */
            rbmSetBitRange(rbm, 4, 8);   /* [4, 8] */

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 4, 4);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 8, 8);
            test_assert(bitNum == 1);

            /* 边界测 */
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


            /* array container 量级 */
            /* 正常测 */
            rbmSetBitRange(rbm, 10, 200);  /* [4, 5]   [10 ,200] */
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 193);

            rbmClearBitRange(rbm, 0, 9);    /* [10 ,200] */

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* 跨边界 */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* 边界外 */
            test_assert(bitNum == 0);

            rbmClearBitRange(rbm, 10, 99);    /* [100 ,200] */

            bitNum = rbmGetBitRange(rbm, 191, 210);  /* 跨边界 */
            test_assert(bitNum == 10);

            rbmClearBitRange(rbm, 191, 200);    /* [100 , 190] */

            bitNum = rbmGetBitRange(rbm, 181, 210);  /* 跨边界 */
            test_assert(bitNum == 10);

            rbmClearBitRange(rbm, 151, 159);    /* 存在区间 [100 ,150]， [160, 190] */

            bitNum = rbmGetBitRange(rbm, 141, 169);  /* 跨边界 */
            test_assert(bitNum == 20);


            /* Bitmap container 量级 */
            rbmSetBitRange(rbm, 200, 1000);   /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 1000 */ /* 触发array container 转为Bitmapcontainer */
            bitNum = rbmGetBitRange(rbm, 0, 1000);   /* array container最大值， Bitmap Container 最大值之间 */
            test_assert(bitNum == 883);

            bitNum = rbmGetBitRange(rbm, 1001, 1001); /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 165, 165);   /* 范围中 */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 160, 160);   /* 范围中 */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 220, 220);   /* 范围中 */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 159, 159);   /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 160, 220); /* 跨边界 */
            test_assert(bitNum == 52);

            /* 存在区间 100 ~150， 160 ~ 190 , 301 ~ 1000 */
            rbmClearBitRange(rbm, 200, 300);    /* 范围外 clear */

            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 782);

            rbmClearBitRange(rbm, 501, 2000);  /* 边界 clear */
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 282);

            rbmClearBitRange(rbm, 0, 129);   /* 边界 clear */
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 252);

            rbmClearBitRange(rbm, 171, 180);  /* 范围内 Clear */
            bitNum = rbmGetBitRange(rbm, 0, 1000);
            test_assert(bitNum == 242);

            rbmSetBitRange(rbm, 501, 1000);
            rbmSetBitRange(rbm, 100, 129);
            rbmSetBitRange(rbm, 171, 180);

            /* 跨 Container */
            rbmSetBitRange(rbm, 200, 4096 * 2 + 1);   /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 + 1 */
            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2 + 1);
            test_assert(bitNum == 8076);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1);
            test_assert(bitNum == 1);

              /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 */
            rbmClearBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1); /* 边界 Clear */

            bitNum = rbmGetBitRange(rbm, 4096 * 2, 4096 * 2);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2);
            test_assert(bitNum == 8075);

               /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 */
            rbmClearBitRange(rbm, 4096 + 1, 4096 * 2); /* 跨 full Container clear */  /*  full Container clear 边界， 生成bitmap container */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2 + 1);
            test_assert(bitNum == 3979);

            bitNum = rbmGetBitRange(rbm, 4090, 4100);   /* 跨边界 */
            test_assert(bitNum == 7);

              /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 - 1 */
            rbmSetBitRange(rbm, 4096 + 1, 4096 * 2 - 1); /* 重新产生 full container */

            /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 + 9 ,  4096 + 4095 ~ 4096 + 4095 */
            rbmClearBitRange(rbm, 4096 + 10, 4096 + 4094); /* full container 中间Clear， 生成 array container */

            bitNum = rbmGetBitRange(rbm, 4096 + 1, 4096 + 4095);
            test_assert(bitNum == 10);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 4);   /* container 级别统计 */
            test_assert(bitNum == 3989);

            rbmClearBitRange(rbm, 4096 * 3, 4096 * 4 - 1000); /* clear empty container, 无效果 */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 4);   /* container 级别统计 */
            test_assert(bitNum == 3989);

            rbmDestory(rbm);
        }

        TEST("roaring bitmap: set get clear operation in upper container location test") {
            roaringBitmap* rbm = rbmCreate();

            uint32_t bitNum = rbmGetBitRange(rbm, 0, 131071);  /*maxbit*/
            test_assert(bitNum == 0);

            /* rbmSetBitRange(rbm, 0, 131072);   超出范围， 直接被assert */

            /* 从高位 往低位 单个 setbit*/
            rbmSetBitRange(rbm, 131071, 131071);        /* 区间 [131071, 131071] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);   /* 边界 */
            test_assert(bitNum == 1);

            /* 从高位 往低位 批量 setbit*/
            rbmSetBitRange(rbm, 131071 - 4096 - 1, 131070);    /* 区间 [131071 - 4096 - 1, 131071] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 4098);

            rbmSetBitRange(rbm, 131071 - 4096 * 2, 131071 - 4096 - 2);   /* 区间 [131071 - 4096 * 2, 131071] */

            bitNum = rbmGetBitRange(rbm, 0, 131071);  /* 批量getbit */
            test_assert(bitNum == 4096 * 2 + 1);

            bitNum = rbmGetBitRange(rbm, 130000, 130000); /* 单个getbit */
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 * 2, 131071 - 4096 * 2);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 131071 - 4096 * 2 - 1, 131071 - 4096 * 2 - 1);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 131071, 131071);   /* 边界 */
            test_assert(bitNum == 1);

            /* 从高位往低位 单个 Clearbit */
            rbmClearBitRange(rbm, 131071, 131071);    /* 区间 [131071 - 4096 * 2, 131070] */

            bitNum = rbmGetBitRange(rbm, 131071, 131071);  /*maxbit*/
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 4096 * 2);

            /* 批量Clear, 触发最后一个container 从Bitmap转为array */
            rbmClearBitRange(rbm, 131071 - 4096 - 4000, 131071 - 4096 - 1);   /* 区间 [131071 - 4096 * 2, 131071 - 4096 - 4001], [131071 - 4096, 131070] */

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

            rbmSetBitRange(rbm, 0, 100);  /* 从低位 批量set */
            bitNum = rbmGetBitRange(rbm, 0, 4096);
            test_assert(bitNum == 101);

            rbmSetBitRange(rbm, 101, 101);  /* 从低位 单个set */
            bitNum = rbmGetBitRange(rbm, 0, 4096);
            test_assert(bitNum == 102);

            rbmSetBitRange(rbm, 102, 4000);  /* 触发从 array container转为 Bitmap container */
            bitNum = rbmGetBitRange(rbm, 0, 4096);
            test_assert(bitNum == 4001);

            rbmSetBitRange(rbm, 4096, 4096 * 4 + 4090); /* 触发 产生3 个 full container */

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

            rbmClearBitRange(rbm, 4096, 4096); /* 单点 Clear 第一个 full container */

            bitNum = rbmGetBitRange(rbm, 4096, 4096);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 4096 + 1, 4096 + 1);
            test_assert(bitNum == 1);

            rbmClearBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1); /* 单点 Clear 第二 full container */

            bitNum = rbmGetBitRange(rbm, 4096 * 2, 4096 * 2);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 4096 * 2 + 2, 4096 * 2 + 2);
            test_assert(bitNum == 1);

            rbmClearBitRange(rbm, 4096 * 3 + 101, 4096 * 3 + 4000); /* 批量 Clear 第三 full container */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 + 100, 4096 * 3 + 100);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 4096 * 3 + 4000, 4096 * 3 + 4000);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 4 + 4090);
            test_assert(bitNum == 4001 + 4096 * 2 - 2 + 196 + 4091);

            rbmSetBitRange(rbm, 4096 * 4 + 4091, 4096 * 5 - 1); /* 产生第四个full Container */

            /* 对第四个full Container 进行单点， 范围get set操作 */
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

            rbmClearBitRange(rbm, 4096, 4096 + 100); /* 第二个 container为empty */

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

            /* 单点 set 触发insert 3个 Bucket */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 2);
            rbmSetBitRange(rbm, 4096 * 3, 4096 * 3);
            rbmSetBitRange(rbm, 4096 * 5 - 1, 4096 * 5 - 1);

            test_assert(rbm->bucketsNum == 3);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 3);

            /* 范围 set 触发insert 3个 Bucket */
            rbmSetBitRange(rbm, 4096 * 6, 4096 * 6 + 99);
            rbmSetBitRange(rbm, 4096 * 7, 4096 * 7 + 99);
            rbmSetBitRange(rbm, 4096 * 8, 4096 * 8 + 99);

            test_assert(rbm->bucketsNum == 6);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 303);

            /*  set full container 触发insert 3个 Bucket */
            rbmSetBitRange(rbm, 4096 * 9, 4096 * 10 - 1);
            rbmSetBitRange(rbm, 4096 * 10, 4096 * 11 - 1);
            rbmSetBitRange(rbm, 4096 * 11, 4096 * 12 - 1);

            test_assert(rbm->bucketsNum == 9);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 303 + 4096 * 3);

            /* 单点删除 Bucket */
            rbmClearBitRange(rbm, 4096 * 9, 4096 * 10 - 1);
            rbmClearBitRange(rbm, 4096 * 6, 4096 * 6 + 99);
            rbmClearBitRange(rbm, 4096 * 2, 4096 * 2);

            test_assert(rbm->bucketsNum == 6);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 202 + 4096 * 2);

            /* 批量删除 Bucket */
            rbmClearBitRange(rbm, 4096 * 5, 4096 * 12 - 1);
            test_assert(rbm->bucketsNum == 2);

            bitNum = rbmGetBitRange(rbm, 0, 131071);
            test_assert(bitNum == 2);

            rbmDestory(rbm);
        }

        TEST("roaring-bitmap: set get clear getbitpos") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* 个数 量级 */
            /* 正常测 */
            rbmSetBitRange(rbm, 4, 8);   /* [4, 8] */

            uint32_t *idxArr = zmalloc(sizeof(uint32_t) * CONTAINER_CAPACITY);

            bitNum = rbmLocateSetBitPos(rbm, 6, idxArr);
            test_assert(bitNum == 5);
            test_assert(idxArr[0] == 4);
            test_assert(idxArr[1] == 5);
            test_assert(idxArr[2] == 6);
            test_assert(idxArr[3] == 7);
            test_assert(idxArr[4] == 8);

            rbmClearBitRange(rbm, 6, 8);    /* [4, 5] */
            bitNum = rbmLocateSetBitPos(rbm, 6, idxArr);
            test_assert(bitNum == 2);
            test_assert(idxArr[0] == 4);
            test_assert(idxArr[1] == 5);

            /* array container 量级 */
            /* 正常测 */
            rbmSetBitRange(rbm, 10, 200);  /* [4, 5]   [10 ,200] */
            bitNum = rbmLocateSetBitPos(rbm, 100, idxArr);
            test_assert(bitNum == 100);
            test_assert(idxArr[0] == 4);
            test_assert(idxArr[99] == 107);

            bitNum = rbmLocateSetBitPos(rbm, 200, idxArr);
            test_assert(bitNum == 193);
            test_assert(idxArr[0] == 4);
            test_assert(idxArr[192] == 200);

            rbmClearBitRange(rbm, 0, 9);    /* [10 ,200] */

            rbmClearBitRange(rbm, 10, 99);    /* [100 ,200] */

            rbmClearBitRange(rbm, 191, 200);    /* [100 , 190] */

            rbmClearBitRange(rbm, 151, 159);    /* 存在区间 [100 ,150]， [160, 190] */

            /* Bitmap container 量级 */
            rbmSetBitRange(rbm, 200, 1000);   /* 存在区间 100 ~ 150， 160 ~ 190 , 200 ~ 1000 */ /* 触发array container 转为Bitmapcontainer */

            bitNum = rbmLocateSetBitPos(rbm, 1000, idxArr);
            test_assert(bitNum == 883);
            test_assert(idxArr[0] == 100);
            test_assert(idxArr[882] == 1000);

            bitNum = rbmLocateSetBitPos(rbm, 800, idxArr);
            test_assert(bitNum == 800);
            test_assert(idxArr[0] == 100);
            test_assert(idxArr[799] == 917);

            /* 存在区间 100 ~150， 160 ~ 190 , 301 ~ 1000 */
            rbmClearBitRange(rbm, 200, 300);    /* 范围外 clear */


            rbmClearBitRange(rbm, 501, 2000);  /* 边界 clear */

            rbmClearBitRange(rbm, 0, 129);   /* 边界 clear */

            rbmClearBitRange(rbm, 171, 180);  /* 范围内 Clear */

            rbmSetBitRange(rbm, 501, 1000);
            rbmSetBitRange(rbm, 100, 129);
            rbmSetBitRange(rbm, 171, 180);

            /* across Container bitmap, full , array */
            rbmSetBitRange(rbm, 200, 4096 * 2 + 1);   /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 + 1 */

            bitNum = rbmLocateSetBitPos(rbm, 4096 * 2 + 1, idxArr);
            test_assert(bitNum == 8076);
            test_assert(idxArr[0] == 100);
            test_assert(idxArr[8075] == 4096 * 2 + 1);

              /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 */
            rbmClearBitRange(rbm, 4096 * 2 + 1, 4096 * 2 + 1); /* 边界 Clear */

             /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 */
            rbmClearBitRange(rbm, 4096 + 1, 4096 * 2); /* 跨 full Container clear */  /*  full Container clear 边界， 生成bitmap container */

              /* 存在区间 100 ~150， 160 ~ 190 , 200 ~ 4096 * 2 - 1 */
            rbmSetBitRange(rbm, 4096 + 1, 4096 * 2 - 1); /* 重新产生 full container */

            /*  full container, mid of container */
            bitNum = rbmLocateSetBitPos(rbm, 4096, idxArr);
            test_assert(bitNum == 4096);
            test_assert(idxArr[0] == 100);
            test_assert(idxArr[4095] == 4213);

            /*  full container, end of container */
            bitNum = rbmLocateSetBitPos(rbm, 8074, idxArr);
            test_assert(bitNum == 8074);
            test_assert(idxArr[0] == 100);
            test_assert(idxArr[8073] == 4096 * 2 - 1);

            zfree(idxArr);
            rbmDestory(rbm);
        }

        TEST("roaring-bitmap: set get duplicate test") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* 个数元素 量级 */
            /* 正常测 */
            /* [0, 8] */
            rbmSetBitRange(rbm, 0, 8);

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            /* 边界测 */
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

            /* 边界测 */
            bitNum = rbmGetBitRange(rbm1, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm1, 8, 200);
            test_assert(bitNum == 1);

            rbmDestory(rbm1);

            /* array container 量级 */
            /* 正常测 */
            /* [0, 8]  [10, 200]*/
            rbmSetBitRange(rbm, 10, 200);
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 200);

            bitNum = rbmGetBitRange(rbm, 9, 9);  /* 界内 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* 跨边界 */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 201, 201);  /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 196, 205); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 205); /* 横跨 set范围 */
            test_assert(bitNum == 191);

            roaringBitmap* rbm2 = rbmCreate();
            rbmdup(rbm2, rbm);

            bitNum = rbmGetBitRange(rbm2, 9, 9);  /* 界内 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm2, 100, 300);  /* 跨边界 */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm2, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm2, 201, 400); /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 201, 201);  /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 196, 205); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm2, 9, 205); /* 横跨 set范围 */
            test_assert(bitNum == 191);

            /* 增量修改 rbm */
            rbmSetBitRange(rbm, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm, 200, 300);  /* 边界set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 300);

            /* 增量修改 rbm2 */
            rbmSetBitRange(rbm2, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm2, 200, 300);  /* 边界set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 300);

            rbmDestory(rbm2);

            /* Bitmap container 量级 */
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

            bitNum = rbmGetBitRange(rbm3, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm3, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm3, 996, 1005); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm3, 9, 1005); /* 横跨 set范围 */
            test_assert(bitNum == 991);

            /* 增量修改rbm */
            rbmSetBitRange(rbm, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 1000);

            /* 增量修改rbm3 */
            rbmSetBitRange(rbm3, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm3, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 1000);

            /* rbm 跨 Container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2); /* container 级别验证 */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm, 4096 - 5, 4096 + 5); /* 区间跨container验证 */
            test_assert(bitNum == 11);

            /* rbm3 跨 Container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm3, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm3, 0, 4096 * 2); /* container 级别验证 */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm3, 4096 - 5, 4096 + 5); /* 区间跨container验证 */
            test_assert(bitNum == 11);

            rbmDestory(rbm3);

            /* 跨full Container set get */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1] */

            rbmSetBitRange(rbm, 4096 * 2 + 1000, 4096 * 2 + 2000);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            roaringBitmap* rbm4 = rbmCreate();
            rbmdup(rbm4, rbm);

            bitNum = rbmGetBitRange(rbm4, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            /* 跨empty container set get */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* 跨empty container set get */
            bitNum = rbmGetBitRange(rbm4, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            rbmDestory(rbm4);

            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]  [4096 * 3 + 1000, 4096 * 3 + 2000]*/
            rbmSetBitRange(rbm, 4096 * 3 + 1000, 4096 * 3 + 2000); /* 填充empty */

            bitNum = rbmGetBitRange(rbm, 4096 * 3, 4096 * 4 - 1); /* container 级别验证 */
            test_assert(bitNum == 1001);

            /* 整个roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 128 - 1); /* container 级别验证 */
            test_assert(bitNum == 6294);

            roaringBitmap* rbm5 = rbmCreate();
            rbmdup(rbm5, rbm);

            bitNum = rbmGetBitRange(rbm5, 4096 * 3, 4096 * 4 - 1); /* container 级别验证 */
            test_assert(bitNum == 1001);

            /* 整个roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm5, 0, 4096 * 128 - 1); /* container 级别验证 */
            test_assert(bitNum == 6294);

            rbmDestory(rbm);
            rbmDestory(rbm5);
        }

        TEST("roaring-bitmap: set get duplicate isEqual test") {
            roaringBitmap* rbm = rbmCreate();
            uint32_t bitNum = 0;

            /* 个数元素 量级 */
            /* 正常测 */
            /* [0, 8] */
            rbmSetBitRange(rbm, 0, 8);

            bitNum = rbmGetBitRange(rbm, 0, 10);
            test_assert(bitNum == 9);

            bitNum = rbmGetBitRange(rbm, 1, 1);
            test_assert(bitNum == 1);

            bitNum = rbmGetBitRange(rbm, 9, 9);
            test_assert(bitNum == 0);

            /* 边界测 */
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

            /* 边界测 */
            bitNum = rbmGetBitRange(rbm1, 20, 20);
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm1, 8, 200);
            test_assert(bitNum == 1);

            test_assert(1 == rbmIsEqual(rbm1, rbm));

            rbmSetBitRange(rbm1, 9, 9);

            test_assert(0 == rbmIsEqual(rbm1, rbm));
            rbmDestory(rbm1);

            /* array container 量级 */
            /* 正常测 */
            /* [0, 8]  [10, 200]*/
            rbmSetBitRange(rbm, 10, 200);
            bitNum = rbmGetBitRange(rbm, 0, 200);
            test_assert(bitNum == 200);

            bitNum = rbmGetBitRange(rbm, 9, 9);  /* 界内 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm, 100, 300);  /* 跨边界 */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm, 201, 400); /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 201, 201);  /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm, 196, 205); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm, 9, 205); /* 横跨 set范围 */
            test_assert(bitNum == 191);

            roaringBitmap* rbm2 = rbmCreate();
            rbmdup(rbm2, rbm);

            bitNum = rbmGetBitRange(rbm2, 9, 9);  /* 界内 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm2, 100, 300);  /* 跨边界 */
            test_assert(bitNum == 101);

            bitNum = rbmGetBitRange(rbm2, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm2, 201, 400); /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 201, 201);  /* 边界外 */
            test_assert(bitNum == 0);

            bitNum = rbmGetBitRange(rbm2, 196, 205); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm2, 9, 205); /* 横跨 set范围 */
            test_assert(bitNum == 191);

            test_assert(1 == rbmIsEqual(rbm2, rbm));

            /* 增量修改 rbm */
            rbmSetBitRange(rbm, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm, 200, 300);  /* 边界set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 300);

            test_assert(0 == rbmIsEqual(rbm2, rbm));

            /* 增量修改 rbm2 */
            rbmSetBitRange(rbm2, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 200);

            rbmSetBitRange(rbm2, 200, 300);  /* 边界set */

            /* [0, 8]  [10, 300] */
            bitNum = rbmGetBitRange(rbm2, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 300);

            test_assert(1 == rbmIsEqual(rbm2, rbm));
            rbmDestory(rbm2);

            /* Bitmap container 量级 */
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

            bitNum = rbmGetBitRange(rbm3, 100, 150);  /* 界内 */
            test_assert(bitNum == 51);

            bitNum = rbmGetBitRange(rbm3, 8, 100);  /* 跨边界 */
            test_assert(bitNum == 92);

            bitNum = rbmGetBitRange(rbm3, 996, 1005); /* 跨边界 */
            test_assert(bitNum == 5);

            bitNum = rbmGetBitRange(rbm3, 9, 1005); /* 横跨 set范围 */
            test_assert(bitNum == 991);

            test_assert(1 == rbmIsEqual(rbm3, rbm));

            /* 增量修改rbm */
            rbmSetBitRange(rbm, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 1000);

            test_assert(1 == rbmIsEqual(rbm3, rbm));

            /* 增量修改rbm3 */
            rbmSetBitRange(rbm3, 150, 160);  /* 重复set */

            bitNum = rbmGetBitRange(rbm3, 0, 4095); /* container 级别验证 */
            test_assert(bitNum == 1000);

            test_assert(1 == rbmIsEqual(rbm3, rbm));

            /* rbm 跨 Container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 2); /* container 级别验证 */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm, 4096 - 5, 4096 + 5); /* 区间跨container验证 */
            test_assert(bitNum == 11);

            test_assert(0 == rbmIsEqual(rbm3, rbm));

            /* rbm3 跨 Container set get */
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]*/
            rbmSetBitRange(rbm3, 4000, 4096 + 100);

            bitNum = rbmGetBitRange(rbm3, 0, 4096 * 2); /* container 级别验证 */
            test_assert(bitNum == 1197);

            bitNum = rbmGetBitRange(rbm3, 4096 - 5, 4096 + 5); /* 区间跨container验证 */
            test_assert(bitNum == 11);

            test_assert(1 == rbmIsEqual(rbm3, rbm));
            rbmDestory(rbm3);

            /* 跨full Container set get */
            rbmSetBitRange(rbm, 4096 * 2, 4096 * 3 - 1);
            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1] */

            rbmSetBitRange(rbm, 4096 * 2 + 1000, 4096 * 2 + 2000);

            bitNum = rbmGetBitRange(rbm, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            roaringBitmap* rbm4 = rbmCreate();
            rbmdup(rbm4, rbm);

            bitNum = rbmGetBitRange(rbm4, 4000, 4096 * 2 + 1000);
            test_assert(bitNum == 1198);

            /* 跨empty container set get */
            bitNum = rbmGetBitRange(rbm, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* 跨empty container set get */
            bitNum = rbmGetBitRange(rbm4, 4096 * 3 - 1000, 4096 * 3 + 1000);
            test_assert(bitNum == 1000);

            /* [0, 8]  [10, 1000]  [4000, 4096 + 100]  [4096 * 2, 4096 * 3 - 1]  [4096 * 3 + 1000, 4096 * 3 + 2000]*/
            rbmSetBitRange(rbm, 4096 * 3 + 1000, 4096 * 3 + 2000); /* 填充empty */

            /* rbm update 之后， 不再相等*/
            test_assert(0 == rbmIsEqual(rbm4, rbm));

            bitNum = rbmGetBitRange(rbm, 4096 * 3, 4096 * 4 - 1); /* container 级别验证 */
            test_assert(bitNum == 1001);

            /* 整个roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm, 0, 4096 * 128 - 1); /* container 级别验证 */
            test_assert(bitNum == 6294);

            rbmDestory(rbm4);

            roaringBitmap* rbm5 = rbmCreate();
            rbmdup(rbm5, rbm);

            bitNum = rbmGetBitRange(rbm5, 4096 * 3, 4096 * 4 - 1); /* container 级别验证 */
            test_assert(bitNum == 1001);

            /* 整个roaring Bitmap get */

            bitNum = rbmGetBitRange(rbm5, 0, 4096 * 128 - 1); /* container 级别验证 */
            test_assert(bitNum == 6294);

            test_assert(1 == rbmIsEqual(rbm5, rbm));

            rbmDestory(rbm);
            rbmDestory(rbm5);
        }

            /* 第一个为 array， 第二个为Bitmap， 第三个为 empty, 第四个为full */
        /*TEST("roaring bitmap: container save 14bits") {

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
        } */

        /* 第一个为 array， 第二个为Bitmap， 第三个为 empty, 第四个为full */
        /*
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

        } */

        /* 500W QPS: [set]: 1/3 [get]: 2/3  total time = 1410622us  */

        TEST("roaring bitmap: perf") {
            size_t querytimes = 500000;

            long long start = ustime();
            roaringBitmap* rbm = rbmCreate();
            uint32_t maxBitNum = 131072;

            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bitIdx = i % maxBitNum;

                uint32_t bitNum = rbmGetBitRange(rbm, bitIdx, bitIdx);
                UNUSED(bitNum);
                rbmSetBitRange(rbm, bitIdx, bitIdx);
                bitNum = rbmGetBitRange(rbm, 0, bitIdx);

                if (bitIdx == maxBitNum - 1) {
                    rbmClearBitRange(rbm, 0, bitIdx / 2);
                }

            }
            printf("[bitmap set get]: %lld\n", ustime() - start);

            rbmDestory(rbm);
        }

        /* 单测接口性能数据 , 单位 TIME/OP (ns)：
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
                uint32_t bitIdx = i % maxBitNum;
                rbmSetBitRange(rbm, bitIdx, bitIdx);
            }
            printf("[bitmap single set]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bitIdx = i % maxBitNum;

                uint32_t bitNum = rbmGetBitRange(rbm, bitIdx, bitIdx);
                UNUSED(bitNum);
            }
            printf("[bitmap single get]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bitIdx = i % maxBitNum;

                uint32_t bitNum = rbmGetBitRange(rbm, 0, bitIdx);
                UNUSED(bitNum);
            }
            printf("[bitmap range get]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bitIdx = i % maxBitNum;

                rbmClearBitRange(rbm, bitIdx, bitIdx);
            }
            printf("[bitmap single clear]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bitIdx = i % maxBitNum;
                rbmSetBitRange(rbm, 0, bitIdx);
            }
            printf("[bitmap range set]: %lld\n", (ustime() - start) / 500);

            start = ustime();
            for (uint32_t i = 0; i < querytimes; i++) {
                uint32_t bitIdx = i % maxBitNum;

                rbmClearBitRange(rbm, 0, bitIdx);
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
