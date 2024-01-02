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

#ifndef __CTRIP_ROARING_BITMAP_H__
#define __CTRIP_ROARING_BITMAP_H__

#include <stdint.h>
#include <stddef.h>

typedef uint16_t arrayContainer;
typedef uint8_t bitmapContainer;

/* There are 3 kinds of container:
 *   arrray Container: sorted array of uint16_t (bit index)
 *   bitmap container: raw bitmap
 *   full container: when all bits set, containerInside is NULL
 */
#define CONTAINER_TYPE_ARRAY 0
#define CONTAINER_TYPE_BITMAP 1
#define CONTAINER_TYPE_FULL 2

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

typedef struct roaringBitmap {
    uint8_t bucketsNum;
    uint8_t* buckets;
    roaringContainer** containers;
} roaringBitmap;

roaringBitmap* rbmCreate(void);

void rbmDestory(roaringBitmap* rbm);

void rbmSetBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit);

void rbmClearBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit);

uint32_t rbmGetBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit);

void rbmdup(roaringBitmap* destRbm, roaringBitmap* srcRbm);

int rbmIsEqual(roaringBitmap* destRbm, roaringBitmap* srcRbm);

/* return pos of the nth one bit, from the startPos*/
/* todo imp */
uint32_t rbmGetBitPos(roaringBitmap* rbm, uint32_t startPos, uint32_t nthBit);

#endif // __CTRIP_ROARING_BITMAP_H__
