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

typedef struct roaringBitmap_t roaringBitmap;

roaringBitmap* rbmCreate(void);

void rbmDestory(roaringBitmap* rbm);

void rbmSetBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit);

void rbmClearBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit);

uint32_t rbmGetBitRange(roaringBitmap* rbm, uint32_t minBit, uint32_t maxBit);

void rbmdup(roaringBitmap* destRbm, roaringBitmap* srcRbm);

int rbmIsEqual(roaringBitmap* destRbm, roaringBitmap* srcRbm);

/* Counting bitsNum bits of '1' from front to back， return real number of '1' bits, and output the indexs to idxArr(malloc by caller),  */
uint32_t rbmLocateSetBitPos(roaringBitmap* rbm, uint32_t bitsNum, uint32_t *idxArr);

char *rbmEncode(roaringBitmap* rbm, size_t *plen);

roaringBitmap* rbmDecode(const char *buf, size_t len);

#endif
