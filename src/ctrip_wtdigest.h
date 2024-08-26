/* Copyright (c) 2024, ctrip.com * All rights reserved.
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


#ifndef __CTRIP_WTDIGEST_H__
#define __CTRIP_WTDIGEST_H__

#include <stdint.h>
#include "math.h"

#define WTD_DEFAULT_NUM_BUCKETS 6

/*
  Window Tdigest algorithm is based on tdigest(MergingDigest). There are td_num buckets 
  (tdigest structs) in one window tdigest. And, caller need to specify the time of window(window_ms).
  Window tdigest will save values in each bucket inside, and reset the old bucket 
  when it exists longer than window_ms. So the returned value of wtdigestQuantile 
  is guaranteed to be within the window_ms.
 */

typedef struct wtdigest_t wtdigest;

typedef long long (*wtdigestNowtime)();

/** 
 * @param num_buckets, recommended to be 1~127, not supported to modify after creating, 
 * which is positively related to memory occpuied,
 * 1KB for each bucket. 
 */
wtdigest* wtdigestCreate(uint8_t num_buckets, wtdigestNowtime nowtime);

void wtdigestDestroy(wtdigest* wt);

/** 
 * @param window_ms, default as 3600000ms. 
 */
void wtdigestSetWindow(wtdigest* wt, unsigned long long window_ms);

unsigned long long wtdigestGetWindow(wtdigest* wt);

void wtdigestReset(wtdigest* wt);

unsigned long long wtdigestGetRunnningTime(wtdigest* wt);

long long wtdigestSize(wtdigest* wt);

/**
 * Adds a value to a wtdigest.
 * @param val The value to add.
 * @param weight The weight of this value, sugggested to set to 1 normally.
 * @return return 0 if success.
 * time complexity : nlog(n)
 */
int wtdigestAdd(wtdigest* wt, double val, unsigned long long weight);

/**
 * Returns an estimate of the cutoff such that a specified fraction of the value
 * added to this wtdigest would be less than or equal to the cutoff.
 *
 * @param nowtime_ms
 * @param q The desired fraction.
 * @return The value x such that cdf(x) == q,（cumulative distribution function，CDF).
 * time complexity : nlog(n)
 */
double wtdigestQuantile(wtdigest* wt, double q);

#endif
