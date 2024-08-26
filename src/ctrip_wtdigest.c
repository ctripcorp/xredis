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

#include "ctrip_swap.h"
#include "ctrip_wtdigest.h"
#include "ctrip_wtdigest_malloc.h"
#include "../deps/tdigest/tdigest.h"

#define DEFAULT_COMPRESSION 100
#define DEFAULT_WINDOW_MS 3600000

struct wtdigest_t {
    uint8_t num_buckets;
    td_histogram_t **buckets;
    unsigned long long last_reset_time;
    uint8_t cur_read_index;
    unsigned long long window_ms;
    unsigned long long begin_time;
    wtdigestNowtime nowtime;
};

wtdigest* wtdigestCreate(uint8_t num_buckets, wtdigestNowtime nowtime)
{
    serverAssert(num_buckets != 0);
    wtdigest *wt = wtdigest_malloc(sizeof(wtdigest));
    wt->num_buckets = num_buckets;

    wt->buckets = wtdigest_malloc(num_buckets * sizeof(td_histogram_t *));
    for (uint8_t i = 0; i < num_buckets; i++) {
        wt->buckets[i] = td_new(DEFAULT_COMPRESSION);
        serverAssert(wt->buckets[i] != NULL);
    }

    wt->cur_read_index = 0;
    wt->window_ms = DEFAULT_WINDOW_MS;
    wt->nowtime = nowtime;
    wt->begin_time = wt->nowtime();
    wt->last_reset_time = wt->nowtime();
    return wt;
}

void wtdigestDestroy(wtdigest* wt)
{
    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        serverAssert(wt->buckets[i] != NULL);
        td_free(wt->buckets[i]);
    }
    wtdigest_free(wt->buckets);
    wtdigest_free(wt);
}

void wtdigestSetWindow(wtdigest* wt, unsigned long long window_ms)
{
    wt->window_ms = window_ms;
}

unsigned long long wtdigestGetWindow(wtdigest* wt)
{
    return wt->window_ms;
}

unsigned long long wtdigestGetRunnningTime(wtdigest* wt)
{
    return wt->nowtime() - wt->begin_time;
}

void wtdigestReset(wtdigest* wt)
{
    for (unsigned long long i = 0; i < wt->num_buckets; i++) {
        td_reset(wt->buckets[i]);
    }

    wt->last_reset_time = wt->nowtime();
    wt->cur_read_index = 0;
    wt->begin_time = wt->nowtime();
}

void resetBucketsIfNeed(wtdigest* wt)
{
    unsigned long long reset_period = wt->window_ms / wt->num_buckets;
    unsigned long long time_passed = wt->nowtime() - wt->last_reset_time;

    if (time_passed < reset_period) {
        return;
    }

    unsigned long long num_buckets_passed = time_passed / reset_period;
    num_buckets_passed = MIN(wt->num_buckets, num_buckets_passed);

    for (unsigned long long i = 0; i < num_buckets_passed; i++) {
        uint8_t reset_index = (wt->cur_read_index + i) % wt->num_buckets;
        td_reset(wt->buckets[reset_index]);
    }
    wt->cur_read_index = (wt->cur_read_index + num_buckets_passed) % wt->num_buckets;
    wt->last_reset_time = wt->nowtime();
    return;
}

int wtdigestAdd(wtdigest* wt, double val, unsigned long long weight)
{
    resetBucketsIfNeed(wt);

    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        int res = td_add(wt->buckets[i], val, weight);
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

double wtdigestQuantile(wtdigest* wt, double q)
{
    resetBucketsIfNeed(wt);
    return td_quantile(wt->buckets[wt->cur_read_index], q);
}

long long wtdigestSize(wtdigest* wt)
{
    resetBucketsIfNeed(wt);
    return td_size(wt->buckets[wt->cur_read_index]);
}

#ifdef REDIS_TEST
int wtdigestTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

        TEST("wtdigest: create destroy") {
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);
            wtdigestDestroy(wt);
        }

        TEST("wtdigest: set & get window & get runnning time") {

            unsigned long long start_time = mstime();
            usleep(10000);
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);
            test_assert(wt->begin_time >= start_time + 10);

            test_assert(wtdigestGetWindow(wt) == DEFAULT_WINDOW_MS);
            wtdigestSetWindow(wt, 7200000);
            test_assert(wtdigestGetWindow(wt) == 7200000);

            usleep(10000);
            unsigned long long running_time = wtdigestGetRunnningTime(wt);
            test_assert(running_time >= 10);

            unsigned long long end_time = mstime();
            test_assert(end_time - start_time >= running_time);
            test_assert(end_time - start_time >= 20);
            wtdigestDestroy(wt);
        }

        TEST("wtdigest: basic add Quantile") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);

            test_assert(wtdigestSize(wt) == 0);

            for (int i = 0; i < 30; i++) {
                wtdigestAdd(wt, 200, 1);
            }
            test_assert(wtdigestSize(wt) == 30);

            for (int i = 0; i < 50; i++) {
                wtdigestAdd(wt, 100, 1);
            }
            test_assert(wtdigestSize(wt) == 80);

            for (int i = 0; i < 20; i++) {
                wtdigestAdd(wt, 300, 1);
            }

            wtdigestAdd(wt, 400, 1);
            wtdigestAdd(wt, 4, 1);

            int q;

            q = (int)wtdigestQuantile(wt, 0.001);
            test_assert(4 == q);

            q = (int)wtdigestQuantile(wt, 0.5);
            test_assert(150 == q);

            q = (int)wtdigestQuantile(wt, 0.80);
            test_assert(268 == q);

            q = (int)wtdigestQuantile(wt, 0.99);
            test_assert(300 == q);

            q = (int)wtdigestQuantile(wt, 0.999);
            test_assert(400 == q);

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: boundary value test") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);
            double q;

            for (int i = 0; i < 300; i++) {
                wtdigestAdd(wt, __DBL_MAX__, 1);
            }

            q = wtdigestQuantile(wt, 0.99);
            test_assert(__DBL_MAX__ == q);

            wtdigestReset(wt);
            for (int i = 0; i < 300; i++) {
                wtdigestAdd(wt, __DBL_MIN__, 1);
            }
            q = wtdigestQuantile(wt, 0.99);
            test_assert(__DBL_MIN__ == q);

            wtdigestReset(wt);
            for (int i = 0; i < 300; i++) {
                wtdigestAdd(wt, (double)LLONG_MAX, 1);
            }
            q = wtdigestQuantile(wt, 0.99);
            test_assert(LLONG_MAX == q);

            wtdigestReset(wt);
            for (int i = 0; i < 300; i++) {
                wtdigestAdd(wt, (double)LLONG_MIN, 1);
            }
            q = wtdigestQuantile(wt, 0.99);
            test_assert(LLONG_MIN == q);

            wtdigestReset(wt);
            for (int i = 0; i < 100; i++) {
                wtdigestAdd(wt, (double)LLONG_MIN, 1);
            }
            for (int i = 0; i < 100; i++) {
                wtdigestAdd(wt, (double)LLONG_MAX, 1);
            }
            q = wtdigestQuantile(wt, 0.99);
            test_assert(LLONG_MAX == q);

            wtdigestReset(wt);
            for (int i = 0; i < 100; i++) {
                wtdigestAdd(wt, __DBL_MAX__, 1);
            }
            for (int i = 0; i < 100; i++) {
                wtdigestAdd(wt, __DBL_MIN__, 1);
            }
            q = wtdigestQuantile(wt, 0.99);
            test_assert(__DBL_MAX__ == q);

            wtdigestReset(wt);
            for (int i = 0; i < 100; i++) {
                wtdigestAdd(wt, (double)LLONG_MAX, 1);
            }
            for (int i = 0; i < 100; i++) {
                wtdigestAdd(wt, 1, 1);
            }
            q = wtdigestQuantile(wt, 0.8);
            test_assert(LLONG_MAX == q);

            q = wtdigestQuantile(wt, 0.55);
            test_assert(LLONG_MAX == q);

            q = wtdigestQuantile(wt, 0.5);
            test_assert(LLONG_MAX > q);
            test_assert(1 < q);

            q = wtdigestQuantile(wt, 0.35);
            test_assert(1 == q);

            q = wtdigestQuantile(wt, 0.2);
            test_assert(1 == q);

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: bucket rotate reading") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);

            /* reset period = 100ms */
            wtdigestSetWindow(wt, 100 * WTD_DEFAULT_NUM_BUCKETS);

            /* rotate for two circle */
            for (int i = 0; i < WTD_DEFAULT_NUM_BUCKETS * 2; i++) {
                wtdigestAdd(wt, 10, 1);
                test_assert((i % WTD_DEFAULT_NUM_BUCKETS) == wt->cur_read_index);

                long long size = td_size(wt->buckets[wt->cur_read_index]);
                test_assert(size <= 1 * WTD_DEFAULT_NUM_BUCKETS);

                uint8_t last_bucket_idx = (wt->cur_read_index + WTD_DEFAULT_NUM_BUCKETS - 1) % WTD_DEFAULT_NUM_BUCKETS;
                long long last_bucket_size = td_size(wt->buckets[last_bucket_idx]);
                test_assert(last_bucket_size == 1);

                long long now_time = mstime();
                test_assert(now_time - wt->last_reset_time < 100);
                usleep(100000);
            }

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: bucket cur_read_index rotate") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);

            /* reset period = 100ms */
            wtdigestSetWindow(wt, 100 * WTD_DEFAULT_NUM_BUCKETS);
            wtdigestAdd(wt, 10, 1);

            uint8_t index1 = wt->cur_read_index;
            usleep(300000);

            (void)wtdigestQuantile(wt, 0.5);

            /* reset and pass 3 buckets */
            uint8_t index2 = wt->cur_read_index;
            test_assert(index2 - index1 == 3);

            usleep(400000);

            (void)wtdigestQuantile(wt, 0.5);

            uint8_t index3 = wt->cur_read_index;
            test_assert(index3 == 1);

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: rotate add Quantile") {
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);

            /* reset period = 1ms */
            wtdigestSetWindow(wt, 1 * WTD_DEFAULT_NUM_BUCKETS);

            /* rotate  */
            for (int j = 0; j < WTD_DEFAULT_NUM_BUCKETS * 100; j++) {
                
                /* add different value at each reset period */
                if (j % WTD_DEFAULT_NUM_BUCKETS <= 2) {
                    for (int i = 0; i < 10; i++) {
                        wtdigestAdd(wt, 100, 1);
                    }
                }

                if (j % WTD_DEFAULT_NUM_BUCKETS == 3 || j % WTD_DEFAULT_NUM_BUCKETS == 4) {
                    for (int i = 0; i < 10; i++) {
                        wtdigestAdd(wt, 200, 1);
                    }
                }

                usleep(300);

                if (j % WTD_DEFAULT_NUM_BUCKETS == 5) {
                    for (int i = 0; i < 10; i++) {
                        wtdigestAdd(wt, 300, 1);
                    }
                }

                int q = (int)wtdigestQuantile(wt, 0.5);

                test_assert(100 <= q);
                test_assert(300 >= q);

            }
            wtdigestDestroy(wt);
        }

        TEST("wtdigest: reset then add") {
        
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, mstime);
        
            /* reset period = 100ms */
            wtdigestSetWindow(wt, 100 * WTD_DEFAULT_NUM_BUCKETS);
            usleep(200000);
            wtdigestAdd(wt, 100, 1);
            test_assert(wt->cur_read_index == 2);

            wtdigestReset(wt);

            for (unsigned long long i = 0; i < wt->num_buckets; i++) {
                test_assert(0 == td_size(wt->buckets[i]));
            }
            test_assert(wt->cur_read_index == 0);

            double q = wtdigestQuantile(wt, 0.5);
            test_assert(isnan(q));

            q = wtdigestQuantile(wt, 0.9);
            test_assert(isnan(q));

            /* restart to work */
            usleep(200000);       
            wtdigestAdd(wt, 100, 1);
            test_assert(wt->cur_read_index == 2);

            test_assert(1 == td_size(wt->buckets[wt->cur_read_index]));

            wtdigestDestroy(wt);
        }

        return error;
}
#endif 