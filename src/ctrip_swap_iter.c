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

/* TODO currently rocks iter could scan obselete data that are already swapped
 * in, althouth it will be filtered when search db.dict, still it wastes io
 * and cpu cycle. delete those data in customized filter for those obseletes. */

static int rocksIterWaitReady(rocksIter* it) {
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    pthread_mutex_lock(&cq->buffer_lock);
    while (1) {
        /* iterResult ready */
        if (cq->processed_count < cq->buffered_count)
            break;
        /* iter finished */
        if (cq->iter_finished) {
            pthread_mutex_unlock(&cq->buffer_lock);
            return 0;
        }
        /* wait io thread */
        pthread_cond_wait(&cq->ready_cond, &cq->buffer_lock);
    }
    pthread_mutex_unlock(&cq->buffer_lock);
    return 1;
}

static void rocksIterNotifyReady(rocksIter* it) {
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    pthread_mutex_lock(&cq->buffer_lock);
    cq->buffered_count++;
    pthread_cond_signal(&cq->ready_cond);
    pthread_mutex_unlock(&cq->buffer_lock);
}

static void rocksIterNotifyFinshed(rocksIter* it) {
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    pthread_mutex_lock(&cq->buffer_lock);
    cq->iter_finished = 1;
    pthread_cond_signal(&cq->ready_cond);
    pthread_mutex_unlock(&cq->buffer_lock);
}

static int rocksIterWaitVacant(rocksIter *it) {
    int64_t slots, occupied;
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    /* wait untill there are vacant slots in buffer. */
    pthread_mutex_lock(&cq->buffer_lock);
    while (1) {
        occupied = cq->buffered_count - cq->processed_count;
        slots = cq->buffer_capacity - occupied;
        if (slots < 0) {
            serverPanic("CQ slots is negative.");
        } else if (slots == 0) {
            pthread_cond_wait(&cq->vacant_cond, &cq->buffer_lock);
        } else {
            break;
        }
    }
    pthread_mutex_unlock(&cq->buffer_lock);
    return slots;
}

static void rocksIterNotifyVacant(rocksIter* it) {
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    pthread_mutex_lock(&cq->buffer_lock);
    cq->processed_count++;
    pthread_cond_signal(&cq->vacant_cond);
    pthread_mutex_unlock(&cq->buffer_lock);
}

void *rocksIterIOThreadMain(void *arg) {
    rocksIter *it = arg;
    bufferedIterCompleteQueue *cq = it->buffered_cq;

    redis_set_thread_title("rocks_iter");

    while (!cq->iter_finished) {
        int64_t slots = rocksIterWaitVacant(it);

        /* there only one producer, slots will decrease only by current
         * thread, we can produce multiple iterResult in one loop. */
        while (slots--) {
            iterResult *cur;
            int curidx;
            const char *rawkey, *rawval;
            size_t rklen, rvlen;

            if (!rocksdb_iter_valid(it->rocksdb_iter)) {
                rocksIterNotifyFinshed(it);
                break;
            }

            curidx = cq->buffered_count % cq->buffer_capacity;
            cur = cq->buffered + curidx;

            rawkey = rocksdb_iter_key(it->rocksdb_iter,&rklen);
            rawval = rocksdb_iter_value(it->rocksdb_iter,&rvlen);

            if (rklen > ITER_CACHED_MAX_KEY_LEN) {
                cur->rawkey = sdsnewlen(rawkey, rklen);
            } else {
                memcpy(cur->cached_key, rawkey, rklen);
                cur->cached_key[rklen] = '\0';
                sdssetlen(cur->cached_key, rklen);
                cur->rawkey = cur->cached_key;
            }

            if (rvlen > ITER_CACHED_MAX_VAL_LEN) {
                cur->rawval = sdsnewlen(rawval, rvlen);
            } else {
                memcpy(cur->cached_val, rawval, rvlen);
                cur->cached_val[rvlen] = '\0';
                sdssetlen(cur->cached_val, rvlen);
                cur->rawval = cur->cached_val;
            }

            rocksIterNotifyReady(it);

            rocksdb_iter_next(it->rocksdb_iter);
        }
    }
    serverLog(LL_WARNING, "Rocks iter thread exit.");

    return NULL;
}

bufferedIterCompleteQueue *bufferedIterCompleteQueueNew(int capacity) {
    int i;
    bufferedIterCompleteQueue* buffered_cq;

    buffered_cq = zmalloc(sizeof(bufferedIterCompleteQueue));
    memset(buffered_cq, 0 ,sizeof(*buffered_cq));
    buffered_cq->buffer_capacity = capacity;

    buffered_cq->buffered = zmalloc(capacity*sizeof(iterResult));
    for (i = 0; i < capacity; i++) {
        iterResult *iter_result = buffered_cq->buffered+i;
        iter_result->cached_key = sdsnewlen(NULL, ITER_CACHED_MAX_KEY_LEN);
        iter_result->rawkey = NULL;
        iter_result->cached_val = sdsnewlen(NULL, ITER_CACHED_MAX_VAL_LEN);
        iter_result->rawval = NULL;
    }

    pthread_mutex_init(&buffered_cq->buffer_lock, NULL);
    pthread_cond_init(&buffered_cq->ready_cond, NULL);
    pthread_mutex_init(&buffered_cq->buffer_lock, NULL);
    pthread_cond_init(&buffered_cq->vacant_cond, NULL);
    return buffered_cq;
}

void bufferedIterCompleteQueueFree(bufferedIterCompleteQueue *buffered_cq) {
    if (buffered_cq == NULL) return;
    for (int i = 0; i < buffered_cq->buffer_capacity;i++) {
        iterResult *res = buffered_cq->buffered+i;
        if (res->rawkey != res->cached_key) sdsfree(res->rawkey);
        if (res->rawval != res->cached_val) sdsfree(res->rawval);
        sdsfree(res->rawkey);
        sdsfree(res->rawval);
    }
    zfree(buffered_cq->buffered);
    pthread_mutex_destroy(&buffered_cq->buffer_lock);
    pthread_cond_destroy(&buffered_cq->ready_cond);
    pthread_mutex_destroy(&buffered_cq->buffer_lock);
    pthread_cond_destroy(&buffered_cq->vacant_cond);
    zfree(buffered_cq);
}

rocksIter *rocksCreateIter(rocks *rocks, redisDb *db) {
    int err;
    rocksdb_iterator_t *rocksdb_iter;
    rocksIter *it = zmalloc(sizeof(rocksIter));

    it->rocks = rocks;
    it->db = db;
    rocksdb_iter = rocksdb_create_iterator(rocks->rocksdb, rocks->rocksdb_ropts);
    if (rocksdb_iter == NULL) {
        serverLog(LL_WARNING, "Create rocksdb iterator failed.");
        goto err;
    }
    rocksdb_iter_seek_to_first(rocksdb_iter);
    it->rocksdb_iter = rocksdb_iter;

    it->buffered_cq = bufferedIterCompleteQueueNew(ITER_BUFFER_CAPACITY_DEFAULT);

    if ((err = pthread_create(&it->io_thread, NULL, rocksIterIOThreadMain, it))) {
        serverLog(LL_WARNING, "Create rocksdb iterator thread failed: %s.", strerror(err));
        goto err;
    }

    return it;
err:
    rocksReleaseIter(it);
    return NULL;
}

int rocksIterSeekToFirst(rocksIter *it) {
    return rocksIterWaitReady(it);
}

void rocksIterKeyValue(rocksIter *it, sds *rawkey, sds *rawval) {
    int idx;
    iterResult *cur;
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    idx = cq->processed_count % cq->buffer_capacity;
    cur = it->buffered_cq->buffered+idx;
    if (rawkey) *rawkey = cur->rawkey;
    if (rawval) *rawval = cur->rawval;
}

/* Will block untill at least one result is ready.
 * note that rawkey and rawval are owned by rocksIter. */
int rocksIterNext(rocksIter *it) {
    int idx;
    iterResult *cur;
    bufferedIterCompleteQueue *cq = it->buffered_cq;
    idx = cq->processed_count % cq->buffer_capacity;
    cur = it->buffered_cq->buffered+idx;
    /* clear previos state */
    if (cur->rawkey != cur->cached_key) sdsfree(cur->rawkey);
    if (cur->rawval != cur->cached_val) sdsfree(cur->rawval);
    rocksIterNotifyVacant(it);
    return rocksIterWaitReady(it);
}

void rocksReleaseIter(rocksIter *it) {
    int err;

    if (it == NULL) return;

    if (it->io_thread) {
        if (pthread_cancel(it->io_thread) == 0) {
            if ((err = pthread_join(it->io_thread, NULL)) != 0) {
                serverLog(LL_WARNING, "Iter io thread can't be joined: %s",
                         strerror(err));
            } else {
                serverLog(LL_WARNING, "Iter io thread terminated.");
            }
        }
        it->io_thread = 0;
    }

   if (it->buffered_cq) {
       bufferedIterCompleteQueueFree(it->buffered_cq);
       it->buffered_cq = NULL;
   }

   if (it->rocksdb_iter) {
       rocksdb_iter_destroy(it->rocksdb_iter);
       it->rocksdb_iter = NULL;
   }

   zfree(it);
}

void rocksIterGetError(rocksIter *it, char **error) {
    rocksdb_iter_get_error(it->rocksdb_iter, error);
}

