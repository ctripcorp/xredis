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

typedef struct {
    swapRdbSaveErrType err_type;
    int dbid;
    size_t klen;
    char key[];
} swapRdbSaveErr;

void loadClientKeyRequestFinished(client *c, swapCtx *ctx) {
    robj *key = ctx->key_request->key;
    if (ctx->errcode) {
        clientSwapError(c,ctx->errcode);
        server.swap_load_err_cnt++;
        serverLog(LL_VERBOSE, "swap.load fail,code=%d,key=%s", ctx->errcode, (char*)key->ptr);
    }
    incrRefCount(key);
    c->keyrequests_count--;
    serverAssert(c->client_hold_mode == CLIENT_HOLD_MODE_EVICT);
    clientReleaseLocks(c,ctx);
    decrRefCount(key);

    server.swap_load_inprogress_count--;
}

int submitLoadClientRequest(client *c, robj *key, int oom_sensitive) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    int flags = (int)c->cmd->intention_flags;
    if (oom_sensitive) flags |= SWAP_OOM_CHECK;
    getKeyRequestsPrepareResult(&result,1);
    incrRefCount(key);
    getKeyRequestsAppendSubkeyResult(&result,REQUEST_LEVEL_KEY,key,0,NULL,
                                     c->cmd->intention,flags,c->db->id);
    c->keyrequests_count++;
    submitDeferredClientKeyRequests(c,&result,loadClientKeyRequestFinished,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);

    server.swap_load_inprogress_count++;
    return 1;
}

int tryLoadKey(redisDb *db, robj *key, int oom_sensitive) {
    int old_keyrequests_count;
    client *load_client = server.load_clients[db->id];

    /* skip pure hot key */
    if (lookupKey(db, key, LOOKUP_NOTOUCH) != NULL && lookupMeta(db, key) == NULL) {
        return 0;
    }

    old_keyrequests_count = load_client->keyrequests_count;
    submitLoadClientRequest(load_client,key, oom_sensitive);
    if (load_client->keyrequests_count == old_keyrequests_count) {
        return 0;
    } else {
        return 1;
    }
}

void swapLoadCommand(client *c) {
    int i, nload = 0;

    for (i = 1; i < c->argc; i++) {
        nload += tryLoadKey(c->db,c->argv[i], 0);
    }

    addReplyLongLong(c, nload);
}

void openSwapChildErrPipe(void) {
    if (pipe(server.swap_child_err_pipe) == -1) {
        /* On error our two file descriptors should be still set to -1,
         * but we call anyway closeChildInfoPipe() since can't hurt. */
        closeSwapChildErrPipe();
    } else if (anetNonBlock(NULL,server.swap_child_err_pipe[0]) != ANET_OK) {
        closeSwapChildErrPipe();
    } else {
        server.swap_child_err_nread = 0;
    }
}

void closeSwapChildErrPipe(void) {
    if (server.swap_child_err_pipe[0] != -1 ||
        server.swap_child_err_pipe[1] != -1)
    {
        close(server.swap_child_err_pipe[0]);
        close(server.swap_child_err_pipe[1]);
        server.swap_child_err_pipe[0] = -1;
        server.swap_child_err_pipe[1] = -1;
        server.swap_child_err_nread = 0;
    }
}

void handleSwapChildErr(swapRdbSaveErrType err_type, int dbid, sds key) {
    int handled = 0;
    if (err_type == SAVE_ERR_META_LEN_MISMATCH) {
        handled = tryLoadKey(server.db + dbid, createObject(OBJ_STRING, key), 1);
    }

    if (!handled) {
        serverLog(LL_VERBOSE, "handle child err %d fail,db=%d,key=%s", err_type, dbid, key);
    }
}

void sendSwapChildErr(swapRdbSaveErrType err_type, int dbid, sds key) {
    /* only handle err from child process */
    if (server.swap_child_err_pipe[1] == -1) return;
    serverAssert(err_type > SAVE_ERR_NONE && err_type < SAVE_ERR_UNRECOVERABLE);

    size_t klen = sdslen(key);
    size_t wlen = sizeof(swapRdbSaveErr) + klen;
    swapRdbSaveErr* err = zmalloc(wlen);

    err->err_type = err_type;
    err->dbid = dbid;
    err->klen = klen;
    memcpy(err->key, key, klen);

    if (write(server.swap_child_err_pipe[1], err, wlen) != (ssize_t)wlen) {
        /* Nothing to do on error, this will be detected by the other side. */
    }
}

int readSwapChildErr(swapRdbSaveErrType *err_type, int *db_id, sds *key) {
    static swapRdbSaveErr buffer = {0};
    static sds key_buffer = NULL;
    size_t buf_len = sizeof(swapRdbSaveErr), key_len;
    ssize_t nread;

    /* 1. read swapRdbSaveErr, $buf_len bytes */
    if (key_buffer == NULL) {
        nread = read(server.swap_child_err_pipe[0], (char *)&buffer + server.swap_child_err_nread, buf_len - server.swap_child_err_nread);
        if (nread > 0) server.swap_child_err_nread += nread;

        if (server.swap_child_err_nread == buf_len) {
            /* read swapRdbSaveErr done, make room for key reading */
            server.swap_child_err_nread = 0;
            key_buffer = sdsempty();
            key_buffer = sdsMakeRoomForExact(key_buffer, buffer.klen);
            sdssetlen(key_buffer, buffer.klen);
        }
    }

    /* 2. read key, $key_len bytes */
    if (key_buffer != NULL) {
        key_len = buffer.klen;
        nread = read(server.swap_child_err_pipe[0], key_buffer + server.swap_child_err_nread, key_len - server.swap_child_err_nread);
        if (nread > 0) server.swap_child_err_nread += nread;

        if (server.swap_child_err_nread == key_len) {
            /* read key done, move to *key */
            server.swap_child_err_nread = 0;
            *err_type = buffer.err_type;
            *db_id = buffer.dbid;
            *key = key_buffer;
            key_buffer = NULL;
            return 1;
        }
    }

    return 0;
}

static inline int reachedSwapLoadInprogressLimit(size_t mem_tofree) {
    if (server.swap_evict_inprogress_limit < 0) return 0;

    int inprogress_limit = server.swap_evict_inprogress_limit - swapEvictInprogressLimit(mem_tofree);
    return server.swap_load_inprogress_count >= inprogress_limit;
}

static inline int swapLoadMayOOM(size_t mem_used) {
    if (server.maxmemory == 0) return 0;
    /* expect mem_used < [100 + (swap_maxmemory_oom_percentage - 100)/2]*maxmemory/100 */
    unsigned long long mem_limit = (50 + server.swap_maxmemory_oom_percentage/2)*server.maxmemory/100;
    return mem_used >= mem_limit;
}

void receiveSwapChildErrs(void) {
    if (server.swap_child_err_pipe[0] == -1) return;

    swapRdbSaveErrType err_type;
    int db_id;
    sds key;

    size_t mem_used = ctrip_getUsedMemory();
    size_t mem_tofree = mem_used > server.maxmemory ? mem_used - server.maxmemory : 0;
    if (swapLoadMayOOM(mem_used) || reachedSwapLoadInprogressLimit(mem_tofree)) {
        server.swap_load_paused = 1;
        return;
    }

    server.swap_load_paused = 0;
    while (readSwapChildErr(&err_type, &db_id, &key)) {
        handleSwapChildErr(err_type, db_id, key);
        if (reachedSwapLoadInprogressLimit(mem_tofree)) break;
    }
}
