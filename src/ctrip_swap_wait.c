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

/* Normally pd is swapCtx, which should not be freed untill binded listener
 * released. so we pass pdfree to listener to free it. */
requestListener *requestListenerCreate(redisDb *db, robj *key,
        requestProceed cb, client *c, void *pd, freefunc pdfree,
        void *msgs) {
    requestListener *listener = zmalloc(sizeof(requestListener));
    UNUSED(msgs);
    listener->db = db;
    if (key) incrRefCount(key);
    listener->key = key;
    listener->proceed = cb;
    listener->c = c;
    listener->pd = pd;
    listener->pdfree = pdfree;
#ifdef SWAP_DEBUG
    listener->msgs = msgs;
#endif
    return listener;
}

void requestListenerRelease(requestListener *listener) {
    if (!listener) return;
    if (listener->key) decrRefCount(listener->key);
    if (listener->pdfree) listener->pdfree(listener->pd);
    zfree(listener);
}

char *requestListenerDump(requestListener *listener) {
    static char repr[64];
    const char *intention = listener->c->cmd ? swapIntentionName(listener->c->cmd->intention) : "<nil>";
    char *cmd = (listener->c && listener->c->cmd) ? listener->c->cmd->name : "<nil>";
    char *key = listener->key ? listener->key->ptr : "<nil>";
    snprintf(repr,sizeof(repr)-1,"(%s:%s:%s)",intention,cmd,key);
    return repr;
}

dictType requestListenersDictType = {
    dictSdsHash,                    /* hash function */
    NULL,                           /* key dup */
    NULL,                           /* val dup */
    dictSdsKeyCompare,              /* key compare */
    dictSdsDestructor,              /* key destructor */
    NULL,                           /* val destructor */
    NULL                            /* allow to expand */
};

static requestListeners *requestListenersCreate(int level, redisDb *db,
        robj *key, requestListeners *parent) {
    requestListeners *listeners;

    listeners = zmalloc(sizeof(requestListeners));
    listeners->listeners = listCreate();
    listeners->nlisteners = 0;
    listeners->parent = parent;
    listeners->level = level;

    switch (level) {
    case REQUEST_LEVEL_SVR:
        listeners->svr.dbnum = server.dbnum;
        listeners->svr.dbs = zmalloc(server.dbnum*sizeof(requestListeners));
        break;
    case REQUEST_LEVEL_DB:
        serverAssert(db);
        listeners->db.db = db;
        listeners->db.keys = dictCreate(&requestListenersDictType, NULL);
        break;
    case REQUEST_LEVEL_KEY:
        serverAssert(key);
        serverAssert(parent->level == REQUEST_LEVEL_DB);
        incrRefCount(key);
        listeners->key.key = key;
        dictAdd(parent->db.keys,sdsdup(key->ptr),listeners);
        break;
    default:
        break;
    }

    return listeners;
}

void requestListenersRelease(requestListeners *listeners) {
    if (!listeners) return;
    serverAssert(!listLength(listeners->listeners));
    listRelease(listeners->listeners);
    listeners->listeners = NULL;

    switch (listeners->level) {
    case REQUEST_LEVEL_SVR:
        zfree(listeners->svr.dbs);
        break;
    case REQUEST_LEVEL_DB:
        dictRelease(listeners->db.keys);
        break;
    case REQUEST_LEVEL_KEY:
        serverAssert(listeners->parent->level == REQUEST_LEVEL_DB);
        dictDelete(listeners->parent->db.keys,listeners->key.key->ptr);
        decrRefCount(listeners->key.key);
        break;
    default:
        break;
    }
    zfree(listeners);
}

sds requestListenersDump(requestListeners *listeners) {
    listIter li;
    listNode *ln;
    sds result = sdsempty();
    char *key;

    switch (listeners->level) {
    case REQUEST_LEVEL_SVR:
        key = "<svr>";
        break;
    case REQUEST_LEVEL_DB:
        key = "<db>";
        break;
    case REQUEST_LEVEL_KEY:
        key = listeners->key.key->ptr;
        break;
    default:
        key = "?";
        break;
    }
    result = sdscatprintf(result,"(level=%s,len=%ld,key=%s):",
            requestLevelName(listeners->level),
            listLength(listeners->listeners), key);

    result = sdscat(result, "[");
    listRewind(listeners->listeners,&li);
    while ((ln = listNext(&li))) {
        requestListener *listener = listNodeValue(ln);
        if (ln != listFirst(listeners->listeners)) result = sdscat(result,",");
        result = sdscat(result,requestListenerDump(listener));
    }
    result = sdscat(result, "]");
    return result;
}

static inline void requestListenersLink(requestListeners *listeners) {
    while (listeners) {
        listeners->nlisteners++;
        listeners = listeners->parent;
    }
}

static inline void requestListenersUnlink(requestListeners *listeners) {
    while (listeners) {
        listeners->nlisteners--;
        listeners = listeners->parent;
    }
}

static void requestListenersPush(requestListeners *listeners,
        requestListener *listener) {
    serverAssert(listeners);
    listAddNodeTail(listeners->listeners, listener);
    requestListenersLink(listeners);
}

requestListener *requestListenersPop(requestListeners *listeners) {
    serverAssert(listeners);
    if (!listLength(listeners->listeners)) return NULL;
    listNode *ln = listFirst(listeners->listeners);
    requestListener *listener = listNodeValue(ln);
    listDelNode(listeners->listeners, ln);
    requestListenersUnlink(listeners);
    return listener;
}

requestListener *requestListenersPeek(requestListeners *listeners) {
    serverAssert(listeners);
    if (!listLength(listeners->listeners)) return NULL;
    listNode *ln = listFirst(listeners->listeners);
    requestListener *listener = listNodeValue(ln);
    return listener;
}

/* return true if current or lower level listeners not finished.
 * - swap should not proceed if current or lower level listeners exists.
 *   (e.g. flushdb shoul not proceed if SWAP GET key exits.)
 * - can't release listeners if current or lower level listeners exists. */
int requestListenersTreeBlocking(requestListeners *listeners) {
    if (listeners && (listLength(listeners->listeners) ||
                listeners->nlisteners > 0)) {
        return 1;
    } else {
        return 0;
    }
}

requestListeners *serverRequestListenersCreate() {
    int i;
    requestListeners *s = requestListenersCreate(
            REQUEST_LEVEL_SVR,NULL,NULL,NULL);

    for (i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db + i;
        s->svr.dbs[i] = requestListenersCreate(
                REQUEST_LEVEL_DB,db,NULL,s);
    }
    return s;
}

void serverRequestListenersRelease(requestListeners *s) {
    int i;
    for (i = 0; i < s->svr.dbnum; i++) {
        requestListenersRelease(s->svr.dbs[i]);
    }
    requestListenersRelease(s);
    zfree(s);
}

static requestListeners *requestBindListeners(redisDb *db, robj *key,
        int create) {
    requestListeners *svr_listeners, *db_listeners, *key_listeners;

    svr_listeners = server.request_listeners;
    if (db == NULL || listLength(svr_listeners->listeners)) {
        return svr_listeners;
    }

    db_listeners = svr_listeners->svr.dbs[db->id];
    if (key == NULL || listLength(db_listeners->listeners)) {
        return db_listeners;
    }

    key_listeners = dictFetchValue(db_listeners->db.keys,key->ptr);
    if (key_listeners == NULL) {
        if (create) {
            key_listeners = requestListenersCreate(
                    REQUEST_LEVEL_KEY,db,key,db_listeners);
        }
    }

    return key_listeners;
}

static inline int proceed(requestListeners *listeners,
        requestListener *listener) {
    DEBUG_MSGS_APPEND(listener->msgs,"wait-proceed","listener=%s",
            requestListenerDump(listener));
    return listener->proceed(listeners,listener->db,
            listener->key,listener->c,listener->pd);
}

int requestWouldBlock(redisDb *db, robj *key) {
    requestListeners *listeners = requestBindListeners(db,key,0);
    if (listeners == NULL) return 0;
    return listeners->nlisteners > 0;
}

int requestWait(redisDb *db, robj *key, requestProceed cb, client *c,
        void *pd, freefunc pdfree, void *msgs) {
    int blocking;
    requestListeners *listeners;
    requestListener *listener;

    listeners = requestBindListeners(db,key,1);
    blocking = listeners->nlisteners > 0;
    listener = requestListenerCreate(db,key,cb,c,pd,pdfree,msgs);
    requestListenersPush(listeners,listener);

#ifdef SWAP_DEBUG
    sds dump = requestListenersDump(listeners);
    DEBUG_MSGS_APPEND(msgs,"wait-bind","listener = %s", dump);
    sdsfree(dump);
#endif

    /* Proceed right away if request key is not blocking, otherwise
     * execution is defered. */
    if (!blocking) proceed(listeners,listener);

    return 0;
}

int requestNotify(void *listeners_) {
    requestListeners *listeners = listeners_, *parent;
    requestListener *current, *next;

    current = requestListenersPop(listeners);

#ifdef SWAP_DEBUG
    sds dump = requestListenersDump(listeners);
    DEBUG_MSGS_APPEND(current->msgs,"wait-unbind","listener=%s", dump);
    sdsfree(dump);
#endif

    requestListenerRelease(current);

    /* Find next proceed-able listeners, then trigger proceed. */
    while (listeners) {
        /* First, try proceed current level listener. */
        if (listLength(listeners->listeners)) {
            next = requestListenersPeek(listeners);
            proceed(listeners,next);
            break;
        }

        /* If current level drained, try proceed parent listener. */ 
        if (listeners->nlisteners) {
            /* child listeners exists, wait untill all child finished. */
            break;
        } else {
            parent = listeners->parent;
            if (listeners->level == REQUEST_LEVEL_KEY) {
                /* Only key level listeners releases, DB or server level
                 * key released only when server exit. */
                requestListenersRelease(listeners);
            }

            if (parent == NULL) {
                listeners = NULL;
                break;
            }

            /* Parent is not proceed-able if sibling listeners exists. */
            if (parent->nlisteners > (int)listLength(parent->listeners)) {
                listeners = NULL;
                break;
            }

            listeners = parent;
        }
    }

    return 0;
}

#ifdef REDIS_TEST

static int blocked;

int proceedNotifyLater(void *listeners, redisDb *db, robj *key, client *c, void *pd_) {
    UNUSED(db), UNUSED(key), UNUSED(c);
    void **pd = pd_;
    *pd = listeners;
    blocked--;
    return 0;
}

int swapWaitTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;
    redisDb *db, *db2;
    robj *key1, *key2, *key3;
    void *handle1, *handle2, *handle3, *handledb, *handledb2, *handlesvr;

    TEST("wait: init") {
        int i;
        server.hz = 10;
        server.dbnum = 4;
        server.db = zmalloc(sizeof(redisDb)*server.dbnum);
        for (i = 0; i < server.dbnum; i++) server.db[i].id = i;
        db = server.db, db2 = server.db+1;
        server.request_listeners = serverRequestListenersCreate();
        key1 = createStringObject("key-1",5);
        key2 = createStringObject("key-2",5);
        key3 = createStringObject("key-3",5);

        test_assert(server.request_listeners);
        test_assert(!blocked);
    }

   TEST("wait: parallel key") {
       handle1 = NULL, handle2 = NULL, handle3 = NULL, handlesvr = NULL;
       requestWait(db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL), blocked++;
       requestWait(db,key2,proceedNotifyLater,NULL,&handle2,NULL,NULL), blocked++;
       requestWait(db,key3,proceedNotifyLater,NULL,&handle3,NULL,NULL), blocked++;
       test_assert(!blocked);
       test_assert(requestWouldBlock(db,key1));
       test_assert(requestWouldBlock(db,key2));
       test_assert(requestWouldBlock(db,key3));
       test_assert(requestWouldBlock(db,NULL));
       requestNotify(handle1);
       test_assert(!requestWouldBlock(db,key1));
       requestNotify(handle2);
       test_assert(!requestWouldBlock(db,key2));
       requestNotify(handle3);
       test_assert(!requestWouldBlock(db,key3));
       test_assert(!requestWouldBlock(NULL,NULL));
   } 

   TEST("wait: pipelined key") {
       int i;
       for (i = 0; i < 3; i++) {
           blocked++;
           requestWait(db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL);
       }
       test_assert(requestWouldBlock(db,key1));
       /* first one proceeded, others blocked */
       test_assert(blocked == 2);
       for (i = 0; i < 2; i++) {
           requestNotify(handle1);
           test_assert(requestWouldBlock(db,key1));
       }
       test_assert(blocked == 0);
       requestNotify(handle1);
       test_assert(!requestWouldBlock(db,key1));
   }

   TEST("wait: parallel db") {
       requestWait(db,NULL,proceedNotifyLater,NULL,&handledb,NULL,NULL), blocked++;
       requestWait(db2,NULL,proceedNotifyLater,NULL,&handledb2,NULL,NULL), blocked++;
       test_assert(!blocked);
       test_assert(requestWouldBlock(db,NULL));
       test_assert(requestWouldBlock(db2,NULL));
       requestNotify(handledb);
       requestNotify(handledb2);
       test_assert(!requestWouldBlock(db,NULL));
       test_assert(!requestWouldBlock(db2,NULL));
   }

    TEST("wait: mixed parallel-key/db/parallel-key") {
        handle1 = NULL, handle2 = NULL, handle3 = NULL, handledb = NULL;
        requestWait(db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL),blocked++;
        requestWait(db,key2,proceedNotifyLater,NULL,&handle2,NULL,NULL),blocked++;
        requestWait(db,NULL,proceedNotifyLater,NULL,&handledb,NULL,NULL),blocked++;
        requestWait(db,key3,proceedNotifyLater,NULL,&handle3,NULL,NULL),blocked++;
        /* key1/key2 proceeded, db/key3 blocked */
        test_assert(requestWouldBlock(db,NULL));
        test_assert(blocked == 2);
        /* key1/key2 notify */
        requestNotify(handle1);
        test_assert(requestWouldBlock(db,NULL));
        requestNotify(handle2);
        test_assert(requestWouldBlock(db,NULL));
        /* db proceeded, key3 still blocked. */
        test_assert(blocked == 1);
        test_assert(handle3 == NULL);
        /* db notified, key3 proceeds but still blocked */
        requestNotify(handledb);
        test_assert(!blocked);
        test_assert(requestWouldBlock(db,NULL));
        /* db3 proceed, noting would block */
        requestNotify(handle3);
        test_assert(!requestWouldBlock(db,NULL));
    }

    TEST("wait: mixed parallel-key/server/parallel-key") {
        handle1 = NULL, handle2 = NULL, handle3 = NULL, handlesvr = NULL;
        requestWait(db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL),blocked++;
        requestWait(db,key2,proceedNotifyLater,NULL,&handle2,NULL,NULL),blocked++;
        requestWait(NULL,NULL,proceedNotifyLater,NULL,&handlesvr,NULL,NULL),blocked++;
        requestWait(db,key3,proceedNotifyLater,NULL,&handle3,NULL,NULL),blocked++;
        /* key1/key2 proceeded, svr/key3 blocked */
        test_assert(requestWouldBlock(NULL,NULL));
        test_assert(requestWouldBlock(db,NULL));
        test_assert(blocked == 2);
        /* key1/key2 notify */
        requestNotify(handle1);
        test_assert(requestWouldBlock(NULL,NULL));
        requestNotify(handle2);
        test_assert(requestWouldBlock(NULL,NULL));
        /* svr proceeded, key3 still blocked. */
        test_assert(blocked == 1);
        test_assert(handle3 == NULL);
        /* svr notified, db3 proceeds but still would block */
        requestNotify(handlesvr);
        test_assert(!blocked);
        test_assert(requestWouldBlock(NULL,NULL));
        /* db3 proceed, noting would block */
        requestNotify(handle3);
        test_assert(!requestWouldBlock(NULL,NULL));
    }

    return 0;
}

#endif