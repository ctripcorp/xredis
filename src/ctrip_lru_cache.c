/* Copyright (c) 2023, ctrip.com * All rights reserved.
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
#include "ctrip_lru_cache.h"

#define LRU_CACHE_LOOKUP_NOTOUCH 1

kvpCacheEntry *kvpCacheEntryNew(listNode *ln, void *val) {
    kvpCacheEntry *e = zmalloc(sizeof(kvpCacheEntry));
    e->ln = ln;
    e->val = val;
    return e;
}

void kvpCacheEntryFree(void *privdata, void *val) {
    UNUSED(privdata);
    if (val != NULL) zfree(val);
}

/* extent list api so that list node re-allocate can be avoided. */
void listUnlink(list *list, listNode *node) {
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;
    list->len--;
}

void listLinkHead(list *list, listNode *node) {
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
}

/* holds most recently accessed keys that definitely not exists in rocksdb. */
dictType keyCacheDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

dictType kvpCacheDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    kvpCacheEntryFree,         /* val destructor */
    NULL                       /* allow to expand */
};

lruCache *lruCacheNew(int mode, size_t capacity) {
    lruCache *cache = zcalloc(sizeof(lruCache));
    cache->capacity = capacity;
    cache->list = listCreate();
    cache->mode = mode;
    if (mode == LRU_CACHE_TYPE_KEY) {
        cache->map = dictCreate(&keyCacheDictType,NULL);
    } else {
        cache->map = dictCreate(&kvpCacheDictType,NULL);
    }
    return cache;
}

void lruCacheFree(lruCache *cache) {
   if (cache == NULL) return;
   dictRelease(cache->map);
   cache->map = NULL;
   listRelease(cache->list);
   cache->list = NULL;
   zfree(cache);
}

static void lruCacheTrim(lruCache *cache) {
    while (listLength(cache->list) > cache->capacity) {
        listNode *ln = listLast(cache->list);
        serverAssert(dictDelete(cache->map,listNodeValue(ln)) == DICT_OK);
        listDelNode(cache->list, ln);
    }
}

int lruCachePut(lruCache *cache, sds key, void *val, void **oval) {
    dictEntry *de;
    listNode *ln;

    if ((de = dictFind(cache->map,key))) {
        if (cache->mode == LRU_CACHE_TYPE_KEY) {
            ln = dictGetVal(de);
        } else {
            kvpCacheEntry *entry = dictGetVal(de);
            ln = entry->ln;
            if (oval) *oval = entry->val;
            entry->val = val;
        }
        listUnlink(cache->list,ln);
        listLinkHead(cache->list,ln);
        return 0;
    } else {
        sds dup = sdsdup(key);
        listAddNodeHead(cache->list,dup);
        ln = listFirst(cache->list);
        if (cache->mode == LRU_CACHE_TYPE_KEY) {
            dictAdd(cache->map,dup,ln);
        } else {
            kvpCacheEntry *entry = kvpCacheEntryNew(ln,val);
            dictAdd(cache->map,dup,entry);
            if (oval) *oval = NULL;
        }
        lruCacheTrim(cache);
        return 1;
    }
}

int lruCacheDelete(lruCache *cache, sds key) {
    dictEntry *de;
    listNode *ln;

    if ((de = dictUnlink(cache->map,key))) {
        if (cache->mode == LRU_CACHE_TYPE_KEY) {
            ln = dictGetVal(de);
        } else {
            kvpCacheEntry *entry = dictGetVal(de);
            ln = entry->ln;
        }
        listDelNode(cache->list,ln);
        dictFreeUnlinkedEntry(cache->map,de);
        return 1;
    } else {
        return 0;
    }
}

static int lruCacheLookup_(lruCache *cache, int flags, sds key, void **pval) {
    dictEntry *de;
    listNode *ln;

    if ((de = dictFind(cache->map,key))) {
        if (cache->mode == LRU_CACHE_TYPE_KEY) {
            ln = dictGetVal(de);
        } else {
            kvpCacheEntry *entry = dictGetVal(de);
            ln = entry->ln;
            if (pval) *pval = entry->val;
        }
        if (!(flags & LRU_CACHE_LOOKUP_NOTOUCH)) {
            listUnlink(cache->list,ln);
            listLinkHead(cache->list,ln);
        }
        return 1;
    } else {
        return 0;
    }
}

int lruCacheGet(lruCache *cache, sds key, void **pval) {
    return lruCacheLookup_(cache,0,key,pval);
}

int lruCacheLookup(lruCache *cache, sds key, void **pval) {
    return lruCacheLookup_(cache,LRU_CACHE_LOOKUP_NOTOUCH,key,pval);
}

void lruCacheSetCapacity(lruCache *cache, size_t capacity) {
    cache->capacity = capacity;
    lruCacheTrim(cache);
}

size_t lruCacheCount(lruCache *cache) {
    return listLength(cache->list);
}

lruCacheIter *lruCacheGetIterator(lruCache *cache, int direction) {
    lruCacheIter *iter = zmalloc(sizeof(lruCacheIter));
    iter->li = listGetIterator(cache->list,direction);
    iter->cache = cache;
    return iter;
}

int lruCacheIterNext(lruCacheIter *iter) {
    iter->ln = listNext(iter->li);
    return iter->ln != NULL;
}

sds lruCacheIterKey(lruCacheIter *iter) {
    return listNodeValue(iter->ln);
}

void *lruCacheIterVal(lruCacheIter *iter) {
    sds key = lruCacheIterKey(iter);
    kvpCacheEntry *e = dictFetchValue(iter->cache->map,key);
    return e->val;
}

#ifdef REDIS_TEST

static int lruCacheExists(lruCache *cache, sds key) {
    return dictFind(cache->map,key) != NULL;
}

int lruCacheTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

    TEST("list link & unlink") {
        listNode *ln;
        list *l = listCreate();

        listAddNodeHead(l, (void*)1);
        ln = listFirst(l);
        listUnlink(l,ln);
        test_assert(listLength(l) == 0 && listFirst(l) == NULL && listLast(l) == NULL);
        listLinkHead(l,ln);
        test_assert(listLength(l) == 1 && listFirst(l) == ln && listLast(l) == ln);

        listAddNodeTail(l, (void*)2);
        listAddNodeTail(l, (void*)3);
        ln = listSearchKey(l, (void*)2);
        listUnlink(l,ln);
        test_assert(listLength(l) == 2);
        listLinkHead(l,ln);
        test_assert(listLength(l) == 3);
        ln = listFirst(l);
        test_assert(listNodeValue(ln) == (void*)2);
        ln = ln->next;
        test_assert(listNodeValue(ln) == (void*)1);
        ln = ln->next;
        test_assert(listNodeValue(ln) == (void*)3);
        test_assert(listLast(l) == ln);

        listRelease(l);
    }

    TEST("lru key cache") {
        sds first = sdsnew("1"), second = sdsnew("2"), third = sdsnew("3"), fourth = sdsnew("4");
        lruCache *cache;

        cache = lruKeyCacheNew(1);
        test_assert(!lruCacheExists(cache,first));
        lruKeyCachePut(cache,first);
        test_assert(lruCacheExists(cache,first));
        lruKeyCachePut(cache,second);
        test_assert(!lruCacheExists(cache,first));
        lruCacheFree(cache);

        cache = lruKeyCacheNew(3);
        lruKeyCachePut(cache,first);
        lruKeyCachePut(cache,second);
        lruKeyCachePut(cache,third);
        lruKeyCachePut(cache,fourth);
        test_assert(!lruCacheExists(cache,first));
        test_assert(lruCacheExists(cache,second));
        test_assert(lruCacheExists(cache,third));
        test_assert(lruCacheExists(cache,fourth));
        lruKeyCachePut(cache,first);
        test_assert(lruCacheExists(cache,first));
        test_assert(!lruCacheExists(cache,second));
        test_assert(lruCacheExists(cache,third));
        test_assert(lruCacheExists(cache,fourth));

        lruCacheDelete(cache,second);
        test_assert(!lruCacheExists(cache,second));

        test_assert(lruKeyCacheGet(cache,second) == 0);
        test_assert(lruKeyCacheGet(cache,third) == 1);
        test_assert(lruKeyCacheGet(cache,first) == 1);

        sdsfree(first), sdsfree(second), sdsfree(third), sdsfree(fourth);
        sds first2 = sdsnew("1"), fourth2 = sdsnew("4");

        lruCacheSetCapacity(cache, 1);
        test_assert(cache->capacity == 1);
        test_assert(lruCacheExists(cache,first2));
        test_assert(!lruCacheExists(cache,fourth2));
        lruCacheFree(cache);
        sdsfree(first2), sdsfree(fourth2);
    }

    TEST("lru kvp cache") {
        sds first = sdsnew("1"), second = sdsnew("2"), third = sdsnew("3"), fourth = sdsnew("4");
        lruCache *cache;
        lruCacheIter *iter;
        int oval;

        cache = lruCacheNew(LRU_CACHE_TYPE_KVP,1);
        test_assert(!lruCacheExists(cache,first));
        lruCachePut(cache,first,(void*)1,NULL);
        test_assert(lruCacheExists(cache,first));
        lruCachePut(cache,second,(void*)2,NULL);
        test_assert(!lruCacheExists(cache,first));
        lruCacheFree(cache);

        cache = lruCacheNew(LRU_CACHE_TYPE_KVP,3);
        lruCachePut(cache,first,(void*)1,NULL);
        lruCachePut(cache,second,(void*)2,NULL);
        lruCachePut(cache,third,(void*)3,NULL);
        lruCachePut(cache,fourth,(void*)4,NULL);
        test_assert(!lruCacheExists(cache,first));
        test_assert(lruCacheExists(cache,second));
        test_assert(lruCacheExists(cache,third));
        test_assert(lruCacheExists(cache,fourth));
        lruCachePut(cache,first,(void*)1,NULL);
        test_assert(lruCacheExists(cache,first));
        test_assert(!lruCacheExists(cache,second));
        test_assert(lruCacheExists(cache,third));
        test_assert(lruCacheExists(cache,fourth));
        test_assert(lruCacheGet(cache,fourth,(void**)&oval) && oval == 4);

        lruCacheDelete(cache,second);
        test_assert(!lruCacheExists(cache,second));

        /* 3-1-4 */
        test_assert(lruCacheLookup(cache,fourth,(void**)&oval) && oval == 4);
        test_assert(lruCacheLookup(cache,first,(void**)&oval) && oval == 1);
        test_assert(lruCacheLookup(cache,third,(void**)&oval) && oval == 3);

        iter = lruCacheGetIterator(cache,LRU_CAHCHE_ITER_FROM_OLDEST);
        test_assert(lruCacheIterNext(iter));
        test_assert(strcmp(lruCacheIterKey(iter),"3") == 0);
        test_assert(lruCacheIterVal(iter) == (void*)3);
        test_assert(lruCacheIterNext(iter));
        test_assert(strcmp(lruCacheIterKey(iter),"1") == 0);
        test_assert(lruCacheIterVal(iter) == (void*)1);
        test_assert(lruCacheIterNext(iter));
        test_assert(strcmp(lruCacheIterKey(iter),"4") == 0);
        test_assert(lruCacheIterVal(iter) == (void*)4);
        test_assert(!lruCacheIterNext(iter));

        test_assert(lruCacheGet(cache,second,(void**)&oval) == 0);
        test_assert(lruCacheGet(cache,third,(void**)&oval) == 1 && oval == 3);
        test_assert(lruCacheGet(cache,first,(void**)&oval) == 1 && oval == 1);

        sdsfree(first), sdsfree(second), sdsfree(third), sdsfree(fourth);
        sds first2 = sdsnew("1"), fourth2 = sdsnew("4");

        lruCacheSetCapacity(cache, 1);
        test_assert(cache->capacity == 1);
        test_assert(lruCacheExists(cache,first2));
        test_assert(!lruCacheExists(cache,fourth2));
        lruCacheFree(cache);
        sdsfree(first2), sdsfree(fourth2);
    }

    return error;
}


#endif
