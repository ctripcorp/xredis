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

#ifndef __CTRIP_LRU_CACHE__
#define __CTRIP_LRU_CACHE__

#include "dict.h"
#include "adlist.h"
#include "sds.h"

#define LRU_CACHE_TYPE_KEY 0
#define LRU_CACHE_TYPE_KVP 1

/* key cache value is ln, while kvp cache contains both ln and val */
typedef struct kvpCacheEntry {
    listNode *ln;
    void *val;
} kvpCacheEntry;

typedef struct lruCache {
  size_t capacity;
  dict *map;
  list *list;
  int mode;
} lruCache;

lruCache *lruCacheNew(int type, size_t capacity);
static inline lruCache *lruKeyCacheNew(size_t capacity) {
  return lruCacheNew(LRU_CACHE_TYPE_KEY,capacity);
}
void lruCacheFree(lruCache *cache);
int lruCachePut(lruCache *cache, sds key, void *val, void **oval);
static inline int lruKeyCachePut(lruCache *cache, sds key) {
  return lruCachePut(cache,key,NULL,NULL);
}
int lruCacheGet(lruCache *cache, sds key, void **pval);
static inline int lruKeyCacheGet(lruCache *cache, sds key) {
  return lruCacheGet(cache,key,NULL);
}
/* get without changing lru order. */
int lruCacheLookup(lruCache *cache, sds key, void **pval);
int lruCacheDelete(lruCache *cache, sds key);
void lruCacheSetCapacity(lruCache *cache, size_t capacity);
size_t lruCacheCount(lruCache *cache);

#define LRU_CAHCHE_ITER_FROM_OLDEST LIST_TAIL
#define LRU_CAHCHE_ITER_FROM_NEWEST LIST_HEAD

typedef struct lruCacheIter {
  lruCache *cache;
  listIter *li;
  listNode *ln;
} lruCacheIter;

lruCacheIter *lruCacheGetIterator(lruCache *cache, int direction);
void lruCacheReleaseIterator(lruCacheIter *iter);
int lruCacheIterNext(lruCacheIter *iter);
sds lruCacheIterKey(lruCacheIter *iter);
void *lruCacheIterVal(lruCacheIter *iter);

#endif
