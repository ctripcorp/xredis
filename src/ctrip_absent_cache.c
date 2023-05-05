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

void listUnlink(list *list, listNode *node);
void listLinkHead(list *list, listNode *node);

absentKeyMapEntry *absentKeyMapEntryNew(listNode *ln, dict *subkeys) {
    absentKeyMapEntry *entry = zmalloc(sizeof(absentKeyMapEntry));
    entry->ln = ln;
    entry->subkeys = subkeys;
    return entry;
}

void absentKeyMapEntryFree(absentKeyMapEntry *entry) {
    if (entry == NULL) return;
    if (entry->subkeys) {
        dictRelease(entry->subkeys);
        entry->subkeys = NULL;
    }
    zfree(entry);
}

absentListEntry *absentListEntryNew(sds key, sds subkey) {
    absentListEntry *entry = zmalloc(sizeof(absentListEntry));
    entry->key = key;
    entry->subkey = subkey;
    return entry;
}

void absentListEntryFree(absentKeyMapEntry *entry) {
    if (entry == NULL) return;
    zfree(entry);
}

void dictAbsentKeyMapEntryFree(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    absentKeyMapEntryFree(val);
}

/* holds most recently accessed keys & subkeys that definitely not exists
 * in rocksdb. */
dictType absentKeyDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictAbsentKeyMapEntryFree, /* val destructor */
    NULL                       /* allow to expand */
};

dictType absentSubkeyDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

absentCache *absentCacheNew(size_t capacity) {
    absentCache *absent = zmalloc(sizeof(absentCache));
    absent->disable_subkey = 0;
    absent->reserved = 0;
    absent->capacity = capacity;
    absent->map = dictCreate(&absentKeyDictType,NULL);
    absent->list = listCreate();
    listSetFreeMethod(absent->list,(void(*)(void*))absentListEntryFree);
    return absent;
}

void absentCacheFree(absentCache *absent) {
    if (absent == NULL) return;
    if (absent->list) {
        listRelease(absent->list);
        absent->list = NULL;
    }
    if (absent->map) {
        dictRelease(absent->map);
        absent->map = NULL;
    }
    zfree(absent);
}

/* Delete both cached absent key & subkeys */
int absentCacheDelete(absentCache *absent, sds key) {
    dictEntry *de;

    if ((de = dictUnlink(absent->map,key))) {
        absentKeyMapEntry *me = dictGetVal(de);
        /* delete cached absent subkeys */
        if (me->subkeys) {
            dictEntry *sde;
            dictIterator *di = dictGetIterator(me->subkeys);
            while ((sde = dictNext(di)) != NULL) {
                listNode *ln = dictGetVal(sde);
                listDelNode(absent->list,ln);
            }
            dictReleaseIterator(di);
            dictRelease(me->subkeys);
            me->subkeys = NULL;
        }
        /* delete cached absent key */
        if (me->ln) {
            listDelNode(absent->list,me->ln);
            me->ln = NULL;
        }
        dictFreeUnlinkedEntry(absent->map,de);
        return 1;
    } else {
        return 0;
    }
}

static void absentCacheTrim(absentCache *absent) {
    while (listLength(absent->list) > absent->capacity) {
        listNode *ln = listLast(absent->list);
        absentListEntry *le = listNodeValue(ln);
        absentKeyMapEntry *me = dictFetchValue(absent->map,le->key);
        if (le->subkey == NULL) {
            serverAssert(me->ln == ln);
            if (me->subkeys == NULL) {
                dictDelete(absent->map,le->key);
            } else {
                me->ln = NULL;
            }
        } else {
            serverAssert(dictDelete(me->subkeys,le->subkey) == DICT_OK);
            if (dictSize(me->subkeys) == 0) {
                dictRelease(me->subkeys);
                me->subkeys = NULL;

                if (me->ln == NULL) {
                    dictDelete(absent->map,le->key);
                }
            }
        }
        listDelNode(absent->list,ln);
    }
}

int absentCachePutKey(absentCache *absent, sds key_) {
    dictEntry *de;
    absentKeyMapEntry *me;
    absentListEntry *le;
    sds key;

    serverAssert(key_);
    if (!(de = dictFind(absent->map,key_))) {
        key = sdsdup(key_);
        me = absentKeyMapEntryNew(NULL,NULL);
        dictAdd(absent->map,key,me);
    } else {
        key = dictGetKey(de);
        me = dictGetVal(de);
        if (me->ln != NULL) {
            listUnlink(absent->list,me->ln);
            listLinkHead(absent->list,me->ln);
            return 0;
        }
    }

    le = absentListEntryNew(key,NULL);
    listAddNodeHead(absent->list,le);
    me->ln = listFirst(absent->list);
    absentCacheTrim(absent);
    return 1;
}

int absentCachePutSubkey(absentCache *absent, sds key_, sds subkey_) {
    dictEntry *de, *sde;
    absentKeyMapEntry *me;
    absentListEntry *le;
    dict *subkeys;
    sds key, subkey;

    serverAssert(key_ && subkey_);

    if (absent->disable_subkey) return 0;

    if (!(de = dictFind(absent->map,key_))) {
        key = sdsdup(key_);
        subkey = sdsdup(subkey_);
        subkeys = dictCreate(&absentSubkeyDictType,NULL);
        me = absentKeyMapEntryNew(NULL,subkeys);
        dictAdd(absent->map,key,me);
    } else {
        me = dictGetVal(de);
        key = dictGetKey(de);
        if (me->subkeys == NULL)
            me->subkeys = dictCreate(&absentSubkeyDictType,NULL);
        subkeys = me->subkeys;
        if (!(sde = dictFind(subkeys,subkey_))) {
            subkey = sdsdup(subkey_);
        } else {
            listNode *ln = dictGetVal(sde);
            listUnlink(absent->list,ln);
            listLinkHead(absent->list,ln);
            return 0;
        }
    }

    le = absentListEntryNew(key,subkey);
    listAddNodeHead(absent->list,le);
    dictAdd(subkeys,subkey,listFirst(absent->list));
    absentCacheTrim(absent);

    return 1;
}

int absentCacheGetKey(absentCache *absent, sds key) {
    absentKeyMapEntry *me = dictFetchValue(absent->map,key);
    if (me && me->ln != NULL) {
        listUnlink(absent->list,me->ln);
        listLinkHead(absent->list,me->ln);
        return 1;
    } else {
        return 0;
    }
}

int absentCacheGetSubkey(absentCache *absent, sds key, sds subkey) {
    listNode *ln;
    absentKeyMapEntry *me;
    if (absent->disable_subkey) return 0;
    me = dictFetchValue(absent->map,key);
    if (me == NULL || me->subkeys == NULL) return 0;
    if ((ln = dictFetchValue(me->subkeys,subkey)) == NULL) return 0;
    listUnlink(absent->list,ln);
    listLinkHead(absent->list,ln);
    return 1;
}

void absentCacheSetCapacity(absentCache *absent, size_t capacity) {
    absent->capacity = capacity;
    absentCacheTrim(absent);
}

void absentCacheDisableSubkey(absentCache *absent) {
    absent->disable_subkey = 1;
}

void absentCacheEnableSubkey(absentCache *absent) {
    absent->disable_subkey = 0;
}

#ifdef REDIS_TEST

static int absentCacheExistsKey(absentCache *absent, sds key) {
    return dictFind(absent->map,key) != NULL;
}

static int absentCacheExistsSubkey(absentCache *absent, sds key, sds subkey) {
    absentKeyMapEntry *me = dictFetchValue(absent->map,key);
    return me && me->subkeys && dictFetchValue(me->subkeys, subkey);
}

int swapAbsentTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

    TEST("absent: key") {
        sds first = sdsnew("1"), second = sdsnew("2"),
            third = sdsnew("3"), fourth = sdsnew("4");
        absentCache *absent;

        absent = absentCacheNew(1);
        test_assert(!absentCacheExistsKey(absent,first));
        absentCachePutKey(absent,first);
        test_assert(absentCacheExistsKey(absent,first));
        absentCachePutKey(absent,second);
        test_assert(!absentCacheExistsKey(absent,first));
        absentCacheFree(absent);

        absent = absentCacheNew(3);
        absentCachePutKey(absent,first);
        absentCachePutKey(absent,second);
        absentCachePutKey(absent,third);
        absentCachePutKey(absent,fourth);
        test_assert(!absentCacheExistsKey(absent,first));
        test_assert(absentCacheExistsKey(absent,second));
        test_assert(absentCacheExistsKey(absent,third));
        test_assert(absentCacheExistsKey(absent,fourth));
        absentCachePutKey(absent,first);
        test_assert(absentCacheExistsKey(absent,first));
        test_assert(!absentCacheExistsKey(absent,second));
        test_assert(absentCacheExistsKey(absent,third));
        test_assert(absentCacheExistsKey(absent,fourth));

        absentCacheDelete(absent,second);
        test_assert(!absentCacheExistsKey(absent,second));

        test_assert(absentCacheGetKey(absent,second) == 0);
        test_assert(absentCacheGetKey(absent,third) == 1);
        test_assert(absentCacheGetKey(absent,first) == 1);

        sdsfree(first), sdsfree(second), sdsfree(third), sdsfree(fourth);
        sds first2 = sdsnew("1"), fourth2 = sdsnew("4");

        absentCacheSetCapacity(absent, 1);
        test_assert(absent->capacity == 1);
        test_assert(absentCacheExistsKey(absent,first2));
        test_assert(!absentCacheExistsKey(absent,fourth2));
        absentCacheFree(absent);
        sdsfree(first2), sdsfree(fourth2);
    }

    TEST("absent: subkey") {
        sds key1 = sdsnew("key1"), key2 = sdsnew("key2");
        sds first = sdsnew("1"), second = sdsnew("2");
        absentCache *absent;

        absent = absentCacheNew(1);
        test_assert(!absentCacheExistsSubkey(absent,key1,first));
        absentCachePutSubkey(absent,key1,first);
        test_assert(absentCacheExistsSubkey(absent,key1,first));
        absentCachePutSubkey(absent,key2,first);
        test_assert(!absentCacheExistsSubkey(absent,key1,first));
        test_assert(absentCacheExistsSubkey(absent,key2,first));
        absentCachePutSubkey(absent,key2,first);
        test_assert(absentCacheExistsSubkey(absent,key2,first));
        absentCacheFree(absent);

        absent = absentCacheNew(3);
        absentCachePutSubkey(absent,key1,first);
        absentCachePutSubkey(absent,key1,second);
        absentCachePutSubkey(absent,key2,first);
        absentCachePutSubkey(absent,key2,second);
        test_assert(!absentCacheExistsSubkey(absent,key1,first));
        test_assert(absentCacheExistsSubkey(absent,key1,second));
        test_assert(absentCacheExistsSubkey(absent,key2,first));
        test_assert(absentCacheExistsSubkey(absent,key2,second));

        absentCacheDelete(absent,key2);
        test_assert(!absentCacheExistsSubkey(absent,key2,first));
        test_assert(!absentCacheExistsSubkey(absent,key2,second));

        absentCachePutSubkey(absent,key2,first);
        absentCachePutSubkey(absent,key2,second);
        test_assert(absentCacheGetSubkey(absent,key1,second));
        test_assert(!absentCacheGetSubkey(absent,key1,first));

        absentCachePutSubkey(absent,key1,first);
        test_assert(!absentCacheGetSubkey(absent,key2,first));
        absentCacheFree(absent);

        sdsfree(first), sdsfree(second);
        sdsfree(key1), sdsfree(key2);
    }

    TEST("absent: mixed key & subkey") {
        sds key1 = sdsnew("key1"), key2 = sdsnew("key2");
        sds first = sdsnew("1"), second = sdsnew("2");
        absentCache *absent;

        absent = absentCacheNew(4);
        test_assert(absentCachePutKey(absent,key1));
        test_assert(absentCachePutSubkey(absent,key1,first));
        test_assert(absentCachePutSubkey(absent,key2,first));
        test_assert(absentCachePutKey(absent,key2));

        test_assert(absentCacheGetKey(absent,key1));
        test_assert(absentCacheGetSubkey(absent,key1,first));
        test_assert(absentCacheGetSubkey(absent,key2,first));
        test_assert(absentCacheGetKey(absent,key2));

        test_assert(absentCacheGetKey(absent,key1));
        test_assert(absentCacheGetSubkey(absent,key1,first));
        test_assert(absentCacheGetSubkey(absent,key2,first));
        test_assert(absentCacheGetKey(absent,key2));

        test_assert(absentCachePutSubkey(absent,key1,second));
        test_assert(!absentCacheGetKey(absent,key1)); /* key1 trimmed */

        test_assert(absentCachePutSubkey(absent,key2,second));
        test_assert(!absentCacheGetSubkey(absent,key1,first)); /* key1.first trimmed */

        test_assert(absentCacheDelete(absent,key2));
        test_assert(!absentCacheGetKey(absent,key2));
        test_assert(!absentCacheGetSubkey(absent,key2,first));
        test_assert(!absentCacheGetSubkey(absent,key2,second));

        test_assert(absentCachePutKey(absent,key2));
        test_assert(absentCachePutSubkey(absent,key2,first));
        test_assert(absentCachePutSubkey(absent,key2,second));
        test_assert(absentCacheGetSubkey(absent,key1,second));

        absentCacheSetCapacity(absent,2);

        test_assert(absentCacheGetSubkey(absent,key2,second));
        test_assert(absentCacheGetSubkey(absent,key1,second));
        test_assert(!absentCacheGetKey(absent,key1));
        test_assert(!absentCacheGetKey(absent,key2));

        absentCacheFree(absent);

        sdsfree(first), sdsfree(second);
        sdsfree(key1), sdsfree(key2);
    }

    return error;
}

#endif
