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

/* Note that db.dirty_subkeys is a satellite dict just like db.expire. */
/* Db->dirty_subkeys */
int dictExpandAllowed(size_t moreMem, double usedRatio);

dictType dbDirtySubkeysDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    dictObjectDestructor,       /* val destructor */
    dictExpandAllowed,          /* allow to expand */
};

void dbAddDirtySubkeys(redisDb *db, robj* key, robj *dss) {
    dictEntry *kde;
    kde = dictFind(db->dict,key->ptr);
    serverAssertWithInfo(NULL,key,kde != NULL);
    serverAssert(dictAdd(db->dirty_subkeys,dictGetKey(kde),dss) == DICT_OK);
}

int dbDeleteDirtySubkeys(redisDb *db, robj* key) {
    if (dictSize(db->dirty_subkeys) == 0) return 0;
    return dictDelete(db->dirty_subkeys,key->ptr) == DICT_OK ? 1 : 0;
}

robj *lookupDirtySubkeys(redisDb *db, robj* key) {
    return dictFetchValue(db->dirty_subkeys,key->ptr);
}

/*
 * dirty_data means all subkeys might be dirty, it will be not cleared (untill
 * object totally swapped out and then swapped in again).
 * dirty_subkeys works only when dirty_data not set, it keeps track of dirty
 * subkeys that differs from rocksdb. if both dirty_data and dirty_subkeys are
 * set, then all subkeys may be dirty, effect is same as dirty_data.
 * dirty_meta means object meta(expire,type,length) in memory differs from
 * rocksdb, cleared whenever swapout finished (swapout persists meta).
 */

inline robj *dirtySubkeysNew() {
    return createHashObject();
}

inline void dirtySubkeysFree(robj *dss) {
    if (dss == NULL) return;
    decrRefCount(dss);
}

inline unsigned long dirtySubkeysLength(robj *dss) {
    return hashTypeLength(dss);
}

inline int dirtySubkeysAdd(robj *dss, sds subkey, size_t sublen) {
    return hashTypeSet(dss,subkey,sdsfromlonglong(sublen),HASH_SET_TAKE_VALUE);
}

inline int dirtySubkeysRemove(robj *dss, sds subkey) {
    if (dirtySubkeysLength(dss) > 0)
        return hashTypeDelete(dss,subkey);
    else
        return 0;
}

void dirtySubkeysIteratorInit(dirtySubkeysIterator *it, robj *dss) {
    it->iter = hashTypeInitIterator(dss);
}

void dirtySubkeysIteratorDeinit(dirtySubkeysIterator *it) {
    if (it->iter) {
        hashTypeReleaseIterator(it->iter);
        it->iter = NULL;
    }
}

robj *dirtySubkeysIteratorNext(dirtySubkeysIterator *it, size_t *len) {
    robj *subkey;
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (hashTypeNext(it->iter) == C_ERR) {
        *len = 0;
        return NULL;
    }

    hashTypeCurrentObject(it->iter,OBJ_HASH_KEY,&vstr,&vlen,&vll);
    if (vstr) {
        subkey = createStringObject((const char*)vstr,vlen);
    } else {
        subkey = createStringObjectFromLongLong(vll);
        subkey = unshareStringValue(subkey);
    }

    hashTypeCurrentObject(it->iter,OBJ_HASH_VALUE,&vstr,&vlen,&vll);
    if (vstr != NULL) {
        long long value;
        if (string2ll((char*)vstr,vlen,&value) && value > 0)
            *len = (size_t)value;
        else
            *len = 0;
    } else {
        *len = vll;
    }
    return subkey;
}

void notifyKeyspaceEventDirtyKey(int type, char *event, robj *key, int dbid) {
    robj* o = lookupKey(&server.db[dbid], key, LOOKUP_NOTOUCH);
    notifyKeyspaceEventDirty(type,event,key,dbid,o,NULL);
}

void notifyKeyspaceEventDirty(int type, char *event, robj *key, int dbid, ...) {
    robj *o;
    va_list ap;

    va_start(ap, dbid);
    while ((o = va_arg(ap, robj*))) setObjectDirtyPersist(dbid,key,o);
    va_end(ap);

    notifyKeyspaceEvent(type,event,key,dbid);
}

void notifyKeyspaceEventDirtySubkeys(int type, char *event, robj *key,
        int dbid, robj *o, int count, sds *subkeys, size_t *sublens) {
    if (server.swap_dirty_subkeys_enabled) {
        if (!objectIsDataDirty(o)) {
            redisDb *db = server.db+dbid;
            robj *dss = lookupDirtySubkeys(db,key);

            if (dss == NULL) {
                dss = dirtySubkeysNew();
                dbAddDirtySubkeys(db,key,dss);
            }

            for (int i = 0; i < count; i++) {
                dirtySubkeysAdd(dss,subkeys[i], sublens[i]);
            }
        }

        setObjectMetaDirtyPersist(dbid,key,o);
    } else {
        setObjectDirtyPersist(dbid,key,o);
    }

    notifyKeyspaceEvent(type,event,key,dbid);
}

void notifyKeyspaceEventDirtyMeta(int type, char *event, robj *key,
        int dbid, robj *o) {
    setObjectMetaDirtyPersist(dbid,key,o);
    notifyKeyspaceEvent(type,event,key,dbid);
}

