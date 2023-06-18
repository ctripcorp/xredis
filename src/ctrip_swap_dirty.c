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

inline int dirtySubkeysAdd(robj *dss, sds subkey) {
    return hashTypeSet(dss,subkey,NULL,0);
}

inline int dirtySubkeysDelete(robj *dss, sds subkey) {
    return hashTypeDelete(dss,subkey);
}

void notifyKeyspaceEventDirtyKey(int type, char *event, robj *key, int dbid) {
    robj* o = lookupKey(&server.db[dbid], key, LOOKUP_NOTOUCH);
    notifyKeyspaceEventDirty(type,event,key,dbid,o,NULL);
}

void notifyKeyspaceEventDirty(int type, char *event, robj *key, int dbid, ...) {
    robj *o;
    va_list ap;

    va_start(ap, dbid);
    while ((o = va_arg(ap, robj*))) setObjectDirty(o);
    va_end(ap);

    notifyKeyspaceEvent(type,event,key,dbid);
}


/* dirty_data: all subkeys may be dirty.
 * dirty_subkeys: only a subset of subkeys is dirty.
 * if both dirty_data and dirty_subkeys are set, then all subkeys may be dirty,
 * effect is same as dirty_data. */
void notifyKeyspaceEventDirtySubkeys(int type, char *event, robj *key,
        int dbid, robj *o, int count, sds *subkeys) {
    if (server.swap_dirty_subkeys_enabled) {
        if (!objectIsDataDirty(o)) {
            redisDb *db = server.db+dbid;
            robj *dss = lookupDirtySubkeys(db,key);

            if (dss == NULL) {
                dss = dirtySubkeysNew();
                dbAddDirtySubkeys(db,key,dss);
            }

            for (int i = 0; i < count; i++) {
                dirtySubkeysAdd(dss,subkeys[i]);
            }
        }

        if (!objectIsMetaDirty(o)) setObjectMetaDirty(o);
    } else {
        setObjectDirty(o);
    }

    notifyKeyspaceEvent(type,event,key,dbid);
}

void notifyKeyspaceEventDirtyMeta(int type, char *event, robj *key,
        int dbid, robj *o) {
    setObjectMetaDirty(o);
    notifyKeyspaceEvent(type,event,key,dbid);
}

