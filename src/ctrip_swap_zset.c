//
// Created by dong on 2022/8/22.
//
#include "ctrip_swap.h"

void zsetTransformBig(robj *o, objectMeta *m) {
    size_t zset_size;

    serverAssert(o && o->type == OBJ_ZSET);

    //TODO bigzset=>wholekey not allowed if there are rocksdb subkeys: 
    //modify meta need requestGetIOAndLock, which requires extra work. support it if
    //wholekey does have performance advantage.
    if (m != NULL || o->big) return;

    zset_size = objectEstimateSize(o);
    if (zset_size > server.swap_big_zset_threshold) {
        o->big = 1;
        o->dirty = 1; /* rocksdb format changed, set dirty to trigger PUT. */
    }
}

static void createFakeZSetForDeleteIfNeeded(bigZSetSwapData *data) {
    if (data->hd.evict) {
        redisDb *db = data->hd.db;
        robj *key = data->hd.key;
        objectMeta *meta_dup = NULL;
        if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
        if (dictSize(db->meta) > 0) {
            meta_dup = dupObjectMeta(data->hd.meta);
            dictDelete(db->meta,key->ptr);
        }
        dictDelete(db->evict,key->ptr);
        dbAdd(db,key, createZsetObject());
        dbAddMeta(db,key,meta_dup);
        data->hd.meta = meta_dup;
    }
}
int bigZSetSwapAna(swapData *data_, int cmd_intention,
                   uint32_t cmd_intention_flags, struct keyRequest *req,
                   int *intention, uint32_t *intention_flags, void *datactx_) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    serverAssert(req->num_subkeys >= 0);
    if (intention_flags) *intention_flags = cmd_intention_flags;
    switch (cmd_intention) {
        case SWAP_NOP:
            *intention = SWAP_NOP;
            *intention_flags = 0;
            break;
        case SWAP_IN:
            if (data->hd.meta == NULL) {
                /* No need to swap in for pure hot key */
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else if (req->num_subkeys == 0) {
                if (cmd_intention_flags == INTENTION_IN_DEL) {
                    if (data->hd.meta->len == 0) {
                        *intention = SWAP_DEL;
                        *intention_flags = INTENTION_DEL_ASYNC;
                        if(data->hd.value) data->hd.value->dirty = 1;
                    } else {
                        *intention = SWAP_IN;
                        *intention_flags = INTENTION_IN_DEL;
                    }     
                } else if (cmd_intention_flags == INTENTION_IN_DEL_MOCK_VALUE) {
                    /* DEL/GETDEL: Lazy delete current key. */
                    createFakeZSetForDeleteIfNeeded(data);
                    *intention = SWAP_DEL;
                    *intention_flags = INTENTION_DEL_ASYNC;
                } else if (cmd_intention_flags == INTENTION_IN_META) {
                    /* HLEN: no need to swap in anything, hlen command will
                     * be modified like dbsize. */
                    *intention = SWAP_NOP;
                    *intention_flags = 0;
                } else if (data->hd.meta->len == 0) {
                    /* no need to swap in, all subkeys are in memory. */
                    *intention = SWAP_NOP;
                    *intention_flags = 0;
                } else {
                    /* HKEYS/HVALS/..., swap in all fields */
                    *intention = SWAP_IN;
                    datactx->hctx.num = 0;
                    datactx->hctx.subkeys = NULL;
                }
            } else { /* keyrequests with subkeys */
                datactx->hctx.num = 0;
                datactx->hctx.subkeys = zmalloc(req->num_subkeys * sizeof(robj*));
                for (int i = 0; i < req->num_subkeys; i++) {
                    robj *subkey = req->subkeys[i];
                    /* HDEL: even if field is hot (exists in value), we still
                     * need to do ROCKS_DEL on those fields. */
                    double score;
                    if (cmd_intention_flags == INTENTION_IN_DEL ||
                        data->hd.value == NULL || 
                        zsetScore(data->hd.value, subkey->ptr, &score) == C_ERR) {
                        incrRefCount(subkey);
                        datactx->hctx.subkeys[datactx->hctx.num++] = subkey;
                    }
                }

                *intention = datactx->hctx.num > 0 ? SWAP_IN : SWAP_NOP;
            }
            break;
        case SWAP_OUT:
            if (data->hd.value == NULL) {
                *intention = SWAP_NOP;
            } else {
                unsigned long long evict_memory = 0;
                datactx->hctx.subkeys = zmalloc(
                        server.swap_evict_step_max_subkeys*sizeof(robj*));
                
                int len = zsetLength(data->hd.value);
                robj *subkey;
                if (len > 0) {
                    if (data->hd.value->encoding == OBJ_ENCODING_ZIPLIST) {
                        unsigned char *zl = data->hd.value->ptr;
                        unsigned char *eptr, *sptr;
                        unsigned char *vstr;
                        unsigned int vlen;
                        long long vlong;
                        eptr = ziplistIndex(zl, 0);
                        sptr = ziplistNext(zl, eptr);
                        while (eptr != NULL) {
                            vlong = 0;
                            ziplistGet(eptr, &vstr, &vlen, &vlong);
                            evict_memory += vlen;
                            if (vstr != NULL) {
                                subkey = createStringObject((const char*)vstr, vlen);
                            } else {
                                subkey = createObject(OBJ_STRING,sdsfromlonglong(vlong));
                            }
                            datactx->hctx.subkeys[datactx->hctx.num++] = subkey;
                            ziplistGet(sptr, &vstr, &vlen, &vlong);
                            evict_memory += vlen;
                            if (datactx->hctx.num >= server.swap_evict_step_max_subkeys ||
                                    evict_memory >= server.swap_evict_step_max_memory) {
                                /* Evict big hash in small steps. */
                                break;
                            }
                            zzlNext(zl, &eptr, &sptr);
                        }
                    } else if (data->hd.value->encoding == OBJ_ENCODING_SKIPLIST) {
                        zset *zs = data->hd.value->ptr;
                        dict* d = zs->dict;
                        dictIterator* di = dictGetIterator(d);
                        dictEntry *de;
                        while ((de = dictNext(di)) != NULL) {
                            sds skey = dictGetKey(de);
                            subkey = createStringObject(skey, sdslen(skey));
                            datactx->hctx.subkeys[datactx->hctx.num++] = subkey;
                            evict_memory += sizeof(zset) + sizeof(dictEntry);
                            if (datactx->hctx.num >= server.swap_evict_step_max_subkeys ||
                                    evict_memory >= server.swap_evict_step_max_memory) {
                                /* Evict big hash in small steps. */
                                break;
                            }
                        }
                        dictReleaseIterator(di);
                    } else {
                        *intention = SWAP_NOP;
                        return 0;
                    }
                }

                /* create new meta if needed, meta version
                * will be used to encode data. */
                if (data->hd.meta == NULL)
                    datactx->hctx.new_meta = createObjectMeta(0);

                if (!data->hd.value->dirty || len == 0) {
                    swapData *data_ = (swapData*)data;
                    /* directly evict value db.dict if not dirty. */
                    swapDataCleanObject(data_, datactx);
                    swapDataSwapOut(data_,datactx);
                    *intention = SWAP_NOP;
                } else {
                    *intention = SWAP_OUT;
                }
            }
            break;
        case SWAP_DEL:
            *intention = SWAP_DEL;
            break;
        default:
            break;
    }
    return 0;
}

static robj *createSwapOutObject(robj *value, robj *evict) {
    serverAssert(value && !evict);
    robj *swapout = createObject(value->type,NULL);
    swapout->lru = value->lru;
    swapout->big = 1;
    return swapout;
}


/* subkeys already cleaned by cleanObject(to save cpu usage of main thread),
 * swapout updates keyspace (db.meta/db.evict/db.dict/db.expire). */
int bigZSetSwapOut(swapData *data_, void *datactx_) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    serverAssert(data->hd.value && !data->hd.evict);
    serverAssert(datactx->hctx.meta_len_delta >= 0);
    int len = zsetLength(data->hd.value);
    if (len == 0) {
        robj *swapout;
        long long expire;
        if (data->hd.meta) {
            datactx->hctx.new_meta = dupObjectMeta(data->hd.meta);
            datactx->hctx.new_meta->len += datactx->hctx.meta_len_delta;
        } else {
            serverAssert(datactx->hctx.meta_len_delta);
            datactx->hctx.new_meta->len += datactx->hctx.meta_len_delta;
        }
        expire = getExpire(data->hd.db,data->hd.key);
        if (expire != -1) removeExpire(data->hd.db,data->hd.key);
        dbDeleteMeta(data->hd.db,data->hd.key);
        dictDelete(data->hd.db->dict,data->hd.key->ptr);
        swapout = createSwapOutObject(data->hd.value,data->hd.evict);
        dbAddEvict(data->hd.db,data->hd.key,swapout);
        if (expire != -1) setExpire(NULL,data->hd.db,data->hd.key,expire);
        serverAssert(datactx->hctx.new_meta->len >= 0);
        dbAddMeta(data->hd.db,data->hd.key,datactx->hctx.new_meta);
        datactx->hctx.new_meta = NULL; /* moved */
    } else {
        
        /* not all fields swapped out. */
        if (!data->hd.meta) {
            if (datactx->hctx.meta_len_delta == 0) {
                /* hot hash stays hot: nop */
            } else {
                /* hot key turns warm: add meta. */
                datactx->hctx.new_meta->len += datactx->hctx.meta_len_delta;
                serverAssert(datactx->hctx.new_meta->len >= 0);
                dbAddMeta(data->hd.db,data->hd.key,datactx->hctx.new_meta);
                datactx->hctx.new_meta = NULL; /* moved */
            }
        } else {
            /* swap out some. */
            data->hd.meta->len += datactx->hctx.meta_len_delta;
            serverAssert(data->hd.meta->len >= 0);
        }
    }
    
    return C_OK;
}

static sds bigZSetEncodeSubkey(sds key, sds subkey) {
    sds rawkey = rocksEncodeSubkey(rocksGetEncType(OBJ_ZSET,1),key,subkey);
    return rawkey;
}

static sds bigZSetEncodeSubval(double score) {
    rio sdsrdb;
    rioInitWithBuffer(&sdsrdb,sdsempty());
    rdbSaveType(&sdsrdb,RDB_TYPE_STRING);
    rdbSaveBinaryDoubleValue(&sdsrdb, score);
    return sdsrdb.io.buffer.ptr;
}

static double bigZSetDecodeSubval(sds subval) {
    rio sdsrdb;
    rioInitWithBuffer(&sdsrdb, subval);
    serverAssert(rdbLoadType(&sdsrdb) == RDB_TYPE_STRING);
    double score = 0;
    serverAssert(rdbLoadBinaryDoubleValue(&sdsrdb, &score) != -1);
    return score;
}

int bigZSetEncodeData(swapData *data_, int intention, void *datactx_,
        int *action, int *numkeys, sds **prawkeys, sds **prawvals) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    if (datactx->hctx.num == 0) {
        *action = 0;
        *numkeys = 0;
        *prawkeys = NULL;
        *prawvals = NULL;
        return C_OK;
    }
    sds *rawkeys = zmalloc(datactx->hctx.num*sizeof(sds));
    sds *rawvals = zmalloc(datactx->hctx.num*sizeof(sds));
    serverAssert(intention == SWAP_OUT);
    for (int i = 0; i < datactx->hctx.num; i++) {
        rawkeys[i] = bigZSetEncodeSubkey(data->hd.key->ptr,
                datactx->hctx.subkeys[i]->ptr);
        double score = 0;
        serverAssert(C_OK == zsetScore(data->hd.value,
                datactx->hctx.subkeys[i]->ptr, &score));
        rawvals[i] = bigZSetEncodeSubval(score);
    }
    *action = ROCKS_WRITE;
    *numkeys = datactx->hctx.num;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    return C_OK;
}


int bigZSetCleanObject(swapData *data_, void *datactx_) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    if (!data->hd.value) return C_OK;
    for (int i = 0; i < datactx->hctx.num; i++) {
        if (zsetDel(data->hd.value,datactx->hctx.subkeys[i]->ptr)) {
            datactx->hctx.meta_len_delta++;
        }
    }
    return C_OK;
}

static robj *createSwapInObject(robj *newval, robj *evict, int data_ditry) {
    robj *swapin = newval;
    serverAssert(newval && newval->type == OBJ_ZSET);
    serverAssert(evict && evict->type == OBJ_ZSET);
    incrRefCount(newval);
    swapin->lru = evict->lru;
    swapin->big = 1;
    swapin->dirty = data_ditry;
    return swapin;
}

/* Note: meta are kept even if all subkeys are swapped in (hot) because
 * meta.version is necessary for swapping in evicted data if not dirty.
 * In fact, meta are kept as long as there are data in rocksdb. */
int bigZSetSwapIn(swapData *data_, robj *result, void *datactx_) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    objectMeta *meta = data->hd.meta;

    /* hot key no need to swap in, this must be a warm or cold key. */
    serverAssert(meta);
    if (!data->hd.value && result == NULL) {
        /* cold key swapped in nothing: nop. */
    } else if (!data->hd.value && result != NULL) {
        /* cold key swapped in fields */
        /* dup expire/meta satellites before evict deleted. */
        meta = dupObjectMeta(data->hd.meta);
        meta->len += datactx->hctx.meta_len_delta;
        serverAssert(meta->len >= 0);
        long long expire = getExpire(data->hd.db,data->hd.key);
        robj *swapin = createSwapInObject(result,data->hd.evict, datactx->hctx.swapin_del_flag & SWAPIN_DEL);
        if (expire != -1) removeExpire(data->hd.db,data->hd.key);
        dbDeleteMeta(data->hd.db,data->hd.key);
        dictDelete(data->hd.db->evict,data->hd.key->ptr);
        dbAdd(data->hd.db,data->hd.key,swapin);
        /* re-add expire/meta satellites for db.dict .*/
        if (expire != -1) setExpire(NULL,data->hd.db,data->hd.key,expire);
        if (!(datactx->hctx.swapin_del_flag & SWAPIN_DEL_FULL)) {
            dbAddMeta(data->hd.db,data->hd.key,meta);
        } else {
            serverAssert(meta->len == 0);
            freeObjectMeta(meta);
        }
    } else {
        /* if data.value exists, then we expect all fields merged already
         * and nothing need to be swapped in. */
        serverAssert(result == NULL);
        data->hd.meta->len += datactx->hctx.meta_len_delta;
        serverAssert(data->hd.meta->len >= 0);
        data->hd.value->dirty = datactx->hctx.swapin_del_flag & SWAPIN_DEL;
        if (datactx->hctx.swapin_del_flag & SWAPIN_DEL_FULL) {
            serverAssert(data->hd.meta->len == 0);
            dbDeleteMeta(data->hd.db, data->hd.key);
        }
    }
    
    return C_OK;
}



int bigZSetEncodeKeys(swapData *data_, int intention, void *datactx_,
        int *action, int *numkeys, sds **prawkeys) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    sds *rawkeys = NULL;

    switch (intention) {
    case SWAP_IN:
        if (datactx->hctx.num) { /* Swap in specific fields */
            int i;
            rawkeys = zmalloc(sizeof(sds)*datactx->hctx.num);
            for (i = 0; i < datactx->hctx.num; i++) {
                rawkeys[i] = bigZSetEncodeSubkey(
                        data->hd.key->ptr,datactx->hctx.subkeys[i]->ptr);
            }
            *numkeys = datactx->hctx.num;
            *prawkeys = rawkeys;
            *action = ROCKS_MULTIGET;
        } else { /* Swap in entire hash. */
            rawkeys = zmalloc(sizeof(sds));
            rawkeys[0] = bigZSetEncodeSubkey(
                    data->hd.key->ptr,NULL);
            *numkeys = 1;
            *prawkeys = rawkeys;
            *action = ROCKS_SCAN;
        }
        return C_OK;
    case SWAP_DEL:
        if (data->hd.meta) {
            rawkeys = zmalloc(sizeof(sds)*2);
            rawkeys[0] = rocksEncodeSubkey(rocksGetEncType(OBJ_ZSET,1),data->hd.key->ptr,NULL);
            rawkeys[1] = rocksCalculateNextKey(rawkeys[0]);
            serverAssert(rawkeys[1] != NULL);
            *numkeys = 2;
            *prawkeys = rawkeys;
            *action = ROCKS_DELETERANGE;
        } else {
            *action = 0;
            *numkeys = 0;
            *prawkeys = NULL;
        }
        return C_OK;
    case SWAP_OUT:
    default:
        /* Should not happen .*/
        *action = 0;
        *numkeys = 0;
        *prawkeys = NULL;
        return C_OK;
    }

    return C_OK;
}

// int rocksDecodeZSetSubkey(const char *raw, size_t rawlen,
//                       const char **key, size_t *klen, const char **sub, size_t *slen) {
//     int obj_type;
//     unsigned int _klen;
//     if (rawlen <= 1+sizeof(unsigned int)) return -1;

//     if ((obj_type = rocksGetObjectType(raw[0])) < 0) return -1;
//     raw++, rawlen--;

//     _klen = *(unsigned int*)raw;
//     if (klen) *klen = (size_t)_klen;
//     raw += sizeof(unsigned int);
//     rawlen -= sizeof(unsigned int);

//     if (key) *key = raw;
//     raw += _klen;
//     rawlen-= _klen;

//     if (rawlen < 0) return -1;
//     if (sub) {
//         if (rawlen != 0) {
//             *sub = raw;
//         } else {
//             *sub = NULL;
//         }
//     }
//     if (slen) *slen = rawlen;
//     return obj_type;
// }

int bigZSetDecodeData(swapData *data_, int num, sds *rawkeys,
        sds *rawvals, robj **pdecoded) {
    int i;
    robj *decoded = NULL;
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    serverAssert(num >= 0);

    if (num == 0) {
        *pdecoded = NULL;
        return C_OK;
    }
    decoded = createZsetZiplistObject();
    for (i = 0; i < num; i++) {
        sds subkey;
        const char *keystr, *subkeystr;
        size_t klen, slen;


        if (rawvals[i] == NULL || sdslen(rawvals[i]) == 0)
            continue;
        if (rocksDecodeSubkey(rawkeys[i],sdslen(rawkeys[i]),
                &keystr,&klen,&subkeystr,&slen) < 0)
            continue;
        /* Decode do not hold obselete data.*/
        if (data->hd.meta == NULL)
            continue;
        subkey = sdsnewlen(subkeystr,slen);
        serverAssert(memcmp(data->hd.key->ptr,keystr,klen) == 0); //TODO remove
        double score = bigZSetDecodeSubval(rawvals[i]);
        int flag = ZADD_IN_NONE;
        int retflags = 0;
        double newscore;
        serverAssert(zsetAdd(decoded,score, subkey, flag, &retflags, &newscore) == 1);
        sdsfree(subkey);
    }   
    *pdecoded = decoded;
    return C_OK;
}

/* decoded moved back by exec to bighash*/
robj *bigZSetCreateOrMergeObject(swapData *data_, robj *decoded, void *datactx_, int del_flag) {
    robj *result;
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    serverAssert(decoded == NULL || decoded->type == OBJ_ZSET);
    datactx->hctx.swapin_del_flag = del_flag;
    int decoded_len = zsetLength(decoded);
    if (!data->hd.value || !decoded) {
        /* decoded moved to exec again. */
        result = decoded;
        if (decoded) {
            datactx->hctx.meta_len_delta -= decoded_len;
        } 
    } else {
        int flag = ZADD_IN_NX;
        int retflags = 0;
        double newscore;
        if (decoded_len > 0) {
            
            if (decoded->encoding == OBJ_ENCODING_ZIPLIST) {
                unsigned char *zl = decoded->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;
                eptr = ziplistIndex(zl, 0);
                sptr = ziplistNext(zl, eptr);
                while(eptr != NULL) {
                    vlong = 0;
                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    sds subkey;
                    if (vstr != NULL) {
                        subkey = sdsnewlen(vstr, vlen);
                    } else {
                        subkey = sdsfromlonglong(vlong);
                    }
                    double score = zzlGetScore(sptr);
                    if (zsetAdd(data->hd.value, score, subkey, flag, &retflags, &newscore) == 1) {
                        if (retflags & ZADD_OUT_ADDED) {
                            datactx->hctx.meta_len_delta --;
                        } 
                    }
                    sdsfree(subkey);
                    zzlNext(zl, &eptr, &sptr);
                }
            } else if (decoded->encoding == OBJ_ENCODING_SKIPLIST) {
                zset* zs = decoded->ptr;
                dict* d = zs->dict;
                dictIterator* di = dictGetIterator(d);
                dictEntry *de;
                while ((de = dictNext(di)) != NULL) {
                    sds subkey = dictGetKey(de);
                    double score = *(double*)dictGetVal(de);
                    if (zsetAdd(data->hd.value, score, subkey, flag, &retflags, &newscore) == 1) {
                        if (retflags & ZADD_OUT_ADDED) {
                            datactx->hctx.meta_len_delta --;
                        }
                    }
                }
                dictReleaseIterator(di);
            }
        }
        /* decoded merged, we can release it now. */
        decrRefCount(decoded);
        result = NULL;
    }
    return result;
}

int bigZSetSwapDel(swapData *data_, void *datactx, int async) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    UNUSED(datactx);
    if (async) {
        if (data->hd.meta) {
            dbDeleteMeta(data->hd.db,data->hd.key);
        }
        return C_OK;
    } else {
        if (data->hd.value) dbDelete(data->hd.db,data->hd.key);
        if (data->hd.evict) dbDeleteEvict(data->hd.db,data->hd.key);
        return C_OK;
    }
}

void freeBigZSetSwapData(swapData *data_, void *datactx_) {
    bigZSetSwapData *data = (bigZSetSwapData*)data_;
    bigZSetDataCtx *datactx = datactx_;
    cleanBigHashSwapData((swapData*)(&(data->hd)), (void*)(&(datactx->hctx)));
    zfree(data);
    zfree(datactx);
}

swapDataType bigZSetSwapDataType = {
        .name = "bigZSet",
        .swapAna = bigZSetSwapAna,
        .encodeData = bigZSetEncodeData,
        .cleanObject = bigZSetCleanObject,
        .swapOut = bigZSetSwapOut,
        .encodeKeys = bigZSetEncodeKeys,
        .decodeData = bigZSetDecodeData,
        .createOrMergeObject = bigZSetCreateOrMergeObject,
        .swapIn = bigZSetSwapIn,
        .swapDel = bigZSetSwapDel,
        .free = freeBigZSetSwapData
};

swapData* createBigZSetSwapData(redisDb *db, robj* key, robj* value, robj *evict, objectMeta *meta, void **pdatactx) {
    bigZSetSwapData *data = zmalloc(sizeof(bigZSetSwapData));
    data->hd.d.type = &bigZSetSwapDataType;
    if (key) incrRefCount(key);
    data->hd.key = key;
    if (value) incrRefCount(value);
    data->hd.value = value;
    if (evict) incrRefCount(evict);
    data->hd.evict = evict;
    data->hd.meta = meta;
    data->hd.db = db;
    bigZSetDataCtx *datactx = zmalloc(sizeof(bigZSetDataCtx));
    datactx->hctx.meta_len_delta = 0;
    datactx->hctx.num = 0;
    datactx->hctx.subkeys = NULL;
    datactx->hctx.swapin_del_flag = SWAPIN_NO_DEL;
    if (pdatactx) *pdatactx = datactx;
    return (swapData*)data;
}


int bigzsetSaveStart(rdbKeyData *keydata, rio *rdb) {
    robj *x;
    robj *key = keydata->savectx.bigzset.key;
    int ret = 0;

    if (keydata->savectx.value)
        x = keydata->savectx.value;
    else
        x = keydata->savectx.evict;
    
    /* save header */
    if (rdbSaveKeyHeader(rdb,key,x,RDB_TYPE_ZSET_2,
                keydata->savectx.expire) == -1)
        return -1;
    ssize_t zlen = 0;
    if (keydata->savectx.value) {
        zlen += zsetLength(x);
    }
    if (keydata->savectx.bigzset.meta) {
        zlen += keydata->savectx.bigzset.meta->len;
    }
    
    if (rdbSaveLen(rdb,zlen) == -1)
            return -1;
    if (!keydata->savectx.value) {
        return 0;
    }
    if (x->encoding == OBJ_ENCODING_ZIPLIST) {
        int len = zsetLength(keydata->savectx.value);
        unsigned char *zl = keydata->savectx.value->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        eptr = ziplistIndex(zl, 0);
        sptr = ziplistNext(zl, eptr);
        while(len > 0) {
            vlong = 0;
            ziplistGet(eptr, &vstr, &vlen, &vlong);
            // sds subkey = sdsnewlen(vstr, vlen);
            double score = zzlGetScore(sptr);
            if (vstr != NULL) {
                if ((rdbSaveRawString(rdb,
                            vstr,vlen)) == -1) {
                    return -1;
                }
            } else {
                char buf[128];
                int len = ll2string(buf, 128, vlong);
                if ((rdbSaveRawString(rdb, (unsigned char*)buf, len)) == -1) {
                    return -1;
                }
            }
            
            if ((rdbSaveBinaryDoubleValue(rdb,score)) == -1)
                return -1;
            // sdsfree(subkey);
            zzlNext(zl, &eptr, &sptr);
            len--;
        }
    } else if (x->encoding == OBJ_ENCODING_SKIPLIST) {
        
        zset *zs = keydata->savectx.value->ptr;
        zskiplist* zsl = zs->zsl;
        /* save fields from value (db.dict) */
        zskiplistNode* zn = zsl->tail;
        while (zn != NULL) {
            if ((rdbSaveRawString(rdb,
                        (unsigned char*)zn->ele,sdslen(zn->ele))) == -1)
            {
                return -1;
            }
            if ((rdbSaveBinaryDoubleValue(rdb,zn->score)) == -1)
                return -1;
            zn = zn->backward;
        }
    }
    return ret;
}


/* return 1 if bighash still need to consume more rawkey. */
int bigzsetSave(rdbKeyData *keydata, rio *rdb, decodeResult *decoded) {
    robj *key = keydata->savectx.bigzset.key;
    serverAssert(!sdscmp(decoded->key, key->ptr));
    if (decoded->enc_type != ENC_TYPE_ZSET_SUB || decoded->rdbtype != RDB_TYPE_STRING) {
        /* check failed, skip this key */
        return 0;
    }
    double score;
    if (keydata->savectx.value != NULL) {
        if (zsetScore(keydata->savectx.value,
                    decoded->subkey, &score) == C_OK) {
            /* already save in save_start, skip this subkey */
            return 0;
        }
    }
    if (rdbSaveRawString(rdb,(unsigned char*)decoded->subkey,
                sdslen(decoded->subkey)) == -1) {
        return -1;
    }
    
    rio sdsrdb;
    rioInitWithBuffer(&sdsrdb, decoded->rdbraw);
    if (rdbWriteRaw(rdb, decoded->rdbraw, sizeof(double)) == -1) {
        return -1;
    }
    keydata->savectx.bigzset.saved++;
    return 0;
}

int bigzsetSaveEnd(rdbKeyData *keydata, int save_result) {
    objectMeta *meta = keydata->savectx.bigzset.meta;
    if (keydata->savectx.bigzset.saved != meta->len) {
        sds key  = keydata->savectx.bigzset.key->ptr;
        sds repr = sdscatrepr(sdsempty(), key, sdslen(key));
        serverLog(LL_WARNING, "key(%s) save end saved(%d) != meta->len(%ld)", repr, keydata->savectx.bigzset.saved, meta->len);
        sdsfree(repr);
        return -1;
    }
    
    return save_result;
}

void bigzsetSaveDeinit(rdbKeyData *keydata) {
    if (keydata->savectx.bigzset.key) {
        decrRefCount(keydata->savectx.bigzset.key);
        keydata->savectx.bigzset.key = NULL;
    }
}

int bigzsetRdbLoad(struct rdbKeyData *keydata, rio *rdb, sds *rawkey,
        sds *rawval, int *error) {
    sds subkey, key = keydata->loadctx.key;
    if (keydata->loadctx.bigzset.zset_nfields == 0) {
        *error = RDB_LOAD_ERR_EMPTY_KEY;
        return 0;
    }
    *error = RDB_LOAD_ERR_OTHER;
    if ((subkey = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
        serverLog(LL_WARNING, "load fail 1");
        return 0;
    }
    double score;
    if (keydata->loadctx.rdbtype == RDB_TYPE_ZSET_2) {
        if (rdbLoadBinaryDoubleValue(rdb,&score) == -1) {
            sdsfree(subkey);
            return 0;
        }
    } else if (keydata->loadctx.rdbtype == RDB_TYPE_ZSET) {
        if (rdbLoadDoubleValue(rdb, &score) == -1) {
            sdsfree(subkey);
            return 0;
        }
    } else {
        serverAssert(0);
    }
    
    *error = 0;
    *rawkey = rocksEncodeSubkey(ENC_TYPE_ZSET_SUB,key,subkey);
    *rawval = bigZSetEncodeSubval(score);
    sdsfree(subkey);
    keydata->loadctx.bigzset.meta->len++;
    return keydata->loadctx.bigzset.meta->len < keydata->loadctx.bigzset.zset_nfields;
}

int bigzsetRdbLoadDbAdd(struct rdbKeyData *keydata, redisDb *db) {
    robj keyobj;
    sds key = keydata->loadctx.key;
    initStaticStringObject(keyobj,key);
    if (lookupKey(db,&keyobj,LOOKUP_NOTOUCH) || lookupEvictKey(db,&keyobj))
        return 0;
    dbDeleteMeta(db,&keyobj);
    serverAssert(dbAddEvictRDBLoad(db,key,keydata->loadctx.bigzset.evict));
    dbAddMeta(db,&keyobj,keydata->loadctx.bigzset.meta);
    return 1;
}

rdbKeyType bigZSetRdbType = {
    .save_start = bigzsetSaveStart,
    .save = bigzsetSave,
    .save_end = bigzsetSaveEnd,
    .save_deinit = bigzsetSaveDeinit,
    .load = bigzsetRdbLoad,
    .load_end = NULL,
    .load_dbadd = bigzsetRdbLoadDbAdd,
    .load_deinit = NULL,
};
/* rdb save */
void rdbKeyDataInitSaveBigZSet(rdbKeyData *keydata, robj *value, robj *evict, objectMeta *meta, long long expire, sds keystr) {
    rdbKeyDataInitSaveKey(keydata,value,evict,expire);
    keydata->type = &bigZSetRdbType;
    keydata->savectx.type = RDB_KEY_TYPE_BIGZSET;
    keydata->savectx.bigzset.meta = meta;
    keydata->savectx.bigzset.key = createStringObject(keystr,sdslen(keystr));
    keydata->savectx.bigzset.saved = 0;
}

/* rdb load */
void rdbKeyDataInitLoadBigZSet(rdbKeyData *keydata, int rdbtype, sds key) {
    robj *evict;
    rdbKeyDataInitLoadKey(keydata,rdbtype,key);
    keydata->type = &bigZSetRdbType;
    keydata->loadctx.type = RDB_KEY_TYPE_BIGZSET;
    keydata->loadctx.bigzset.zset_nfields = 0;
    keydata->loadctx.bigzset.meta = createObjectMeta(0);
    evict = createObject(OBJ_ZSET,NULL);
    evict->big = 1;
    keydata->loadctx.bigzset.evict = evict;
}


#ifdef REDIS_TEST

#define SWAP_EVICT_STEP 2
#define SWAP_EVICT_MEM  (1*1024*1024)

int swapDataBigZSetTest(int argc, char **argv, int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    initTestRedisServer();
    redisDb* db = server.db + 0;
    int error = 0;
    swapData *zset1_data, *cold1_data;
    bigZSetDataCtx *zset1_ctx, *cold1_ctx;
    robj *key1, *zset1, *cold1, *cold1_evict;
    objectMeta *cold1_meta;
    keyRequest _kr1, *kr1 = &_kr1, _cold_kr1, *cold_kr1 = &_cold_kr1;
    int intention;
    uint32_t intention_flags;
    robj *subkeys1[4];
    sds f1,f2,f3,f4;
    int action, numkeys;
    sds *rawkeys, *rawvals;
    
    TEST("bigZset - init") {
        server.swap_evict_step_max_subkeys = SWAP_EVICT_STEP;
        server.swap_evict_step_max_memory = SWAP_EVICT_MEM;

        key1 = createStringObject("key1",4);
        cold1 = createStringObject("cold1",5);
        cold1_evict = createObject(OBJ_ZSET,NULL);
        cold1_evict->big = 1;
        cold1_meta = createObjectMeta(4);
        f1 = sdsnew("f1"), f2 = sdsnew("f2"), f3 = sdsnew("f3"), f4 = sdsnew("f4");
        zset1 = createZsetObject();
        zset1->big = 1;
        int flags ;
        double ns;
        zsetAdd(zset1, 1.0, f1, ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1, 2.0, f2, ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1, 3.0, f3, ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1, 4.0, f4, ZADD_IN_NONE, &flags, &ns);
        incrRefCount(zset1);
        kr1->key = key1;
        kr1->level = REQUEST_LEVEL_KEY;
        kr1->num_subkeys = 0;
        kr1->subkeys = NULL;
        incrRefCount(key1);
        cold_kr1->key = key1;
        cold_kr1->level = REQUEST_LEVEL_KEY;
        cold_kr1->num_subkeys = 0;
        cold_kr1->subkeys = NULL;
        dbAdd(db,key1,zset1);
        zset1_data = createBigZSetSwapData(db,key1,zset1,NULL,NULL,(void**)&zset1_ctx);
        cold1_data = createBigZSetSwapData(db,cold1,NULL,cold1_evict,cold1_meta,(void**)&cold1_ctx);
    }

    TEST("bigZSet - swapAna") {
        /* nop: NOP/IN_META/IN_DEL/IN hot/OUT cold/DEL_ASYNC... */
        swapDataAna(zset1_data,SWAP_NOP,0,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(zset1_data,SWAP_IN,INTENTION_IN_META,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(zset1_data,SWAP_IN,INTENTION_IN_DEL_MOCK_VALUE,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(zset1_data,SWAP_IN,INTENTION_IN_DEL,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(zset1_data,SWAP_IN,0,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(zset1_data,SWAP_IN,0,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(cold1_data,SWAP_OUT,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        swapDataAna(cold1_data,SWAP_DEL,INTENTION_DEL_ASYNC,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_DEL && intention_flags == INTENTION_DEL_ASYNC);
        /* in: entire or with subkeys */
        swapDataAna(cold1_data,SWAP_IN,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->hctx.num == 0 && cold1_ctx->hctx.subkeys == NULL);
        subkeys1[0] = createStringObject(f1,sdslen(f1));
        subkeys1[1] = createStringObject(f2,sdslen(f2));
        cold_kr1->num_subkeys = 2;
        cold_kr1->subkeys = subkeys1;
        swapDataAna(cold1_data,SWAP_IN,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->hctx.num == 2 && cold1_ctx->hctx.subkeys != NULL);
        /* out: evict by small steps */
        kr1->num_subkeys = 0;
        kr1->subkeys = NULL;
        swapDataAna(zset1_data,SWAP_OUT,0,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(cold1_ctx->hctx.num == SWAP_EVICT_STEP && cold1_ctx->hctx.subkeys != NULL);
    }

    TEST("bigZSet - encodeData/DecodeData") {
        robj *decoded;
        size_t old = server.swap_evict_step_max_subkeys;
        bigZSetSwapData *zset1_data_ = (bigZSetSwapData*)zset1_data;
        server.swap_evict_step_max_subkeys = 1024;
        kr1->num_subkeys = 0;
        kr1->subkeys = NULL;
        zfree(zset1_ctx->hctx.subkeys), zset1_ctx->hctx.subkeys = NULL;
        zset1_ctx->hctx.num = 0;
        zset1_data_->hd.meta = createObjectMeta(1);
        swapDataAna(zset1_data,SWAP_OUT,0,kr1,&intention,&intention_flags,zset1_ctx);
        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(zset1_ctx->hctx.num == (int)zsetLength(zset1_data_->hd.value));
        serverAssert(zset1_ctx->hctx.subkeys != NULL);

        bigZSetEncodeData(zset1_data,intention,zset1_ctx,
                &action,&numkeys,&rawkeys,&rawvals);
        test_assert(action == ROCKS_WRITE);
        test_assert(numkeys == zset1_ctx->hctx.num);

        bigZSetDecodeData(zset1_data,numkeys,rawkeys,rawvals,&decoded);
        test_assert(zsetLength(decoded) == zsetLength(zset1));

        freeObjectMeta(zset1_data_->hd.meta);
        zset1_data_->hd.meta = NULL;
        server.swap_evict_step_max_subkeys = old;
    }

    TEST("bigZSet - swapIn_and_del") {
        robj *z, *e;
        objectMeta *m;
        robj* zset2 = createZsetObject();
        robj* key2 = createStringObject("key2",4);
        bigZSetDataCtx *zset2_ctx;
        
        test_assert(lookupMeta(db,key2) == NULL);
        sds zf1 = sdsnewlen("f1",2);
        sds zf2 = sdsnewlen("f2",2);
        int flags;
        double ns;
        zsetAdd(zset2, 1.0, zf1, ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset2, 2.0, zf2, ZADD_IN_NONE, &flags, &ns);
        test_assert(zsetLength(zset2) == 2);
        dbAdd(db,key2,zset2);
        // hot data
        bigZSetSwapData* zset2_data = (bigZSetSwapData*)createBigZSetSwapData(db,key2,zset2,NULL,NULL,(void**)&zset2_ctx);
        // clean
        zset2_ctx->hctx.num = 1;
        zset2_ctx->hctx.subkeys = zmalloc(sizeof(robj*) * zset2_ctx->hctx.num);
        zset2_ctx->hctx.subkeys[0] = createRawStringObject(zf1, sdslen(zf1));
        
        if (zset2_data->hd.meta == NULL)
            zset2_ctx->hctx.new_meta = createObjectMeta(0);
        bigZSetCleanObject((swapData*)zset2_data, zset2_ctx);
        test_assert(zset2_ctx->hctx.meta_len_delta == 1);
        // swapout
        bigZSetSwapOut((swapData*)zset2_data, zset2_ctx);
        test_assert((m = lookupMeta(db, key2)) != NULL);

        //swapout 2
        freeBigZSetSwapData((swapData*)zset2_data, zset2_ctx);
        zset2_data = (bigZSetSwapData*)createBigZSetSwapData(db,key2,zset2,NULL,m,(void**)&zset2_ctx);
        zset2_ctx->hctx.num = 1;
        zset2_ctx->hctx.subkeys = zmalloc(sizeof(robj*) * zset2_ctx->hctx.num);
        zset2_ctx->hctx.subkeys[0] = createRawStringObject(zf2, sdslen(zf2));
        bigZSetCleanObject((swapData*)zset2_data, zset2_ctx);
        test_assert(zset2_ctx->hctx.meta_len_delta == 1);
        // swapout
        bigZSetSwapOut((swapData*)zset2_data, zset2_ctx);
        test_assert((e = lookupEvictKey(db, key2)) != NULL);
        test_assert((m = lookupMeta(db, key2)) != NULL);
        test_assert((z = lookupKey(db, key2, LOOKUP_NOTOUCH)) == NULL);

        // swapIn_del 
        freeBigZSetSwapData((swapData*)zset2_data, zset2_ctx);
        zset2_data = (bigZSetSwapData*)createBigZSetSwapData(db,key2,NULL,e,m,(void**)&zset2_ctx);
        zset2_ctx->hctx.num = 1;
        zset2_ctx->hctx.subkeys = zmalloc(sizeof(robj*) * zset2_ctx->hctx.num);
        zset2_ctx->hctx.subkeys[0] = createRawStringObject(zf2, sdslen(zf2));

        zset2 = createZsetObject();
        zsetAdd(zset2, 2.0, zf2, ZADD_IN_NONE, &flags, &ns);
        z = bigZSetCreateOrMergeObject((swapData*)zset2_data,zset2,zset2_ctx,SWAPIN_DEL);
        test_assert(z != NULL);
        test_assert(zsetLength(z) == 1);
        bigZSetSwapIn((swapData*)zset2_data,z,zset2_ctx);
        test_assert((e = lookupEvictKey(db, key2)) == NULL);
        test_assert((m = lookupMeta(db, key2)) != NULL);
        test_assert(m->len == 1);
        test_assert((z = lookupKey(db, key2, LOOKUP_NOTOUCH)) != NULL);
        test_assert(z->dirty == 1);
        
        // swapIn_del all
        freeBigZSetSwapData((swapData*)zset2_data, zset2_ctx);
        zset2_data = (bigZSetSwapData*)createBigZSetSwapData(db,key2,z,e,m,(void**)&zset2_ctx);
        zset2_ctx->hctx.num = 2;
        zset2_ctx->hctx.subkeys = zmalloc(sizeof(robj*) * zset2_ctx->hctx.num);
        zset2_ctx->hctx.subkeys[0] = createRawStringObject(zf1, sdslen(zf1));
        zset2_ctx->hctx.subkeys[1] = createRawStringObject(zf2, sdslen(zf2));
        zset2 = createZsetObject();
        zsetAdd(zset2, 1.0, zf1, ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset2, 2.0, zf2, ZADD_IN_NONE, &flags, &ns);
        test_assert(zsetLength(zset2) == 2);
        z = bigZSetCreateOrMergeObject((swapData*)zset2_data,zset2,zset2_ctx,SWAPIN_DEL | SWAPIN_DEL_FULL);
        test_assert(z == NULL);
        robj* z1;
        test_assert((z1 = lookupKey(db, key2, LOOKUP_NOTOUCH)) != NULL);
        test_assert(zsetLength(z1) == 2);
        bigZSetSwapIn((swapData*)zset2_data,z,zset2_ctx);
        test_assert((e = lookupEvictKey(db, key2)) == NULL);
        test_assert((m = lookupMeta(db, key2)) == NULL);
        test_assert((z = lookupKey(db, key2, LOOKUP_NOTOUCH)) != NULL);
        test_assert(zsetLength(z) == 2);
        test_assert(z->dirty == 1);
        
        //swapOut
        freeBigZSetSwapData((swapData*)zset2_data, zset2_ctx);
        zset2_data = (bigZSetSwapData*)createBigZSetSwapData(db,key2,z,e,m,(void**)&zset2_ctx);
        // clean
        zset2_ctx->hctx.num = 2;
        zset2_ctx->hctx.subkeys = zmalloc(sizeof(robj*) * zset2_ctx->hctx.num);
        zset2_ctx->hctx.subkeys[0] = createRawStringObject(zf1, sdslen(zf1));
        zset2_ctx->hctx.subkeys[1] = createRawStringObject(zf2, sdslen(zf2));
        if (zset2_data->hd.meta == NULL)
            zset2_ctx->hctx.new_meta = createObjectMeta(0);
        bigZSetCleanObject((swapData*)zset2_data, zset2_ctx);
        test_assert(zset2_ctx->hctx.meta_len_delta == 2);
        // swapout
        bigZSetSwapOut((swapData*)zset2_data, zset2_ctx);
        test_assert((m = lookupMeta(db, key2)) != NULL);
        test_assert((e = lookupEvictKey(db, key2)) != NULL);
        test_assert((z = lookupKey(db, key2, LOOKUP_NOTOUCH)) == NULL);

        // swapIn_del all
        freeBigZSetSwapData((swapData*)zset2_data, zset2_ctx);
        zset2_data = (bigZSetSwapData*)createBigZSetSwapData(db,key2,z,e,m,(void**)&zset2_ctx);
        zset2_ctx->hctx.num = 2;
        zset2_ctx->hctx.subkeys = zmalloc(sizeof(robj*) * zset2_ctx->hctx.num);
        zset2_ctx->hctx.subkeys[0] = createRawStringObject(zf1, sdslen(zf1));
        zset2_ctx->hctx.subkeys[1] = createRawStringObject(zf2, sdslen(zf2));
        zset2 = createZsetObject();
        zsetAdd(zset2, 1.0, zf1, ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset2, 2.0, zf2, ZADD_IN_NONE, &flags, &ns);
        test_assert(zsetLength(zset2) == 2);
        z = bigZSetCreateOrMergeObject((swapData*)zset2_data,zset2,zset2_ctx,SWAPIN_DEL | SWAPIN_DEL_FULL);
        test_assert(z != NULL);
        test_assert(zsetLength(z) == 2);
        bigZSetSwapIn((swapData*)zset2_data,z,zset2_ctx);
        test_assert((e = lookupEvictKey(db, key2)) == NULL);
        test_assert((m = lookupMeta(db, key2)) == NULL);
        test_assert((z = lookupKey(db, key2, LOOKUP_NOTOUCH)) != NULL);
        test_assert(zsetLength(z) == 2);
        test_assert(z->dirty == 1);

    }

    TEST("bigZSet - swapIn/swapOut") {
        robj *h, *e;
        objectMeta *m;
        bigZSetSwapData _data = *(bigZSetSwapData*)zset1_data, *data = &_data;
        test_assert(lookupMeta(db,key1) == NULL);

        /* hot => warm => cold */
        zsetDel(zset1,f1);
        zsetDel(zset1,f2);
        zset1_ctx->hctx.meta_len_delta = 2;
        bigZSetSwapOut((swapData*)data, zset1_ctx);
        test_assert((m =lookupMeta(db,key1)) != NULL && m->len == 2);
        test_assert(lookupEvictKey(db,key1) == NULL);

        zsetDel(zset1,f3);
        zsetDel(zset1,f4);
        zset1_ctx->hctx.meta_len_delta = 2;
        data->hd.meta = lookupMeta(data->hd.db,data->hd.key);
        bigZSetSwapOut((swapData*)data, zset1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 4);
        test_assert((e = lookupEvictKey(db,key1)) != NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) == NULL);

        /* cold => warm => hot */
        zset1 = createZsetObject();
        int flags ;
        double ns;
        zsetAdd(zset1,1.0,f1,ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1,2.0,f2,ZADD_IN_NONE, &flags, &ns);
        zset1_ctx->hctx.meta_len_delta = -2;
        data->hd.value = h;
        data->hd.evict = e;
        data->hd.meta = m;
        bigZSetSwapIn((swapData*)data,zset1,zset1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 2);
        test_assert((e = lookupEvictKey(db,key1)) == NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(h->big && zsetLength(h) == 2);

        zsetAdd(zset1,3.0,f3,ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1,4.0,f4,ZADD_IN_NONE, &flags, &ns);
        data->hd.value = h;
        data->hd.evict = e;
        data->hd.meta = m;
        bigZSetCreateOrMergeObject((swapData*)data,zset1,zset1_ctx,0);
        bigZSetSwapIn((swapData*)data,NULL,zset1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL);
        test_assert((e = lookupEvictKey(db,key1)) == NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(h->big && zsetLength(h) == 4);

        /* hot => cold */
        zsetDel(zset1,f1);
        zsetDel(zset1,f2);
        zsetDel(zset1,f3);
        zsetDel(zset1,f4);
        zset1_ctx->hctx.new_meta = createObjectMeta(0);
        zset1_ctx->hctx.meta_len_delta = 4;
        *data = *(bigZSetSwapData*)zset1_data;
        bigZSetSwapOut((swapData*)data, zset1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 4);
        test_assert((e = lookupEvictKey(db,key1)) != NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) == NULL);

        /* cold => hot */
        zset1 = createZsetObject();
        zsetAdd(zset1,1.0,f1,ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1,2.0,f2,ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1,3.0,f3,ZADD_IN_NONE, &flags, &ns);
        zsetAdd(zset1,4.0,f4,ZADD_IN_NONE, &flags, &ns);
        data->hd.value = h;
        data->hd.meta = m;
        data->hd.evict = e;
        zset1_ctx->hctx.meta_len_delta = -4;
        bigZSetSwapIn((swapData*)data,zset1,zset1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL);
        test_assert((e = lookupEvictKey(db,key1)) == NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(h->big && zsetLength(h) == 4);
    }

    TEST("bigZSet - rdbLoad & rdbSave") {
        // server.swap_big_zset_threshold = 0;
        server.swap_big_zset_threshold = 0; 
        int err = 0;
		sds myzset_key = sdsnew("myzset");
		robj *myzset = createZsetObject();
        sds f1 = sdsnew("f1"), f2 = sdsnew("f2");
        sds rdbv1 = bigZSetEncodeSubval(10);
        sds rdbv2 = bigZSetEncodeSubval(20);
        int flags;
        double newscore;
        zsetAdd(myzset, 10, f1, ZADD_IN_NONE, &flags, &newscore);
        zsetAdd(myzset, 20, f2, ZADD_IN_NONE, &flags, &newscore);

        /* rdbLoad */
        rio sdsrdb;
        sds rawval = rocksEncodeValRdb(myzset);
        rioInitWithBuffer(&sdsrdb,sdsnewlen(rawval+1,sdslen(rawval)-1));
        rdbKeyData _keydata, *keydata = &_keydata;
        rdbKeyDataInitLoad(keydata,&sdsrdb,rawval[0],myzset_key);
        sds subkey, subraw;
        int cont;
        cont = bigzsetRdbLoad(keydata,&sdsrdb,&subkey,&subraw,&err);
        test_assert(cont == 1 && err == 0);
        cont = bigzsetRdbLoad(keydata,&sdsrdb,&subkey,&subraw,&err);
        test_assert(cont == 0 && err == 0);
        test_assert(keydata->loadctx.bigzset.meta->len == 2);
        test_assert(keydata->loadctx.bigzset.evict->type == OBJ_ZSET);

        sds coldraw,warmraw,hotraw;
        objectMeta *meta = createObjectMeta(2);

        decodeResult _decoded_fx, *decoded_fx = &_decoded_fx;
        decoded_fx->enc_type = ENC_TYPE_ZSET_SUB;
        decoded_fx->key = myzset_key;
        decoded_fx->rdbtype = rdbv2[0];
        decoded_fx->subkey = f2;
        decoded_fx->rdbraw = sdsnewlen(rdbv2+1, sdslen(rdbv2)-1);

        // /* save cold */
        rio rdbcold, rdbwarm, rdbhot;
        rioInitWithBuffer(&rdbcold,sdsempty());
        robj *evict = createObject(OBJ_ZSET,NULL);
        rdbKeyDataInitSaveBigZSet(keydata,NULL,evict,meta,-1,myzset_key);
        test_assert(rdbKeySaveStart(keydata,&rdbcold) == 0);
        test_assert(rdbKeySave(keydata,&rdbcold,decoded_fx) == 0);
        decoded_fx->subkey = f1, decoded_fx->rdbraw = sdsnewlen(rdbv1+1,sdslen(rdbv1)-1);
        test_assert(rdbKeySave(keydata,&rdbcold,decoded_fx) == 0);
        decoded_fx->key = myzset_key;
        coldraw = rdbcold.io.buffer.ptr;

        // /* save warm */
        rioInitWithBuffer(&rdbwarm,sdsempty());
        robj *value = createZsetObject();
        // hashTypeSet(value,f2,sdsnew("v2"),HASH_SET_COPY);
        zsetAdd(value, 20, f2, ZADD_IN_NONE, &flags, &newscore);
        meta->len = 1;
        rdbKeyDataInitSaveBigZSet(keydata,value,evict,meta,-1,myzset_key);
        test_assert(rdbKeySaveStart(keydata,&rdbwarm) == 0);
        test_assert(rdbKeySave(keydata,&rdbwarm,decoded_fx) == 0);
        warmraw = rdbwarm.io.buffer.ptr;

        // /* save hot */
        robj keyobj;
        rioInitWithBuffer(&rdbhot,sdsempty());
        initStaticStringObject(keyobj,myzset_key);
        test_assert(rdbSaveKeyValuePair(&rdbhot,&keyobj,myzset,-1) != -1);
        hotraw = rdbhot.io.buffer.ptr;

        test_assert(!sdscmp(hotraw,coldraw));
        test_assert(!sdscmp(hotraw,warmraw));
    }


    return error;
}

#endif