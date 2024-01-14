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

#include "ctrip_swap_rordb.h"
#include <dirent.h>
#include <sys/stat.h>

#define RORDB_SST_READ_BUF_LEN (16*1024)

static int rordbSaveSSTFile(rio *rdb, char* filepath) {
    FILE *fp = NULL;
    char *filename = NULL, *buffer = NULL;
    struct stat statbuf;
    size_t buflen = RORDB_SST_READ_BUF_LEN, readlen = 0, filesize, toread;

    filename = strrchr(filepath,'/');
    if (filename == NULL) {
        serverLog(LL_WARNING,
                "[rordb] filepath invalid, (%s) is not absolute path",
                filepath);
        goto werr;
    }
    filename++;

    if (rdbSaveType(rdb,RORDB_OPCODE_SST) == -1) goto werr;
    if (rdbSaveRawString(rdb,(unsigned char*)filename,strlen(filename)) == -1)
        goto werr;

    if (stat(filepath,&statbuf)) goto werr;
    filesize = statbuf.st_size;
    if (rdbSaveLen(rdb,filesize) == -1) goto werr;

    if ((fp = fopen(filepath,"r")) == NULL) goto werr;
    buffer = zmalloc(buflen);

    while (1) {
        toread = MIN(buflen,filesize-readlen);
        if (toread == 0) break;

        if (fread(buffer,toread,1,fp) <= 0) {
            serverLog(LL_WARNING, "[rordb] read sst file(%s) error: %s(%d)",
                    filepath,strerror(errno),errno);
            goto werr;
        }
        readlen += toread;

        if (rdbWriteRaw(rdb,buffer,toread) == -1) {
            serverLog(LL_WARNING, "[rordb] write sst to rdb error: %s(%d)",
                    strerror(errno),errno);
            goto werr;
        }
    }

    if (fp) fclose(fp);
    if (buffer) zfree(buffer);
    return C_OK;

werr:
    if (fp) fclose(fp);
    if (buffer) zfree(buffer);
    return C_ERR;
}

static int rordbSaveSSTFiles(rio *rdb, char* path) {
	DIR *dir;
	struct dirent *ent;
    int saved = 0, skipped = 0;

	if ((dir = opendir(path)) == NULL) goto werr;

	while ((ent = readdir(dir))) {
		char *filepath;
		size_t fplen;
		struct stat statbuf;

		if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, ".."))
			continue;

		fplen = strlen(path) + strlen(ent->d_name) + 2;
		filepath = zmalloc(fplen);

		snprintf(filepath, fplen, "%s/%s", path, ent->d_name);
		if (stat(filepath, &statbuf)) {
            serverLog(LL_WARNING,
                    "[rordb] stat file(%s) failed when saving sst: %s (%d)",
                    path, strerror(errno), errno);
            zfree(filepath);
            goto werr;
        }

        if (!S_ISREG(statbuf.st_mode)) {
            skipped++;
            continue;
        }

        if (rordbSaveSSTFile(rdb, filepath) != C_OK) {
            zfree(filepath);
            goto werr;
        } else {
            saved++;
            serverLog(LL_DEBUG, "[rordb] saved sst file: %s", filepath);
        }

		zfree(filepath);
	}

    serverLog(LL_NOTICE,
            "[rordb] save sst files in (%s) ok: saved %d, skipped %d file.",
            path, saved, skipped);

	if (dir) closedir(dir);
    return C_OK;

werr:
    serverLog(LL_WARNING,
            "[rordb] save sst files in (%s) err: saved %d, skipped %d file.",
            path, saved, skipped);

    if (dir) closedir(dir);
    return C_ERR;
}

int rordbSaveSST(rio *rdb) {
    if (rdbSaveType(rdb,RORDB_OPCODE_SWAP_VERSION) == -1) goto err;
    if (rdbSaveLen(rdb,server.swap_key_version) == -1) goto err;
    if (rordbSaveSSTFiles(rdb,server.rocks->checkpoint_dir) == -1) goto err;
    return C_OK;
err:
    return C_ERR;
}

static int rordbLoadSSTFile(rio *rdb, char* path) {
    sds filename = NULL;
    FILE *fp = NULL;
    char *filepath = NULL, *buffer = NULL;
    size_t buflen = RORDB_SST_READ_BUF_LEN, readlen = 0,
           filesize, toread, fplen;

    filename = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
    if (filename == NULL) {
        serverLog(LL_WARNING,"[rordb] load filename failed: %s(%d)",
                strerror(errno),errno);
        goto err;
    }

    if ((filesize = rdbLoadLen(rdb,NULL)) == RDB_LENERR) {
        serverLog(LL_WARNING,"[rordb] load filesize failed: %s(%d)",
                strerror(errno),errno);
        goto err;
    }

    fplen = strlen(path) + 2 + sdslen(filename);
    filepath = zmalloc(fplen);
    snprintf(filepath,fplen,"%s/%s",path,filename);

    if ((fp = fopen(filepath,"w")) == NULL) goto err;

    buffer = zmalloc(buflen);

    while (1) {
        toread = MIN(buflen,filesize-readlen);
        if (toread == 0) break;

        if (rioRead(rdb,buffer,toread) == 0) {
            serverLog(LL_WARNING, "[rordb] read sst from rdb error: %s(%d)",
                    strerror(errno),errno);
            goto err;
        }
        readlen += toread;

        if (fwrite(buffer,toread,1,fp) <= 0) {
            serverLog(LL_WARNING, "[rordb] write sst to file error: %s(%d)",
                    strerror(errno),errno);
            goto err;
        }
    }

    serverLog(LL_DEBUG, "[rordb] saved sst file(%s) ok.", filepath);

    if (filename) sdsfree(filename);
    if (fp) fclose(fp);
    if (filepath) zfree(filepath);
    if (buffer) zfree(buffer);
    return C_OK;

err:
    if (filename) sdsfree(filename);
    if (fp) fclose(fp);
    if (filepath) zfree(filepath);
    if (buffer) zfree(buffer);
    return C_ERR;
}

int rmdirRecursive(const char *path);
int rordbLoadSSTStart(rio *rdb) {
    UNUSED(rdb);
    if (rmdirRecursive(RORDB_CHECKPOINT_DIR)) {
        serverLog(LL_WARNING, "[rordb] cleanup rordb checkpoint dir failed.");
        return C_ERR;
    } else {
        serverLog(LL_WARNING, "[rordb] cleanup rordb checkpoint dir ok.");
        return C_OK;
    }
}

int rordbLoadSSTType(rio *rdb, int type) {
    serverAssert(rordbOpcodeIsSSTType(type));
    if (type == RORDB_OPCODE_SWAP_VERSION) {
        uint64_t version;
        if ((version = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return C_ERR;
        swapSetVersion(version);
    } else {
        if (rordbLoadSSTFile(rdb,RORDB_CHECKPOINT_DIR) == -1) return C_ERR;
    }
    return C_OK;
}

int rordbLoadSSTFinished(rio *rdb) {
    UNUSED(rdb);
    serverLog(LL_NOTICE, "[rordb] restoring from checkpoint(%s).", RORDB_CHECKPOINT_DIR);
    if (rocksRestore(RORDB_CHECKPOINT_DIR)) {
        serverLog(LL_WARNING, "[rordb] restore from checkpoint(%s) failed.", RORDB_CHECKPOINT_DIR);
        return C_ERR;
    } else {
        serverLog(LL_NOTICE, "[rordb] restore from checkpoint(%s) failed.", RORDB_CHECKPOINT_DIR);
        return C_OK;
    }
}

#define RORRDB_CUCKOO_FILTER_FORMAT_V1 1

static int rordbSaveCuckooFilter(rio *rdb, cuckooFilter *cuckoo_filter) {
    if (rdbSaveType(rdb,RORDB_OPCODE_CUCOO_FILTER) == -1) goto err;
    if (rdbSaveLen(rdb,RORRDB_CUCKOO_FILTER_FORMAT_V1) == -1) goto err;
    if (rdbSaveLen(rdb,cuckoo_filter->bits_per_tag) == -1) goto err;
    if (rdbSaveLen(rdb,cuckoo_filter->ntables) == -1) goto err;

    for (int i = 0; i < cuckoo_filter->ntables; i++) {
        cuckooTable *table = cuckoo_filter->tables+i;
        size_t data_bytes = table->bytes_per_bucket * table->nbuckets;

        if (rdbSaveLen(rdb,table->bits_per_tag) == -1) goto err;
        if (rdbSaveLen(rdb,table->bytes_per_bucket) == -1) goto err;
        if (rdbSaveLen(rdb,table->nbuckets) == -1) goto err;

        if (rdbSaveLen(rdb,table->victim.used) == -1) goto err;
        if (rdbSaveLen(rdb,table->victim.tag) == -1) goto err;
        if (rdbSaveLen(rdb,table->victim.index) == -1) goto err;

        if (rdbSaveLen(rdb,table->ntags) == -1) goto err;
        if (rdbWriteRaw(rdb,table->data,data_bytes) == -1) goto err;
    }
    return C_OK;

err:
    return C_ERR;
}

static cuckooFilter *rordbLoadCuckooFilter(rio *rdb) {
    uint64_t len;
    cuckooFilter *cuckoo_filter = zcalloc(sizeof(cuckooFilter));

    if (rdbLoadLen(rdb,NULL) != RORRDB_CUCKOO_FILTER_FORMAT_V1) goto err;
    /* V1 is bound to use dictGenHashFunction */
    cuckoo_filter->hash_fn = dictGenHashFunction;
    if ((cuckoo_filter->bits_per_tag = rdbLoadLen(rdb,NULL)) == -1) goto err;
    if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;
    cuckoo_filter->ntables = len;
    cuckoo_filter->tables = zcalloc(cuckoo_filter->ntables*sizeof(cuckooTable));

    for (int i = 0; i < cuckoo_filter->ntables; i++) {
        cuckooTable *table = cuckoo_filter->tables+i;
        size_t data_bytes;

        if ((table->bits_per_tag = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;
        if ((table->bytes_per_bucket = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;
        if ((table->nbuckets = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;

        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;
        table->victim.used = len;
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;
        table->victim.tag = len;
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;
        table->victim.index = len;

        if ((table->ntags = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;

        data_bytes = table->bytes_per_bucket*table->nbuckets;
        table->data = zcalloc(data_bytes);
        if (rioRead(rdb,table->data,data_bytes) == 0) goto err;
    }

    return cuckoo_filter;
err:
    if (cuckoo_filter) cuckooFilterFree(cuckoo_filter);
    return NULL;
}

static int rdbSaveObjectMeta(rio *rdb, robj *key, objectMeta *object_meta) {
    int opcode = rordbOpcodeFromObjectType(object_meta->object_type);
    sds extend = objectMetaEncode(object_meta);
    if (opcode < 0 || extend == NULL) goto err;
    rdbSaveType(rdb,opcode);
    if (rdbSaveStringObject(rdb,key) == -1) goto err;
    if (rdbSaveLen(rdb,object_meta->version) == -1) goto err;
    if (rdbSaveRawString(rdb,(unsigned char *)extend,sdslen(extend)) == -1) goto err;
    if (extend) sdsfree(extend);
    return C_OK;
err:
    if (extend) sdsfree(extend);
    return C_ERR;
}

static int rordbSaveDbObjectMeta(rio *rdb, redisDb *db) {
    dictEntry *de;
    dictIterator *di = NULL;

    di = dictGetSafeIterator(db->meta);
    while((de = dictNext(di)) != NULL) {
        robj key;
        sds keystr = dictGetKey(de);
        objectMeta *om = dictGetVal(de);
        initStaticStringObject(key,keystr);
        if (rdbSaveObjectMeta(rdb,&key,om) == -1) goto err;
    }
    if (di) dictReleaseIterator(di);
    return C_OK;

err:
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

int rordbLoadObjectMeta(rio *rdb, int object_type, OUT sds *pkey,
        OUT objectMeta **pobject_meta) {
    uint64_t version;
    sds key = NULL, extend = NULL;
    objectMeta *object_meta = NULL;

    key = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
    if (key == NULL) {
        serverLog(LL_WARNING, "[rordb] meta key load failed.");
        goto err;
    }
    if ((version = rdbLoadLen(rdb,NULL)) == RDB_LENERR) goto err;

    extend = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL);
    if (extend == NULL) {
        serverLog(LL_WARNING, "[rordb] meta extend load failed.");
        goto err;
    }

    if (buildObjectMeta(object_type,version,extend,sdslen(extend),
                &object_meta) == -1) {
        goto err;
    }

    if (pkey) {
        *pkey = key;
    } else {
        sdsfree(key);
    }

    if (pobject_meta) {
        *pobject_meta = object_meta;
    } else {
        freeObjectMeta(object_meta);
    }

    if (extend) sdsfree(extend);
    return C_OK;

err:
    if (extend) sdsfree(extend);
    if (key) sdsfree(key);
    if (pkey) *pkey = NULL;
    if (pobject_meta) *pobject_meta = NULL;
    return C_ERR;
}

int rordbLoadDbType(rio *rdb, redisDb *db, int type) {
    serverAssert(rordbOpcodeIsDbType(type));
    if (type == RORDB_OPCODE_COLD_KEY_NUM) {
        uint64_t cold_keys;
        if ((cold_keys = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return C_ERR;
        db->cold_keys = cold_keys;
    } else if(type == RORDB_OPCODE_CUCOO_FILTER) {
        cuckooFilter *cuckoo_filter = rordbLoadCuckooFilter(rdb);
        if (cuckoo_filter == NULL) return C_ERR;
        if (!server.swap_cuckoo_filter_enabled) {
            serverLog(LL_WARNING,
                    "[rordb] loading cuckoo filter while it is disabled");
            cuckooFilterFree(cuckoo_filter);
            return C_ERR;
        }
        if (db->cold_filter->filter) {
            cuckooFilterFree(db->cold_filter->filter);
        }
        db->cold_filter->filter = cuckoo_filter;
    } else {
        robj key;
        sds keyptr = NULL;
        objectMeta *object_meta;
        int object_type = rordbObjectTypeFromOpcode(type);
        if (rordbLoadObjectMeta(rdb,object_type,&keyptr,&object_meta) == -1)
            return C_ERR;
        initStaticStringObject(key,keyptr);
        dbAddMeta(db,&key,object_meta);
        sdsfree(keyptr);
    }
    return C_OK;
}

int rordbSaveDbRio(rio *rdb, redisDb *db) {
    if (rdbSaveType(rdb,RORDB_OPCODE_COLD_KEY_NUM) == -1) goto err;
    if (rdbSaveLen(rdb,db->cold_keys) == -1) goto err;
    if (db->cold_filter->filter) {
        if (rordbSaveCuckooFilter(rdb,db->cold_filter->filter) == -1) goto err;
    }
    if (rordbSaveDbObjectMeta(rdb,db) == -1) goto err;
    return C_OK;
err:
    return C_ERR;
}

ssize_t rdbSaveAuxFieldStrStr(rio *rdb, char *key, char *val);
int rordbSaveAuxFields(rio *rdb) {
    return rdbSaveAuxFieldStrStr(rdb,RORDB_AUX,RORDB_VERSION);
}

int rordbLoadAuxFields(robj *key, robj *val) {
    UNUSED(val);
    if (!strcasecmp(key->ptr, RORDB_AUX)) {
        if (strcasecmp(val->ptr, RORDB_VERSION)) {
            serverLog(LL_WARNING, "[rordb] unexpected rordb version: %s",
                    (sds)val->ptr);
        }
        return 1;
    } else {
        return 0;
    }
}

#ifdef REDIS_TEST

#define TMP_PATH_MAX 64

typedef struct segment {
  int type;
  long index;
  long len;
} segment;

typedef struct listMeta {
    long len; /* total len of segments */
    long num; /* num of segments */
    segment *segments; /* segments of rockslist or memlist */
    long capacity; /* capacity of segments */
} listMeta;

struct listMeta *listMetaCreate();
int listMetaAppendSegment(struct listMeta *list_meta, int type, long index, long len);
void listMetaFree(struct listMeta *list_meta);

void initServerConfig(void);
int swapRordbTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;

    TEST("rordb: init") {
        initServerConfig();
        ACLInit();
        server.hz = 10;
        initTestRedisServer();
    }

    TEST("rordb: save & load sst") {
        rio _rdb, *rdb = &_rdb;
        mstime_t identity = mstime();
        char identity_dir[TMP_PATH_MAX], checkpoint_dir[TMP_PATH_MAX],
        load_dir[TMP_PATH_MAX], hello_filepath[TMP_PATH_MAX], foo_filepath[TMP_PATH_MAX];
        FILE *hello_file, *foo_file;
        char read_buffer[16];
        size_t read_len;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"

        snprintf(identity_dir,sizeof(identity_dir),"/tmp/%lld",identity);
        snprintf(checkpoint_dir,sizeof(checkpoint_dir), "/tmp/%lld/rordb-checkpoint", identity);
        snprintf(load_dir,sizeof(load_dir), "/tmp/%lld/rordb-load", identity);

        mkdir(identity_dir, 0755), mkdir(checkpoint_dir, 0755), mkdir(load_dir, 0755);

        snprintf(hello_filepath,sizeof(hello_filepath),"%s/hello.sst", checkpoint_dir);
        snprintf(foo_filepath,sizeof(foo_filepath),"%s/foo.sst", checkpoint_dir);

        hello_file = fopen(hello_filepath,"w");
        fwrite("world",5,1,hello_file);
        fclose(hello_file);
        foo_file = fopen(foo_filepath,"w");
        fwrite("bar",3,1,foo_file);
        fclose(foo_file);

        rioInitWithBuffer(rdb,sdsempty());
        test_assert(rordbSaveSSTFiles(rdb,checkpoint_dir) == C_OK);

        rioInitWithBuffer(rdb,rdb->io.buffer.ptr);
        test_assert(rdbLoadType(rdb) == RORDB_OPCODE_SST);
        test_assert(rordbLoadSSTFile(rdb,load_dir) == 0);
        test_assert(rdbLoadType(rdb) == RORDB_OPCODE_SST);
        test_assert(rordbLoadSSTFile(rdb,load_dir) == 0);

        snprintf(hello_filepath,sizeof(hello_filepath),"%s/hello.sst", load_dir);
        snprintf(foo_filepath,sizeof(foo_filepath),"%s/foo.sst", load_dir);

#pragma GCC diagnostic pop

        hello_file = fopen(hello_filepath,"r");
        read_len = fread(read_buffer,1,sizeof(read_buffer),hello_file);
        test_assert(read_len == 5);
        test_assert(memcmp(read_buffer,"world",read_len) == 0);
        fclose(hello_file);

        foo_file = fopen(foo_filepath,"r");
        read_len = fread(read_buffer,1,sizeof(read_buffer),foo_file);
        test_assert(read_len == 3);
        test_assert(memcmp(read_buffer,"bar",read_len) == 0);
        fclose(foo_file);
        sdsfree(rdb->io.buffer.ptr);
    }

    TEST("rordb: save & load cuckoo filter") {
        rio _rdb, *rdb = &_rdb;
        cuckooFilter *origin, *loaded;

        origin = cuckooFilterNew(dictGenHashFunction,CUCKOO_FILTER_BITS_PER_TAG_8,16);
        cuckooFilterInsert(origin,"hello",5);
        cuckooFilterInsert(origin,"foo",3);

        rioInitWithBuffer(rdb,sdsempty());
        test_assert(rordbSaveCuckooFilter(rdb,origin) == C_OK);

        rioInitWithBuffer(rdb,rdb->io.buffer.ptr);
        test_assert(rdbLoadType(rdb) == RORDB_OPCODE_CUCOO_FILTER);
        test_assert((loaded = rordbLoadCuckooFilter(rdb)) != NULL);
        test_assert(cuckooFilterContains(loaded,"hello",5) == CUCKOO_OK);
        test_assert(cuckooFilterContains(loaded,"world",5) == CUCKOO_ERR);
        test_assert(cuckooFilterContains(loaded,"foo",3) == CUCKOO_OK);
        test_assert(cuckooFilterContains(loaded,"bar",3) == CUCKOO_ERR);

        cuckooFilterFree(origin);
        cuckooFilterFree(loaded);
        sdsfree(rdb->io.buffer.ptr);
    }

    TEST("rordb: save & load object meta") {
        redisDb *db1 = server.db+1, *db2 = server.db+2;
        robj *key1 = createStringObject("key1",4), *key2 = createStringObject("key2",4),
             *key3 = createStringObject("key3",4), *key4 = createStringObject("key4",4);
        struct listMeta *lm1 = listMetaCreate();
        listMetaAppendSegment(lm1,SEGMENT_TYPE_HOT,100,5);
        listMetaAppendSegment(lm1,SEGMENT_TYPE_COLD,105,5);
        listMetaAppendSegment(lm1,SEGMENT_TYPE_HOT,110,5);
        objectMeta *om1 = createHashObjectMeta(1,1),
                   *om2 = createSetObjectMeta(2,2),
                   *om3 = createZsetObjectMeta(3,3),
                   *om4 = createListObjectMeta(4,lm1);
        test_assert(dictSize(db1->dict) == 0);
        test_assert(dictSize(db2->dict) == 0);
        dbAdd(db1, key1, shared.redacted), dbAdd(db1, key2, shared.redacted);
        dbAdd(db1, key3, shared.redacted), dbAdd(db1, key4, shared.redacted);
        dbAddMeta(db1, key1, om1), dbAddMeta(db1, key2, om2);
        dbAddMeta(db1, key3, om3), dbAddMeta(db1, key4, om4);
        dbAdd(db2, key1, shared.redacted), dbAdd(db2, key2, shared.redacted);
        dbAdd(db2, key3, shared.redacted), dbAdd(db2, key4, shared.redacted);

        rio _rdb, *rdb = &_rdb;
        rioInitWithBuffer(rdb,sdsempty());
        test_assert(rordbSaveDbObjectMeta(rdb,db1) == C_OK);

        int i, type;
        objectMeta *om;
        struct listMeta *lm;

        rioInitWithBuffer(rdb,rdb->io.buffer.ptr);
        for (i = 0; i < 4; i++) {
            type = rdbLoadType(rdb);
            test_assert(type >= RORDB_OPCODE_HASH && type <= RORDB_OPCODE_BITMAP);
            test_assert(rordbLoadDbType(rdb,db2,type) == C_OK);
        }

        test_assert((om = lookupMeta(db2,key1)) && om->object_type == OBJ_HASH && om->len == 1);
        test_assert((om = lookupMeta(db2,key2)) && om->object_type == OBJ_SET && om->len == 2);
        test_assert((om = lookupMeta(db2,key3)) && om->object_type == OBJ_ZSET && om->len == 3);
        test_assert((om = lookupMeta(db2,key4)) && om->object_type == OBJ_LIST);
        lm = objectMetaGetPtr(om);
        test_assert(lm->len = 15 && lm->num == 3);

        dbDelete(db1,key1), dbDelete(db1,key2), dbDelete(db1,key3), dbDelete(db1,key4);
        dbDelete(db2,key1), dbDelete(db2,key2), dbDelete(db2,key3), dbDelete(db2,key4);
        decrRefCount(key1), decrRefCount(key2), decrRefCount(key3), decrRefCount(key4);
        sdsfree(rdb->io.buffer.ptr);
    }

    return error;
}

#endif

