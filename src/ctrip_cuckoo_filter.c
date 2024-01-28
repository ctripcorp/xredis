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
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "ctrip_cuckoo_filter.h"
#include "ctrip_cuckoo_malloc.h"

static int isPowOf2(uint64_t n) { return (n & (n - 1)) == 0 && n != 0; }

static inline uint64_t upperPowOf2(uint64_t n) {
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	n |= n >> 32;
	n++;
	return n;
}

static int isLittleEndian() { int n = 1; return (*(char *)&n == 1); }

static inline int cuckooGetBitsPerTag(int bits_per_tag_type) {
    assert(bits_per_tag_type < CUCKOO_FILTER_BITS_PER_TAG_TYPES);
    int bits_per_tag_array[CUCKOO_FILTER_BITS_PER_TAG_TYPES] = {8,12,16,32};
    return bits_per_tag_array[bits_per_tag_type];
}

void cuckooTableInit(cuckooTable *table, int bits_per_tag, size_t nbuckets) {
    size_t bytes_per_bucket = (bits_per_tag*CUCKOO_FILTER_TAGS_PER_BUCKET+7)>>3;
    assert(isPowOf2(nbuckets));
    table->bits_per_tag = bits_per_tag;
    table->bytes_per_bucket = bytes_per_bucket;
    table->nbuckets = nbuckets;
    table->victim.used = 0;
    table->victim.index = 0;
    table->victim.tag = 0;
    table->ntags = 0;
    table->data = cuckoo_calloc(nbuckets*(bytes_per_bucket));
}

void cuckooTableDeinit(cuckooTable *table) {
    if (table->data) {
        cuckoo_free(table->data);
        table->data = NULL;
    }
}

static inline void cuckooTableIndexTag(cuckooTable *table, uint64_t hv,
        size_t *i1, uint32_t *tag) {
    *i1 = (hv >> 32) & (table->nbuckets - 1);
    *tag = (hv & 0xFFFFFFFF) & ((1ULL << table->bits_per_tag) - 1);
    *tag += *tag == 0;
}

static inline size_t cuckooTableAltIndex(cuckooTable *table, size_t i1,
        uint32_t tag) {
    return (i1 ^ ((size_t)tag * 0x5bd1e995)) & (table->nbuckets - 1);
}

/* works only for little-endian */
static inline
uint32_t cuckooTableReadTag(cuckooTable *table, size_t i, size_t j) {
    assert(i < table->nbuckets && j < CUCKOO_FILTER_TAGS_PER_BUCKET);
    uint32_t tag = 0;
    uint8_t *p = table->data + i*table->bytes_per_bucket;
    if (table->bits_per_tag == 8) {
        tag = ((uint8_t*)p)[j];
    } else if (table->bits_per_tag == 12) {
        p += j + (j >> 1);
        tag = (*((uint16_t *)p) >> ((j & 1) << 2)) & 0xFFF;
    } else if (table->bits_per_tag == 16) {
        tag = ((uint16_t*)p)[j];
    } else if (table->bits_per_tag == 32) {
        tag = ((uint32_t*)p)[j];
    }
    return tag;
}

/* works only for little-endian */
static inline
void cuckooTableWriteTag(cuckooTable *table, size_t i, size_t j, int32_t tag) {
    uint8_t *p = table->data + i*table->bytes_per_bucket;
    if (table->bits_per_tag == 8) {
        ((uint8_t*)p)[j] = tag;
    } else if (table->bits_per_tag == 12) {
      p += (j + (j >> 1));
      if ((j & 1) == 0) {
        ((uint16_t *)p)[0] &= 0xf000;
        ((uint16_t *)p)[0] |= tag;
      } else {
        ((uint16_t *)p)[0] &= 0x000f;
        ((uint16_t *)p)[0] |= (tag << 4);
      }
    } else if (table->bits_per_tag == 16) {
        ((uint16_t*)p)[j] = tag;
    } else if (table->bits_per_tag == 32) {
        ((uint32_t*)p)[j] = tag;
    }
}

int cuckooTableInsertNoKick(cuckooTable *table, uint64_t hv) {
    size_t i1, i2;
    uint32_t tag;

    cuckooTableIndexTag(table,hv,&i1,&tag);
    i2 = cuckooTableAltIndex(table,i1,tag);

    for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
        if (cuckooTableReadTag(table,i1,j) == CUCKOO_TAG_NULL) {
            cuckooTableWriteTag(table,i1,j,tag);
            table->ntags++;
            return CUCKOO_OK;
        }
    }

    for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
        if (cuckooTableReadTag(table,i2,j) == CUCKOO_TAG_NULL) {
            cuckooTableWriteTag(table,i2,j,tag);
            table->ntags++;
            return CUCKOO_OK;
        }
    }

    return CUCKOO_ERR;
}

static int cuckooTableInsertKickOutIndexTag(cuckooTable *table, size_t i,
        uint32_t tag) {
    uint32_t otag;

    if (table->victim.used) return CUCKOO_ERR;

    for (int loop = 0; loop < CUCKOO_FILTER_MAX_ITERATION; loop++) {
        for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
            if (cuckooTableReadTag(table,i,j) == CUCKOO_TAG_NULL) {
                cuckooTableWriteTag(table,i,j,tag);
                table->ntags++;
                return CUCKOO_OK;
            }
        }

        size_t kj = rand() & (CUCKOO_FILTER_TAGS_PER_BUCKET-1);
        otag = cuckooTableReadTag(table,i,kj);
        cuckooTableWriteTag(table,i,kj,tag);
        tag = otag;
        i = cuckooTableAltIndex(table,i,tag);
    }

    table->victim.used = 1;
    table->victim.index = i;
    table->victim.tag = tag;
    table->ntags++;

    return CUCKOO_OK;
}

int cuckooTableInsertKickOut(cuckooTable *table, uint64_t hv) {
    size_t i;
    uint32_t tag;
    cuckooTableIndexTag(table,hv,&i,&tag);
    return cuckooTableInsertKickOutIndexTag(table,i,tag);
}

int cuckooTableContains(cuckooTable *table, uint64_t hv) {
    size_t i1, i2;
    uint32_t tag;

    cuckooTableIndexTag(table,hv,&i1,&tag);
    i2 = cuckooTableAltIndex(table,i1,tag);

    if (table->bits_per_tag == CUCKOO_FILTER_BITS_PER_TAG_8) {
        return CUCKOO_ERR;
    }

    for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
        if (cuckooTableReadTag(table,i1,j) == tag) {
            return CUCKOO_OK;
        }
    }

    for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
        if (cuckooTableReadTag(table,i2,j) == tag) {
            return CUCKOO_OK;
        }
    }

    if (table->victim.used && table->victim.tag == tag &&
            (i1 == table->victim.index || i2 == table->victim.index)) {
        return CUCKOO_OK;
    }

    return CUCKOO_ERR;
}

static inline void cuckooTableTryEliminateVictimCache(cuckooTable *table) {
    if (table->victim.used) {
        table->victim.used = 0;
        table->ntags--;
        cuckooTableInsertKickOutIndexTag(table,table->victim.index,
                table->victim.tag);
    }
}

int cuckooTableDelete(cuckooTable *table, uint64_t hv) {
    size_t i1, i2;
    uint32_t tag;

    cuckooTableIndexTag(table,hv,&i1,&tag);
    i2 = cuckooTableAltIndex(table,i1,tag);

    for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
        if (cuckooTableReadTag(table,i1,j) == tag) {
            cuckooTableWriteTag(table,i1,j,CUCKOO_TAG_NULL);
            table->ntags--;
            cuckooTableTryEliminateVictimCache(table);
            return CUCKOO_OK;
        }
    }

    for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
        if (cuckooTableReadTag(table,i2,j) == tag) {
            cuckooTableWriteTag(table,i2,j,CUCKOO_TAG_NULL);
            table->ntags--;
            cuckooTableTryEliminateVictimCache(table);
            return CUCKOO_OK;
        }
    }

    if (table->victim.used && table->victim.tag == tag &&
            (i1 == table->victim.index || i2 == table->victim.index)) {
        table->victim.used = 0;
        table->ntags--;
        return CUCKOO_OK;
    }

    return CUCKOO_ERR;
}


static inline uint64_t cuckooFilterGenerateHash(cuckooFilter *filter,
        const char *key, size_t klen) {
    return filter->hash_fn(key,klen);
}

static inline cuckooTable *cuckooFilterCurrentTable(cuckooFilter *filter) {
    return &filter->tables[filter->ntables-1];
}

static size_t cuckooEstimateBuckets(size_t estimated_keys) {
    size_t nbuckets = upperPowOf2(estimated_keys)/CUCKOO_FILTER_TAGS_PER_BUCKET;
    return nbuckets < CUCKOO_FILTER_TABLE_MIN_BUCKETS ? CUCKOO_FILTER_TABLE_MIN_BUCKETS : nbuckets;
}

cuckooFilter *cuckooFilterNew(cuckoo_hash_fn hash_fn, int bits_per_tag_type,
        size_t estimated_keys) {
    assert(isLittleEndian());
    size_t nbuckets = cuckooEstimateBuckets(estimated_keys);
    cuckooFilter *filter = cuckoo_malloc(sizeof(cuckooFilter));
    filter->hash_fn = hash_fn;
    filter->bits_per_tag = cuckooGetBitsPerTag(bits_per_tag_type);
    filter->ntables = 1;
    filter->tables = cuckoo_malloc(sizeof(cuckooTable));
    cuckooTableInit(filter->tables,filter->bits_per_tag,nbuckets);
    return filter;
}

void cuckooFilterFree(cuckooFilter *filter) {
    if (filter == NULL) return;
    for (int i = 0; i < filter->ntables; i++) {
        cuckooTableDeinit(filter->tables+i);
    }
    cuckoo_free(filter->tables);
    cuckoo_free(filter);
}

static int cuckooFilterExpand(cuckooFilter *filter) {
    cuckooTable *table = cuckooFilterCurrentTable(filter);
    size_t nbuckets = table->nbuckets*CUCKOO_FILTER_BUCKETS_EXPANSION;
    /* NOTE that we never expand more then 4 tables. */
    if (filter->ntables >= CUCKOO_FILTER_MAX_TABLES)
        return CUCKOO_ERR;
    /* i1 uses higher 32bit of hv, can't index more that 2^32 */
    if (nbuckets > UINT32_MAX) nbuckets = UINT32_MAX;
    filter->ntables++;
    filter->tables = cuckoo_realloc(filter->tables,
            filter->ntables*sizeof(cuckooTable));
    table = cuckooFilterCurrentTable(filter);
    cuckooTableInit(table,filter->bits_per_tag,nbuckets);
    return 0;
}

int cuckooFilterInsert(cuckooFilter *filter, const char *key, size_t klen) {
    cuckooTable *table;
    uint64_t hv = cuckooFilterGenerateHash(filter,key,klen);

    /* Try insert without kickout for all tables. */
    for (int i = filter->ntables-1; i >= 0; i--) {
        table = filter->tables+i;
        if (cuckooTableInsertNoKick(table,hv) == CUCKOO_OK)
            return CUCKOO_OK;
    }

    /* Try insert with kickout for current table. */
    table = cuckooFilterCurrentTable(filter);
    if (cuckooTableInsertKickOut(table,hv) == CUCKOO_OK)
        return CUCKOO_OK;

    /* Try expand and insert. */
    if (cuckooFilterExpand(filter) != CUCKOO_OK) {
        return CUCKOO_ERR;
    } else {
        table = cuckooFilterCurrentTable(filter);
        return cuckooTableInsertNoKick(table,hv);
    }
}

int cuckooFilterContains(cuckooFilter *filter, const char *key, size_t klen) {
    cuckooTable *table;
    uint64_t hv = cuckooFilterGenerateHash(filter,key,klen);

    for (int i = filter->ntables-1; i >= 0; i--) {
        table = filter->tables+i;
        if (cuckooTableContains(table,hv) == CUCKOO_OK)
            return CUCKOO_OK;
    }

    return CUCKOO_ERR;
}

int cuckooFilterDelete(cuckooFilter *filter, const char *key, size_t klen) {
    cuckooTable *table;
    uint64_t hv = cuckooFilterGenerateHash(filter,key,klen);

    for (int i = filter->ntables-1; i >= 0; i--) {
        table = filter->tables+i;
        if (cuckooTableDelete(table,hv) == CUCKOO_OK)
            return CUCKOO_OK;
    }

    return CUCKOO_ERR;
}

void cuckooFilterGetStat(cuckooFilter *filter, cuckooFilterStat *stat) {
    size_t total_slots = 0;
    memset(stat,0,sizeof(cuckooFilterStat));
    stat->ntables = filter->ntables;
    for (int i = 0; i < filter->ntables; i++) {
        cuckooTable *table = filter->tables+i;
        size_t slots = table->nbuckets*CUCKOO_FILTER_TAGS_PER_BUCKET;
        stat->ntags += table->ntags;
        stat->used_memory += table->bytes_per_bucket * table->nbuckets;
        stat->load_factors[i] = (double)table->ntags / slots;
        total_slots += slots;
    }
    stat->load_factor = (double)stat->ntags / total_slots;
}

size_t cuckooFilterUsedMemory(cuckooFilter *filter) {
    size_t used_memory = 0;
    for (int i = 0; i < filter->ntables; i++) {
        cuckooTable *table = filter->tables+i;
        used_memory += table->bytes_per_bucket * table->nbuckets;
    }
    return used_memory;
}

void cuckooFilterDump(cuckooFilter *filter) {
    serverLog(LL_NOTICE, "==== cuckoo filter(%p) ====", (void*)filter);
    serverLog(LL_NOTICE, "  bits_per_tag: %d", filter->bits_per_tag);
    serverLog(LL_NOTICE, "  ntables: %d", filter->ntables);
    for (int i = 0; i < filter->ntables; i++) {
        cuckooTable *table = filter->tables+i;
        serverLog(LL_NOTICE, "  table(%d):", i);
        serverLog(LL_NOTICE, "    bits_per_tag: %lu", table->bits_per_tag);
        serverLog(LL_NOTICE, "    bytes_per_bucket: %lu", table->bytes_per_bucket);
        serverLog(LL_NOTICE, "    nbuckets: %lu", table->nbuckets);
        serverLog(LL_NOTICE, "    victim: used=%d,tag=%u,index=%lu", table->victim.used, table->victim.tag, table->victim.index);
        serverLog(LL_NOTICE, "    ntags: %ld", table->ntags);
        serverLogHexDump(LL_NOTICE, "    data:",table->data,table->bytes_per_bucket*table->nbuckets);
    }
    serverLog(LL_NOTICE, "===========================");
}

#ifdef REDIS_TEST

#define KEYMAXLEN 16

int cuckooFilterTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

    TEST("cuckoo-filter: utility") {
        test_assert(isPowOf2(1) == 1);
        test_assert(isPowOf2(2) == 1);
        test_assert(isPowOf2(3) == 0);
        test_assert(isPowOf2(4) == 1);
        test_assert(isPowOf2(5) == 0);
        test_assert(isPowOf2(255) == 0);
        test_assert(isPowOf2(256) == 1);

        test_assert(upperPowOf2(1) == 1);
        test_assert(upperPowOf2(2) == 2);
        test_assert(upperPowOf2(3) == 4);
        test_assert(upperPowOf2(4) == 4);
        test_assert(upperPowOf2(5) == 8);
        test_assert(upperPowOf2(6) == 8);
    }

    TEST("cuckoo-filter: table") {
        cuckooTable table_, *table = &table_;
        int bits[4] = {8,12,16,32};
        int nbuckets = 16;

        uint32_t tags[nbuckets][4];
        for (int bi = 0; bi < 4; bi++) {
            int nbit = bits[bi];

            cuckooTableInit(table,nbit,nbuckets);

            for (int i = 0; i < nbuckets; i++) {
                for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
                    uint32_t tag = rand() & ((1<<nbit)-1);
                    tags[i][j] = tag;
                    cuckooTableWriteTag(table,i,j,tag);
                }
            }

            for (int i = 0; i < nbuckets; i++) {
                for (int j = 0; j < CUCKOO_FILTER_TAGS_PER_BUCKET; j++) {
                    test_assert(cuckooTableReadTag(table,i,j) == tags[i][j]);
                }
            }
            cuckooTableDeinit(table);
        }
    }

    TEST("cuckoo-filter: filter (unique keys) ") {
        cuckooFilter *filter;
        size_t ncases = 2048, nbuckets_base = 16;
        cuckooFilterStat stat_, *stat = &stat_;
        char key[KEYMAXLEN];

        for (int bt = 0; bt < CUCKOO_FILTER_BITS_PER_TAG_TYPES; bt++) {
            size_t expected_used_memory = cuckooGetBitsPerTag(bt)*
                CUCKOO_FILTER_TAGS_PER_BUCKET/8*nbuckets_base*(1+4+16+64);
            filter = cuckooFilterNew(cuckooGenHashFunction,bt,16);
            for (size_t i = 0; i < ncases; i++) {
                snprintf(key,KEYMAXLEN,"%08ld",i);
                test_assert(cuckooFilterInsert(filter,key,strlen(key)) == CUCKOO_OK);
            }

            cuckooFilterGetStat(filter,stat);
            test_assert(stat->ntables == 4);
            test_assert(stat->used_memory == expected_used_memory);
            test_assert(stat->ntags == ncases);

            for (size_t i = 0; i < ncases; i++) {
                snprintf(key,KEYMAXLEN,"%08ld",i);
                test_assert(cuckooFilterContains(filter,key,strlen(key)) == CUCKOO_OK);
            }

            for (size_t i = 0; i < ncases; i++) {
                snprintf(key,KEYMAXLEN,"%08ld",i);
                test_assert(cuckooFilterDelete(filter,key,strlen(key)) == CUCKOO_OK);
            }

            cuckooFilterGetStat(filter,stat);
            test_assert(stat->ntables == 4);
            test_assert(stat->used_memory == expected_used_memory);
            test_assert(stat->ntags == 0);

            for (size_t i = 0; i < ncases; i++) {
                snprintf(key,KEYMAXLEN,"%08ld",i);
                test_assert(cuckooFilterContains(filter,key,strlen(key)) == CUCKOO_ERR);
            }
            cuckooFilterFree(filter);
        }
    }

    TEST("cuckoo-filter: filter (duplicate keys) ") {
        cuckooFilter *filter;
        size_t ncases = 24, nbuckets_base = 16;
        cuckooFilterStat stat_, *stat = &stat_;
        char *key = "12345678", *nokey = "00000000";

        for (int bt = 0; bt < CUCKOO_FILTER_BITS_PER_TAG_TYPES; bt++) {
            size_t expected_used_memory = cuckooGetBitsPerTag(bt)*
                CUCKOO_FILTER_TAGS_PER_BUCKET/8*nbuckets_base*(1+4+16);
            filter = cuckooFilterNew(cuckooGenHashFunction,bt,16);

            for (size_t i = 0; i < ncases; i++) {
                test_assert(cuckooFilterInsert(filter,key,strlen(key)) == CUCKOO_OK);
            }
            cuckooFilterGetStat(filter,stat);
            test_assert(stat->ntables == 3);
            test_assert(stat->ntags == ncases);
            test_assert(stat->used_memory == expected_used_memory);

            test_assert(cuckooFilterContains(filter,key,strlen(key)) == CUCKOO_OK);
            test_assert(cuckooFilterContains(filter,nokey,strlen(nokey)) == CUCKOO_ERR);

            for (size_t i = 0; i < ncases; i++) {
                test_assert(cuckooFilterDelete(filter,nokey,strlen(nokey)) == CUCKOO_ERR);
            }
            cuckooFilterGetStat(filter,stat);
            test_assert(stat->ntables == 3);
            test_assert(stat->ntags == ncases);

            for (size_t i = 0; i < ncases; i++) {
                test_assert(cuckooFilterDelete(filter,key,strlen(key)) == CUCKOO_OK);
            }
            cuckooFilterGetStat(filter,stat);
            test_assert(stat->ntables == 3);
            test_assert(stat->ntags == 0);

            test_assert(cuckooFilterContains(filter,key,strlen(key)) == CUCKOO_ERR);
            cuckooFilterFree(filter);
        }
    }

    TEST("cuckoo-filter: bench") {
        size_t nkeys = 1000000;
        double fp_rate;
        cuckooFilter *filter;
        double fp_rate_max[] = {0.03, 0.003, 0.0003, 0.0001};

        for (int bt = 0; bt < CUCKOO_FILTER_BITS_PER_TAG_TYPES; bt++) {
            size_t false_positive = 0, total = 0;
            filter = cuckooFilterNew(cuckooGenHashFunction, bt, nkeys);

            for (size_t i = 0; i < nkeys; i++) {
                test_assert(cuckooFilterInsert(filter,(char*)&i,sizeof(size_t)) == CUCKOO_OK);
            }

            for (size_t i = 0; i < nkeys; i++) {
                test_assert(cuckooFilterContains(filter,(char*)&i,sizeof(size_t)) == CUCKOO_OK);
            }

            for (size_t i = nkeys; i < nkeys*2; i++) {
                if (cuckooFilterContains(filter,(char*)&i,sizeof(size_t)) == CUCKOO_OK) {
                    false_positive++;
                }
                total++;
            }

            fp_rate = (double)false_positive/total;
            test_assert(fp_rate < fp_rate_max[bt]);

            cuckooFilterStat stat_, *stat = &stat_;
            cuckooFilterGetStat(filter, stat);
            test_assert(filter->ntables == 1);
            printf("cucoo-filter(%d): fp_rate=%0.4f, ntables=%ld, ntags=%ld, load_factor[0] = %0.2f\n",
                    cuckooGetBitsPerTag(bt),fp_rate,stat->ntables,stat->ntags,stat->load_factors[0]);
            if (filter->ntables > 1) {
                printf(" load_factors[1]= %0.2f\n", stat->load_factors[1]);
            }

            cuckooFilterFree(filter);
        }
    }

    /* make CFLAGS="-DREDIS_TEST" ./src/redis-server test swap;
     * 500W QPS: [insert]: 11381663 [contains]: 11665840 */
    /*
    TEST("cuckoo-filter: perf") {
        size_t estimated_keys = 32*1024*1024, nkeys = 64*1024*1024;
        cuckooFilter *filter;

        filter = cuckooFilterNew(cuckooGenHashFunction, CUCKOO_FILTER_BITS_PER_TAG_8, estimated_keys);

        long long start = ustime();
        for (size_t i = 0; i < nkeys; i++) {
            test_assert(cuckooFilterInsert(filter,(char*)&i,sizeof(size_t)) == CUCKOO_OK);
        }
        printf("[insert]: %lld\n", ustime() - start);
        start = ustime();

        for (size_t i = 0; i < nkeys; i++) {
            test_assert(cuckooFilterContains(filter,(char*)&i,sizeof(size_t)) == CUCKOO_OK);
        }
        printf("[contains]: %lld\n", ustime() - start);
        start = ustime();


        cuckooFilterStat stat_, *stat = &stat_;
        cuckooFilterGetStat(filter, stat);

        printf("cucoo-filter(8): ntables=%ld, ntags=%ld, load_factor[0] = %0.2f\n",
                stat->ntables,stat->ntags,stat->load_factors[0]);
        if (filter->ntables > 1) {
            printf(" load_factors[1]= %0.2f\n", stat->load_factors[1]);
        }

        cuckooFilterFree(filter);
    } */

    return error;
}
#endif

