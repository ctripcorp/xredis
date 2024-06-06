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

#ifndef __CTRIP_SWAP_RORDB_H__
#define __CTRIP_SWAP_RORDB_H__

#include "server.h"

#define RORDB_AUX                     "rordb"
#define RORDB_VERSION                 "00001"

#define RORDB_OPCODE_BASE             128
#define RORDB_OPCODE(i)               (RORDB_OPCODE_BASE+i)

#define RORDB_OBJECT_FLAGS_DIRTY_META (1<<0)
#define RORDB_OBJECT_FLAGS_DIRTY_DATA (1<<1)
#define RORDB_OBJECT_FLAGS_PERSISTENT (1<<2)
#define RORDB_OBJECT_FLAGS_PERSIST_KEEP (1<<3)

#define RORDB_OPCODE_OBJECT_FLAGS     RORDB_OPCODE(0)

/* ror.sst opcodes */
#define RORDB_OPCODE_SWAP_VERSION     RORDB_OPCODE(1)
#define RORDB_OPCODE_SST              RORDB_OPCODE(2)
/* ror.db opcodes */
#define RORDB_OPCODE_COLD_KEY_NUM     RORDB_OPCODE(3)
#define RORDB_OPCODE_CUCKOO_FILTER    RORDB_OPCODE(4)
/* String doesn't have object meta. */
#define RORDB_OPCODE_HASH             RORDB_OPCODE(5)
#define RORDB_OPCODE_SET              RORDB_OPCODE(6)
#define RORDB_OPCODE_ZSET             RORDB_OPCODE(7)
#define RORDB_OPCODE_LIST             RORDB_OPCODE(8)
#define RORDB_OPCODE_BITMAP           RORDB_OPCODE(9)
/* ror opcode must lt limit */
#define RORDB_OPCODE_LIMIT            RORDB_OPCODE(10)

#define RORDB_CHECKPOINT_DIR          "rordb_checkpoint"

static inline int rordbOpcodeIsValid(int type) {
  return type >= RORDB_OPCODE_BASE && type < RORDB_OPCODE_LIMIT;
}

static inline int rordbOpcodeIsObjectFlags(int type) {
  return type == RORDB_OPCODE_OBJECT_FLAGS;
}

static inline int rordbOpcodeIsSSTType(int type) {
  return type == RORDB_OPCODE_SWAP_VERSION || type == RORDB_OPCODE_SST;
}

static inline int rordbOpcodeIsDbType(int type) {
  return type >= RORDB_OPCODE_COLD_KEY_NUM && type < RORDB_OPCODE_LIMIT;
}

static inline int rordbOpcodeFromSwapType(int swap_type) {
  switch (swap_type) {
  case SWAP_TYPE_HASH:
    return RORDB_OPCODE_HASH;
  case SWAP_TYPE_SET:
    return RORDB_OPCODE_SET;
  case SWAP_TYPE_ZSET:
    return RORDB_OPCODE_ZSET;
  case SWAP_TYPE_LIST:
    return RORDB_OPCODE_LIST;
  case SWAP_TYPE_BITMAP:
    return RORDB_OPCODE_BITMAP;
  default:
    serverPanic("unexpected swap_type.");
    return -1;
  }
}

static inline int rordbSwapTypeFromOpcode(int type) {
  switch (type) {
  case RORDB_OPCODE_HASH:
    return SWAP_TYPE_HASH;
  case RORDB_OPCODE_SET:
    return SWAP_TYPE_SET;
  case RORDB_OPCODE_ZSET:
    return SWAP_TYPE_ZSET;
  case RORDB_OPCODE_LIST:
    return SWAP_TYPE_LIST;
  case RORDB_OPCODE_BITMAP:
    return SWAP_TYPE_BITMAP;
  default:
    serverPanic("unexpected type.");
    return -1;
  }
}

int rordbSetObjectFlags(robj *val, long long object_flags);
int rordbSaveObjectFlags(rio *rdb, robj *val);
int rordbSaveAuxFields(rio *rdb);
int rordbLoadAuxFields(robj *key, robj *val);
int rordbSaveSST(rio *rdb);
int rordbSaveDbRio(rio *rdb, redisDb *db);
int rordbLoadSSTStart(rio *rdb);
int rordbLoadSSTType(rio *rdb, int type);
int rordbLoadSSTFinished(rio *rdb);
int rordbLoadDbType(rio *rdb, redisDb *db, int type);

#endif
