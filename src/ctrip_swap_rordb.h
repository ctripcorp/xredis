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
/* ror.sst opcodes */
#define RORDB_OPCODE_SWAP_VERSION     RORDB_OPCODE(0)
#define RORDB_OPCODE_SST              RORDB_OPCODE(1)
/* ror.db opcodes */
#define RORDB_OPCODE_COLD_KEY_NUM     RORDB_OPCODE(2)
#define RORDB_OPCODE_CUCKOO_FILTER    RORDB_OPCODE(3)
/* String doesn't have object meta. */
#define RORDB_OPCODE_HASH             RORDB_OPCODE(4)
#define RORDB_OPCODE_SET              RORDB_OPCODE(5)
#define RORDB_OPCODE_ZSET             RORDB_OPCODE(6)
#define RORDB_OPCODE_LIST             RORDB_OPCODE(7)
#define RORDB_OPCODE_BITMAP           RORDB_OPCODE(8)
/* ror opcode must lt limit */
#define RORDB_OPCODE_LIMIT            RORDB_OPCODE(9)

#define RORDB_CHECKPOINT_DIR          "rordb_checkpoint"

static inline int rordbOpcodeIsValid(int type) {
  return type >= RORDB_OPCODE_BASE && type < RORDB_OPCODE_LIMIT;
}

static inline int rordbOpcodeIsSSTType(int type) {
  return type == RORDB_OPCODE_SWAP_VERSION || type == RORDB_OPCODE_SST;
}

static inline int rordbOpcodeIsDbType(int type) {
  return type >= RORDB_OPCODE_COLD_KEY_NUM && type < RORDB_OPCODE_LIMIT;
}

static inline int rordbOpcodeFromObjectType(int object_type) {
  switch (object_type) {
  case OBJ_HASH:
    return RORDB_OPCODE_HASH;
  case OBJ_SET:
    return RORDB_OPCODE_SET;
  case OBJ_ZSET:
    return RORDB_OPCODE_ZSET;
  case OBJ_LIST:
    return RORDB_OPCODE_LIST;
  default:
    serverPanic("unexpected object_type.");
    return -1;
  }
}

static inline int rordbObjectTypeFromOpcode(int type) {
  switch (type) {
  case RORDB_OPCODE_HASH:
    return OBJ_HASH;
  case RORDB_OPCODE_SET:
    return OBJ_SET;
  case RORDB_OPCODE_ZSET:
    return OBJ_ZSET;
  case RORDB_OPCODE_LIST:
    return OBJ_LIST;
  default:
    serverPanic("unexpected type.");
    return -1;
  }
}

int rordbSaveAuxFields(rio *rdb);
int rordbLoadAuxFields(robj *key, robj *val);
int rordbSaveSST(rio *rdb);
int rordbSaveDbRio(rio *rdb, redisDb *db);
int rordbLoadSSTStart(rio *rdb);
int rordbLoadSSTType(rio *rdb, int type);
int rordbLoadSSTFinished(rio *rdb);
int rordbLoadDbType(rio *rdb, redisDb *db, int type);

#endif
