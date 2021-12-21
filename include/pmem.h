#ifndef PMEM_H_
#define PMEM_H_

#include "common.h"

#include <cstdio>

//#include <libpmemobj++/pool.hpp>
#include <libpmemobj.h>
#include <libpmempool.h>

struct DBRoot;
struct LogBase;
struct LogBlockBase;
struct RegionBase;
struct IndexBase;
struct BraidedSkipListBase;

extern PMEMobjpool* root_pop;
extern PMEMobjpool* vpop[kMaxNumShards][kMaxNumRegions];
extern PMEMobjpool* lpop[kMaxNumShards][kMaxNumRegions];
extern PMEMobjpool* ipop[kMaxNumShards][kMaxNumPmemLevels][kMaxNumRegions];
extern TOID(DBRoot) db_root;


TOID_DECLARE(char, 0);
POBJ_LAYOUT_BEGIN(BRDB);
POBJ_LAYOUT_TOID(BRDB, DBRoot);
POBJ_LAYOUT_TOID(BRDB, RegionBase);
POBJ_LAYOUT_TOID(BRDB, IndexBase);
POBJ_LAYOUT_TOID(BRDB, BraidedSkipListBase);
// Log
POBJ_LAYOUT_TOID(BRDB, LogBase);
POBJ_LAYOUT_TOID(BRDB, LogBlockBase);
POBJ_LAYOUT_END(BRDB);

struct DBRoot {
  int num_shards;
  TOID(RegionBase) regions[kMaxNumRegions];
};

struct RegionBase {
  TOID(LogBase) wal;
  TOID(IndexBase) index;
};

struct LogBase {
  TOID(LogBlockBase) head_block_base;
  TOID(LogBlockBase) npp;  // non-persistent pointer
};

struct LogBlockBase {
  uint64_t p;
  uint64_t npp;
  TOID(LogBlockBase) next;
  char data[1];
};

struct IndexBase {
  TOID(char) level[kMaxNumPmemLevels];
};

struct BraidedSkipListBase {
  TOID(char) head[kMaxNumRegions];
};

void init_pmem_pools(bool use_existing = true);

void init_root_pool(bool use_existing = true);

void init_value_pool(const int s, bool use_existing = true);

void init_log_pool(const int s, bool use_existing = true);

void init_index_pool(const int s, bool use_existing = true);

#endif  // PMEM_H_
