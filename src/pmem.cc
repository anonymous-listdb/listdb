#include "pmem.h"

PMEMobjpool* root_pop;
PMEMobjpool* vpop[kMaxNumShards][kMaxNumRegions];
PMEMobjpool* lpop[kMaxNumShards][kMaxNumRegions];
PMEMobjpool* ipop[kMaxNumShards][kMaxNumPmemLevels][kMaxNumRegions];
TOID(DBRoot) db_root;

void init_pmem_pools(bool use_existing) {
  init_root_pool(use_existing);
  for (int i = 0; i < kNumShards; i++) {
    init_log_pool(i, use_existing);
    init_index_pool(i, use_existing);
    init_value_pool(i, use_existing);
  }
}

void init_root_pool(bool use_existing) {
  char poolpath[200];
  snprintf(poolpath, 200, "/pmem0/brdb_%s/root", kUserName);
  root_pop = NULL;
  if (use_existing) {
    root_pop = pmemobj_open(poolpath, "DBRoot");
  } else {
    pmempool_rm(poolpath, 0);
  }
  if (!root_pop) {
    root_pop = pmemobj_create(poolpath, "DBRoot", PMEMOBJ_MIN_POOL, 0666);
    size_t root_size = sizeof(DBRoot)
                       + (kNumRegions - 1) * sizeof(TOID(RegionBase));
    db_root = pmemobj_root_construct(root_pop, root_size, NULL, NULL);
  } else {
    db_root = POBJ_ROOT(root_pop, DBRoot);
  }
  assert(root_pop != NULL);
}

void init_value_pool(const int s, bool use_existing) {
#if 0
  for (int i = 0; i < kNumRegions; i++) {
    char poolsetpath[100];
    snprintf(poolsetpath, 100, "/pmem%d/brdb_%s/value_%d.set", i, kUserName, s);
    vpop[s][i] = NULL;
    if (use_existing) {
      vpop[s][i] = pmemobj_open(poolsetpath, "VALUE");
    } else {
      pmempool_rm(poolsetpath, 0);
    }
    if (!vpop[s][i]) {
      vpop[s][i] = pmemobj_create(poolsetpath, "VALUE", 0, 0666);
    }
    assert (vpop[s][i] != NULL);
  }
#else
  for (int i = 0; i < kNumRegions; i++) {
    if (s == 0) {
      char poolsetpath[200];
      snprintf(poolsetpath, 200, "/pmem%d/brdb_%s/value_%d.set", i, kUserName, s);
      vpop[s][i] = NULL;
      if (use_existing) {
        vpop[s][i] = pmemobj_open(poolsetpath, "VALUE");
      } else {
        pmempool_rm(poolsetpath, 0);
      }
      if (!vpop[s][i]) {
        vpop[s][i] = pmemobj_create(poolsetpath, "VALUE", 0, 0666);
      }
      assert (vpop[s][i] != NULL);
    } else {
      vpop[s][i] = vpop[0][i];
    }
  }
#endif
}

void init_log_pool(const int s, bool use_existing) {
#if 0
  for (int i = 0; i < kNumRegions; i++) {
    char poolsetpath[100];
    snprintf(poolsetpath, 100, "/pmem%d/brdb_%s/log_%d.set", i, kUserName, s);
    lpop[s][i] = NULL;
    if (use_existing) {
      lpop[s][i] = pmemobj_open(poolsetpath, "LOG");
    } else {
      pmempool_rm(poolsetpath, 0);
    }
    if (!lpop[s][i]) {
      lpop[s][i] = pmemobj_create(poolsetpath, "LOG", 0, 0666);
    }
    assert (lpop[s][i] != NULL);
  }
#else
  if (s == 0) {
    for (int i = 0; i < kNumRegions; i++) {
      char poolsetpath[200];
      snprintf(poolsetpath, 200, "/pmem%d/brdb_%s/log_%d.set", i, kUserName, s);
      lpop[s][i] = NULL;
      if (use_existing) {
        lpop[s][i] = pmemobj_open(poolsetpath, "LOG");
      } else {
        pmempool_rm(poolsetpath, 0);
      }
      if (!lpop[s][i]) {
        lpop[s][i] = pmemobj_create(poolsetpath, "LOG", 0, 0666);
      }
      assert (lpop[s][i] != NULL);
    }
  } else {
    for (int i = 0; i < kNumRegions; i++) {
      lpop[s][i] = lpop[0][i];
    }
  }
#endif
}

void init_index_pool(const int s, bool use_existing) {
#if 0
  for (int i = 0; i < kNumLogUnifiedLevels; i++) {
    for (int j = 0; j < kNumRegions; j++) {
      if (i == 0) {
        char poolsetpath[100];
        snprintf(poolsetpath, 100, "/pmem%d/brdb_%s/level_%d/index_%d.set", j, kUserName, i, s);
        ipop[s][i][j] = NULL;
        if (use_existing) {
          ipop[s][i][j] = pmemobj_open(poolsetpath, "INDEX");
        } else {
          pmempool_rm(poolsetpath, PMEMPOOL_RM_FORCE);
        }
        if (!ipop[s][i][j]) {
          ipop[s][i][j] = pmemobj_create(poolsetpath, "INDEX", 0, 0666);
        }
        assert (ipop[s][i][j] != NULL);
      } else {
        ipop[s][i][j] = ipop[s][0][j];
      }
    }
  }
  for (int i = kNumLogUnifiedLevels; i < kNumPmemLevels; i++) {
    for (int j = 0; j < kNumRegions; j++) {
      char poolsetpath[100];
      snprintf(poolsetpath, 100, "/pmem%d/brdb_%s/level_%d/index_%d.set", j, kUserName, i, s);
      ipop[s][i][j] = NULL;
      if (use_existing) {
        ipop[s][i][j] = pmemobj_open(poolsetpath, "INDEX");
      } else {
        pmempool_rm(poolsetpath, PMEMPOOL_RM_FORCE);
      }
      if (!ipop[s][i][j]) {
        ipop[s][i][j] = pmemobj_create(poolsetpath, "INDEX", 0, 0666);
      }
      assert (ipop[s][i][j] != NULL);
    }
  }
#else
  for (int i = 0; i < kNumLogUnifiedLevels; i++) {
    for (int j = 0; j < kNumRegions; j++) {
      if (s == 0) {
        if (i == 0) {
          char poolsetpath[200];
          snprintf(poolsetpath, 200, "/pmem%d/brdb_%s/level_%d/index_%d.set", j, kUserName, i, s);
          ipop[s][i][j] = NULL;
          if (use_existing) {
            ipop[s][i][j] = pmemobj_open(poolsetpath, "INDEX");
          } else {
            pmempool_rm(poolsetpath, PMEMPOOL_RM_FORCE);
          }
          if (!ipop[s][i][j]) {
            ipop[s][i][j] = pmemobj_create(poolsetpath, "INDEX", 0, 0666);
          }
          assert (ipop[s][i][j] != NULL);
        } else {
          ipop[s][i][j] = ipop[s][0][j];
        }
      } else {
        ipop[s][i][j] = ipop[0][i][j];
      }
    }
  }
  for (int i = kNumLogUnifiedLevels; i < kNumPmemLevels; i++) {
    for (int j = 0; j < kNumRegions; j++) {
      if (s == 0) {
        char poolsetpath[200];
        snprintf(poolsetpath, 200, "/pmem%d/brdb_%s/level_%d/index_%d.set", j, kUserName, i, s);
        ipop[s][i][j] = NULL;
        if (use_existing) {
          ipop[s][i][j] = pmemobj_open(poolsetpath, "INDEX");
        } else {
          pmempool_rm(poolsetpath, PMEMPOOL_RM_FORCE);
        }
        if (!ipop[s][i][j]) {
          ipop[s][i][j] = pmemobj_create(poolsetpath, "INDEX", 0, 0666);
        }
        assert (ipop[s][i][j] != NULL);
      } else {
        ipop[s][i][j] = ipop[0][i][j];
      }
    }
  }
#endif
}
