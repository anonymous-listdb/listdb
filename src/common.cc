#include "common.h"

#include <string>
#include <string_view>

#include "sha1.h"
#include "murmur3.h"

int aahit = 0;
int aamiss = 0;

int kNumShards = kNumShardsDefault;
int kNumRegions = kNumRegionsDefault;
int kNumPmemLevels = kNumPmemLevelsDefault;
int kNumLogUnifiedLevels = kNumLogUnifiedLevelsDefault;
int kNumWorkers = kNumWorkersDefault;
int kCompactionTaskSize = kCompactionTaskSizeDefault;
int kNumMergesToLevel[kMaxNumPmemLevels];
int64_t kMemTableSize = kMemTableSizeDefault;
size_t kDRAMSizeTotal = kDRAMSizeTotalDefault;
int64_t kPmemTableSize[kMaxNumPmemLevels];

char kUserName[100];
char kTabString[kMaxNumPmemLevels][100];

Counters* counter;

// Hash Table
size_t kLookupCacheSize = 0x03ffffff + 1;
HashTableItem* kHashTable[kMaxNumShards];

// DBConf
void DBConf::print() {
  fprintf(stdout, "num_regions: %d\n", num_regions);
  fprintf(stdout, "num_shards: %d\n", num_shards);
  fprintf(stdout, "mem_size: %ld\n", mem_size/1000/1000);
  fprintf(stdout, "dram_limit: %zu\n", dram_limit/1000/1000);
  fprintf(stdout, "num_workers: %d\n", num_workers);
  fprintf(stdout, "task_size: %d\n", task_size);
}

void common_init_global_variables(const DBConf& dbconf) {
  kNumShards = dbconf.num_shards;
  kNumRegions = dbconf.num_regions;
  kMemTableSize = dbconf.mem_size;
  kDRAMSizeTotal = dbconf.dram_limit;
  kNumWorkers = dbconf.num_workers;
  kCompactionTaskSize = dbconf.task_size;
  kNumPmemLevels = dbconf.num_pmem_levels;
  kNumLogUnifiedLevels = dbconf.num_log_unified_levels;
  kLookupCacheSize = dbconf.lookup_cache_size;
  if (kCompactionTaskSize != 1) {
    flogf(stderr, "kCompactionTaskSize cannot be greater than 1 at this moment. (current: %d)", kCompactionTaskSize);
    exit(0);
  }

  getlogin_r(kUserName, 100);

  size_t lower_size = kMemTableSize;
  kNumMergesToLevel[0] = dbconf.l0_mem_merges;
  kNumMergesToLevel[1] = 10;
  kNumMergesToLevel[2] = -1;
  for (int i = 0; i < kNumPmemLevels; i++) {
    kPmemTableSize[i] = kNumMergesToLevel[i] * lower_size;
    lower_size = kPmemTableSize[i];
    sprintf(kTabString[i], "%s", std::string(4*(i+1), ' ').c_str());
  }
  if (kNumPmemLevels > 1) {
    kPmemTableSize[kNumPmemLevels - 1] = -1;  // infinite size for the last level
  }

  kLookupCacheSize = kLookupCacheSize / kNumShards;
  for (int s = 0; s < kNumShards; s++) {
    kHashTable[s] = new HashTableItem[kLookupCacheSize];
    for (uint64_t i = 0; i < kLookupCacheSize; i++) {
      std::atomic_store((std::atomic<uint64_t>*) &kHashTable[s][i].version, 1UL);
    }
    //kStash = new HashTableItem[kStashSize];
    //for (uint64_t i = 0; i < kStashSize; i++) {
    //  std::atomic_store((std::atomic<uint64_t>*) &kStash[i].version, 1UL);
    //}
  }
}

//std::string_view VARSTR_node::head_key() {
//  const static std::string hk("\0\0\0\0", 4);
//  return std::string_view(hk);
//}
//
//std::string_view VARSTR_pnode::head_key() {
//  const static std::string hk("\0\0\0\0", 4);
//  return std::string_view(hk);
//}
//

void ht_add(const int s, const uint64_t key, const uint64_t value) {
  //uint32_t idx = key % kLookupCacheSize;
	//uint64_t idx = ht_sha1(key);
#ifndef BR_STRING_KV
  // key: integer key
  uint32_t idx = ht_murmur3(key);
  uint32_t idx2 = ht_sha1(key);
  auto& buckt = kHashTable[s][idx];
  auto& buckt2 = kHashTable[s][idx2];
  uint64_t prev_ver = std::atomic_load((std::atomic<uint64_t>*) &buckt.version);
  uint64_t prev_ver2 = std::atomic_load((std::atomic<uint64_t>*) &buckt2.version);
  if (prev_ver > prev_ver2) {
    while (!std::atomic_compare_exchange_weak((std::atomic<uint64_t>*) &buckt2.version, &prev_ver2, 0UL)) continue;
    buckt2.key = key;
    buckt2.value = value;
    std::atomic_store((std::atomic<uint64_t>*) &buckt2.version, prev_ver2 + 1);
  } else {
    while (!std::atomic_compare_exchange_weak((std::atomic<uint64_t>*) &buckt.version, &prev_ver, 0UL)) continue;
    buckt.key = key;
    buckt.value = value;
    std::atomic_store((std::atomic<uint64_t>*) &buckt.version, prev_ver + 1);
  }
#else
  // key: pointer to encoded key
  StringKey encoded_key((char*) key);
	//uint32_t idx = ht_sha1(encoded_key.key());
  uint32_t idx = ht_murmur3(encoded_key.key());
  auto& buckt = kHashTable[s][idx];
  uint64_t prev_ver = std::atomic_load((std::atomic<uint64_t>*) &buckt.version);
  while (!std::atomic_compare_exchange_weak((std::atomic<uint64_t>*) &buckt.version, &prev_ver, 0UL)) continue;
  buckt.key = key;
  buckt.value = value;
  std::atomic_store((std::atomic<uint64_t>*) &buckt.version, prev_ver + 1);
#endif
}

bool ht_get(const int s, const std::string_view& key, uint64_t* value_out) {
  //uint64_t idx = key % kLookupCacheSize;
	//uint32_t idx = ht_sha1(key);
  uint32_t idx = ht_murmur3(key);
  auto& buckt = kHashTable[s][idx];
  uint64_t k;
  uint64_t v;
  while (true) {
    uint64_t prev_ver = buckt.version;
    if (prev_ver == 0) continue;
    k = buckt.key;
    v = buckt.value;
    if (prev_ver != buckt.version) continue;
    break;
  }
  if (buckt.version > 1) {
    StringKey encoded_key((char*) k);
    if (encoded_key.key().compare(key) == 0) {
      if (value_out) {
        *value_out = v;
      }
      return true;
    }
  }
  return false;
}
bool ht_get(const int s, const uint64_t key, uint64_t* value_out) {
  //uint64_t idx = key % kLookupCacheSize;
	//uint32_t idx = ht_sha1(key);
  uint64_t k;
  uint64_t v;
  uint64_t k2;
  uint64_t v2;
  uint32_t idx = ht_murmur3(key);
  uint32_t idx2 = ht_sha1(key);
  auto& buckt = kHashTable[s][idx];
  auto& buckt2 = kHashTable[s][idx2];
  uint64_t prev_ver;
  uint64_t prev_ver2;
  while (true) {
    prev_ver = buckt.version;
    if (prev_ver == 0) continue;
    k = buckt.key;
    v = buckt.value;
    if (prev_ver != buckt.version) continue;
    break;
  }
  if (buckt.version > 1) {
    if (k == key) {
      if (value_out) {
        *value_out = v;
      }
      return true;
    }
  }
  while (true) {
    prev_ver2 = buckt2.version;
    if (prev_ver2 == 0) continue;
    k2 = buckt2.key;
    v2 = buckt2.value;
    if (prev_ver2 != buckt2.version) continue;
    break;
  }
  if (buckt2.version > 1) {
    if (k2 == key) {
      if (value_out) {
        *value_out = v2;
      }
      return true;
    }
  }
  return false;
}

uint32_t ht_sha1(const std::string_view& key) {
	char result[21];  // 5 * 32bit
	SHA1(result, key.data(), key.size());
	return *reinterpret_cast<uint32_t*>(result) % kLookupCacheSize;
}
uint32_t ht_sha1(const uint64_t key) {
	char result[21];  // 5 * 32bit
	SHA1(result, (char*) &key, 8);
	return *reinterpret_cast<uint32_t*>(result) % kLookupCacheSize;
}
void ht_sha1(const uint64_t key, char* result) {
	//char result[21];  // 5 * 32bit
	SHA1(result, (char*) &key, 8);
}
uint32_t ht_murmur3(const std::string_view& key) {
	uint32_t h;
	static const uint32_t seed = 0xcafeb0ba;
#ifndef BR_STRING_KV
	MurmurHash3_x86_32(key.data(), sizeof(uint64_t), seed, (void*) &h);
#else
	MurmurHash3_x86_32(key.data(), key.length(), seed, (void*) &h);
#endif
	return h % kLookupCacheSize;
}
uint32_t ht_murmur3(const uint64_t key) {
	uint32_t h;
	static const uint32_t seed = 0xcafeb0ba;
	MurmurHash3_x86_32(&key, sizeof(uint64_t), seed, (void*) &h);
	return h % kLookupCacheSize;
}
void ht_murmur3(const uint64_t key, uint32_t* h) {
	static const uint32_t seed = 0xcafeb0ba;
	MurmurHash3_x86_32(&key, sizeof(uint64_t), seed, (void*) h);
}
