#ifndef COMMON_H_
#define COMMON_H_

#include <atomic>
#include <iostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include <sched.h>
#include <assert.h>
#include <unistd.h>
#include <sys/syscall.h>

#include "util.h"
#include "key_types.h"
//#include "ds/hash_table.h"

extern int aahit;
extern int aamiss;

// Node Type
#ifndef BR_STRING_KV
#define mNode UINT64_node
#define pNode UINT64_pnode
#else
#define mNode VARSTR_node
#define pNode VARSTR_pnode
#endif
// Maximum Values
constexpr int16_t kPrimaryRegion = 0;
constexpr int kMaxNumShards = 256;
constexpr int kMaxNumRegions = 4;
constexpr int kMaxHeight = 12;
//constexpr size_t kLogBlockSize = 128*(1ull<< 20) - 32;  // 32 = sizeof(LogBlockBase) - 1
constexpr size_t kLogBlockSize = (1ull<<30)/kMaxNumShards - 32;  // 32 = sizeof(LogBlockBase) - 1
constexpr int kMaxNumPmemLevels = 3;
constexpr int kMaxNumClients = 80;
constexpr int kMaxNumWorkers = 80;
#ifndef BR_LOG_IUL
constexpr char kLoggingModeString[4] = "WAL";
#else
constexpr char kLoggingModeString[4] = "IUL";
#endif


// Default Values
constexpr int kNumShardsDefault = 1;
constexpr int kNumRegionsDefault = 1;
constexpr int kNumPmemLevelsDefault = 2;
constexpr int kNumLogUnifiedLevelsDefault = 2;
constexpr int kNumWorkersDefault = 1;
constexpr int kCompactionTaskSizeDefault = 1;
constexpr int64_t kMemTableSizeDefault = 256*1000*1000;
constexpr size_t kDRAMSizeTotalDefault = 1024*1000*1000;
constexpr int kPeriodicCompactionModeDefault = 0;
constexpr int kPerformanceMonitorModeDefault = 0;
constexpr int kNumMergesToL0Default = 1;

// In-use
extern int kNumShards;
extern int kNumRegions;
extern int kNumPmemLevels;
extern int kNumLogUnifiedLevels;
extern int kNumWorkers;
extern int kCompactionTaskSize;
extern int64_t kMemTableSize;
extern size_t kDRAMSizeTotal;
extern int kNumMergesToLevel[kMaxNumPmemLevels];
extern int64_t kPmemTableSize[kMaxNumPmemLevels];

struct alignas(64) Counters {
  size_t put_cnt = 0;
  size_t get_cnt = 0;
};
extern Counters* counter;

// Hash Table

//#define LPV2

struct SharedVector {
  std::shared_mutex mu;
  std::vector<uint32_t> vec;
};
struct HashTableItem {
  uint64_t version;
  uint64_t key;
  uint64_t value;
#ifdef LPV2
  SharedVector idx3;
#endif
};
extern size_t kLookupCacheSize;
extern HashTableItem* kHashTable[kMaxNumShards];
//extern thread_local uint64_t ht_lookup_cnt;
//extern thread_local uint64_t ht_hit_cnt;
void ht_add(const int s, const uint64_t key, const uint64_t value);
void ht_add(const int s, const uint64_t key, const uint64_t value);
bool ht_get(const int s, const std::string_view& key, uint64_t* value_out);
bool ht_get(const int s, const uint64_t key, uint64_t* value_out);
#ifndef LPV2
void ht_add_lp(const uint64_t key, const uint64_t value);
bool ht_get_lp(const uint64_t key, uint64_t* value_out);
#else
void ht_add_lp_v2(const uint64_t key, const uint64_t value);
bool ht_get_lp_v2(const uint64_t key, uint64_t* value_out);
#endif
void ht_add_stash(const uint64_t key, const uint64_t value);
bool ht_get_stash(const uint64_t key, uint64_t* value_out);
uint32_t ht_sha1(const std::string_view& key);
uint32_t ht_sha1(const uint64_t key);
void ht_sha1(const uint64_t key, char* result);
void ht_murmur3(const uint64_t key, uint32_t* h);
uint32_t ht_murmur3(const std::string_view& key);
uint32_t ht_murmur3(const uint64_t key);


// etc.
extern char kUserName[100];
extern char kTabString[kMaxNumPmemLevels][100];

enum ValueType {
  kTypeShortcut = 0x0,
  kTypeValue = 0x1,
  kTypeDeletion = 0x2
};


struct alignas(64) ThreadData {
  int cpu;
  int16_t numa;
  int16_t region;
  size_t mem_compaction_cnt = 0;
  double mem_compaction_dur = 0;
  size_t pmem_compaction_cnt[kMaxNumPmemLevels] = { 0 };
  double pmem_compaction_dur[kMaxNumPmemLevels] = { 0 };
  Random rnd;

  ThreadData() : rnd(0xdeadbeef) { }
};

struct DBConf {
  int num_shards = kNumShardsDefault;
  int num_regions = kNumRegionsDefault;
  int64_t mem_size = kMemTableSizeDefault;
  size_t dram_limit = kDRAMSizeTotalDefault;
  int num_workers = kNumWorkersDefault;
  int task_size = kCompactionTaskSizeDefault;  // parallelism
  bool use_existing = false;
  // 0: Disabled
  // 1: All non-empty tables
  int periodic_compaction_mode = kPeriodicCompactionModeDefault;
  // 0: Disabled
  // 1: Enabled
  int performance_monitor_mode = kPerformanceMonitorModeDefault;
  int l0_mem_merges = kNumMergesToL0Default;
  int num_pmem_levels = kMaxNumPmemLevels;
  int num_log_unified_levels = kNumLogUnifiedLevelsDefault;
  size_t lookup_cache_size = 0x03ffffff + 1;

  void print();
};

struct UINT64_pnode;

struct UINT64_node {
  uint64_t k;       // integer value or offset
  uint64_t t;       // tag
  uint64_t v;     // integer value or offset, (SHORTCUT: pointer to next memtable node)
  uint64_t log_moff;  // log marked offset, (SHORTCUT: moff to upper pmem node)
  int height;
  std::atomic<UINT64_node*> next[1];

  static uint64_t head_key() { return 0ULL; }

  static size_t compute_alloc_size(const uint64_t key, const int height) {
    return aligned_size(8, sizeof(UINT64_node) + (height-1)*8);
  }

  static UINT64_node* init_node(char* buf, const uint64_t key,
                                 const uint64_t tag, const uint64_t value,
                                 const int height, bool init_next_arr = true) {
    UINT64_node* node = (UINT64_node*) buf;
    node->k = key;
    node->t = tag;
    node->v = value;
    //node->shortcut = NULL;
    //node->pmem_shortcut = NULL;
    node->height = height;
    if (init_next_arr) {
      memset(node->next, 0, height * 8);
    }
    return node;
  }

  size_t alloc_size() const { return sizeof(UINT64_node) + (height-1)*8; }
  uint64_t key() const { return k; }
  uint64_t tag() const { return t; }
  uint8_t type() const { return ValueType(t & 0xff); }
  uint64_t value() const { return v; }
  char* data() const { return (char*) this; }
};

struct UINT64_pnode {
  uint64_t k;  // key
  uint64_t t;  // tag
  uint64_t v;  // value  (SHORTCUT: moff to next pmemtable node)
  int height;
  std::atomic<uint64_t> next[1];

  static uint64_t head_key() { return 0ULL; }

  static size_t compute_alloc_size(const uint64_t key, const int height) {
    return aligned_size(8, sizeof(UINT64_pnode) + (height-1)*8);
  }

  static UINT64_pnode* init_node(char* buf, const uint64_t key,
                                 const uint64_t tag, const uint64_t value,
                                 const int height, bool init_next_arr = true) {
    UINT64_pnode* node = (UINT64_pnode*) buf;
    node->k = key;
    node->t = tag;
    node->v = value;
    //node->shortcut = NULL;
    node->height = height;
    if (init_next_arr) {
      memset(node->next, 0, height * 8);
    }
    return node;
  }

  static UINT64_pnode* load_node(char* buf) {
    return (UINT64_pnode*) buf;
  }

  size_t alloc_size() const { return sizeof(UINT64_pnode) + (height-1)*8; }
  uint64_t key() const { return k; }
  uint64_t tag() const { return t; }
  uint8_t type() const { return ValueType(t & 0xff); }
  uint64_t value() const { return v; }
  char* data() const { return (char*) this; }
};

struct VARSTR_node {
  int key_roff;
  int height;
  uint64_t log_moff;
  std::atomic<VARSTR_node*> next[1];

  static std::string_view head_key() {
    const static std::string hk("\0\0\0\0", 4);
    return std::string_view(hk);
  }

  static size_t compute_alloc_size(const std::string_view& key, const int height) {
    return StringKey::compute_alloc_size(key) + sizeof(VARSTR_node) + (height-1)*8;
  }

  static VARSTR_node* init_node(char* buf, const std::string_view& key,
                                 const uint64_t tag, const uint64_t value,
                                 const int height, bool init_next_arr = true) {
    StringKey encoded_key(buf, key, tag, value);
    size_t enckey_alloc_size = encoded_key.alloc_size();
    int key_roff = (-1) * enckey_alloc_size;
    char* p = buf - key_roff;
    VARSTR_node* node = (VARSTR_node*) p;
    node->key_roff = key_roff;
    node->height = height;
    if (init_next_arr) {
      memset(node->next, 0, height * 8);
    }
    return node;
  }

  size_t alloc_size() {
    StringKey encoded_key(this->data());
    return encoded_key.alloc_size() + sizeof(VARSTR_node) + (height-1)*8;
  }

  StringKey encoded_key() const {
    StringKey encoded_key(this->data());
    return encoded_key;
  }

  std::string_view key() const {
    StringKey encoded_key(this->data());
    return encoded_key.key();
  }
  uint64_t tag() const {
    StringKey encoded_key(this->data());
    return encoded_key.tag();
  }
  uint8_t type() const {
    return ValueType(tag() & 0xff);
  }
  uint64_t value() const {
    StringKey encoded_key(this->data());
    return encoded_key.value();
  }
  char* data() const {
    return (char*) this + key_roff;
  }
};

struct VARSTR_pnode {
  int key_roff;
  int height;
  std::atomic<uint64_t> next[1];

  static std::string_view head_key() {
    const static std::string hk("\0\0\0\0", 4);
    return std::string_view(hk);
  }

  static size_t compute_alloc_size(const std::string_view& key, const int height) {
    return StringKey::compute_alloc_size(key) + sizeof(VARSTR_pnode) + (height-1)*8;
  }

  static VARSTR_pnode* init_node(char* buf, const std::string_view& key,
                                 const uint64_t tag, const uint64_t value,
                                 const int height, bool init_next_arr = true) {
    StringKey encoded_key(buf, key, tag, value);
    size_t enckey_alloc_size = encoded_key.alloc_size();
    int key_roff = (-1) * enckey_alloc_size;
    char* p = buf - key_roff;
    VARSTR_pnode* node = (VARSTR_pnode*) p;
    node->key_roff = key_roff;
    node->height = height;
    if (init_next_arr) {
      memset(node->next, 0, height * 8);
    }
    return node;
  }

  static VARSTR_pnode* load_node(char* buf) {
    StringKey encoded_key(buf);
    char* p = buf + encoded_key.alloc_size();
    return (VARSTR_pnode*) p;
  }

  size_t alloc_size() {
    return sizeof(VARSTR_pnode) + (height-1)*8 - key_roff;
  }

  char* data() const {
    return (char*) this + key_roff;
  }
  StringKey encoded_key() const {
    StringKey encoded_key(this->data());
    return encoded_key;
  }
  std::string_view key() const {
    StringKey encoded_key(this->data());
    return encoded_key.key();
  }
  uint64_t tag() const {
    StringKey encoded_key(this->data());
    return encoded_key.tag();
  }
  uint8_t type() const {
    return ValueType(tag() & 0xff);
  }
  uint64_t value() const {
    StringKey encoded_key(this->data());
    return encoded_key.value();
  }
};

struct UINT64_cmp {
  int operator()(const UINT64_node* a, const UINT64_node* b) const {
    assert(b != NULL);
    if (a == NULL || a->key() > b->key()) {
      return 1;
    } else if (a->key() < b->key()) {
      return -1;
    } else {
      if (a->tag() < b->tag()) {
        return 1;
      } else if (a->tag() > b->tag()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
  int operator()(const UINT64_node* a, const uint64_t b) const {
    if (a == NULL || a->key() > b) {
      return 1;
    } else if (a->key() < b) {
      return -1;
    } else {
      return 0;
    }
  }
};

struct UINT64_pcmp {
  int operator()(const UINT64_pnode* a, const UINT64_pnode* b) const {
    assert(b != NULL);
    if (a == NULL || a->key() > b->key()) {
      return 1;
    } else if (a->key() < b->key()) {
      return -1;
    } else {
      if (a->tag() < b->tag()) {
        return 1;
      } else if (a->tag() > b->tag()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
  int operator()(const UINT64_pnode* a, const uint64_t b) const {
    if (a == NULL || a->key() > b) {
      return 1;
    } else if (a->key() < b) {
      return -1;
    } else {
      return 0;
    }
  }
};

struct VARSTR_cmp {
  int operator()(const VARSTR_node* a, const VARSTR_node* b) const {
    assert(b != NULL);
    if (a == NULL) {
      return 1;
    } else {
      auto a_enc = a->encoded_key();
      auto b_enc = b->encoded_key();
      int cmp;
      if ((cmp = a_enc.key().compare(b_enc.key())) == 0) {
        if (a_enc.tag() > b_enc.tag()) {
          return -1;
        } else if (a_enc.tag() < b_enc.tag()) {
          return 1;
        } else {
          return 0;
        }
      } else {
        return cmp;
      }
    }
  }
  int operator()(const VARSTR_node* a, const std::string_view& b) const {
    if (a == NULL) {
      return 1;
    } else {
      auto a_user_key = a->key();
      return a_user_key.compare(b);
    }
  }
};

struct VARSTR_pcmp {
  int operator()(const VARSTR_pnode* a, const VARSTR_pnode* b) const {
    assert(b != NULL);
    if (a == NULL) {
      return 1;
    } else {
      auto a_enc = a->encoded_key();
      auto b_enc = b->encoded_key();
      int cmp;
      if ((cmp = a_enc.key().compare(b_enc.key())) == 0) {
        if (a_enc.tag() > b_enc.tag()) {
          return -1;
        } else if (a_enc.tag() < b_enc.tag()) {
          return 1;
        } else {
          return 0;
        }
      } else {
        return cmp;
      }
    }
  }
  int operator()(const VARSTR_pnode* a, const std::string_view& b) const {
    if (a == NULL) {
      return 1;
    } else {
      auto a_user_key = a->key();
      return a_user_key.compare(b);
    }
  }
};


// Entry Layout
// | internal_key | value_off | padding | node |
struct EmbeddedKey;
struct ConcatenatedNode {
  uint32_t key_off;  // negatively relative to this position (not PMEMobjpool)
  uint16_t node_size;  // relative to this position (not PMEMobjpool)
  uint16_t height;
  std::atomic<uint64_t> next[1];

  EmbeddedKey* embedded_key_ptr() {
    //char* p = (char*) this - key_off;
    char* p = (char*) ((uint64_t) &key_off - (uint64_t) key_off);
    return (EmbeddedKey*) p;
  }

  char* value_field_ptr() {
    //return (char*) this + node_size;
    return (char*) ((uint64_t) &key_off + (uint64_t) node_size);
  }
};

struct EmbeddedKey {
  uint64_t cmp;
  uint32_t internal_key_size;
  char ikey[1];

  const char* encoded_key() {
    return (char*) &internal_key_size;
  }

  ConcatenatedNode* node_ptr() {
    uint32_t encoded_len = (sizeof(EmbeddedKey) - 1) + internal_key_size;
    uint32_t mod = encoded_len & 7;
    uint32_t slop = (mod == 0 ? 0 : 8 - mod);
    //char* p = (char*) this + encoded_len + slop;
    char* p = (char*) ((uint64_t) &cmp + encoded_len + slop);
    return (ConcatenatedNode*) p;
  }

  //ValueType type() {
  //  char* p = ikey + internal_key_size - 8;
  //  uint64_t tag = *reinterpret_cast<uint64_t*>(p);
  //  ValueType type = static_cast<ValueType>(tag & 0xff);
  //  return type;
  //}
};

inline void set_affinity(int coreid) {
  coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(coreid, &mask);
#ifndef NDEBUG
  int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
  assert(rc == 0);
#else
  sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
#endif
}

void common_init_global_variables(const DBConf& dbconf);

#endif  // COMMON_H_
