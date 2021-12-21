#include "brdb.h"

void brdb::put(ThreadData* td, const std::string_view& key, const std::string_view& value) {
  uint64_t cmp = *((uint64_t*) key.data());
  int s = cmp % kNumShards;
  if (conf_.mem_size == 0) {
#ifndef BR_STRING_KV
    write_to_pmem(td, s, key, *((uint64_t*) value.data()));
#else
    uint64_t value_moff = store_value(td, s, value);
    write_to_pmem(td, s, key, value_moff);
#endif
  } else {
#ifndef BR_STRING_KV
    write_to_mem(td, s, key, *((uint64_t*) value.data()));
#else
    uint64_t value_moff = store_value(td, s, value);
    write_to_mem(td, s, key, value_moff);
#endif
  }
  //td->put_cnt++;
  counter[td->cpu].put_cnt++;
}

void brdb::put_batch(ThreadData* td, std::vector<std::pair<std::string_view, std::string_view>>& kvbatch) {
fprintf(stderr, "NOT IMPLEMENTED : %s\n", __FUNCTION__);
exit(1);
  // Implement this later
  //std::vector<std::vector<std::pair<std::string_view, std::string_view>>> batch_table(kNumShards);
  //for (auto& kv : kvbatch) {
  //  uint64_t cmp = *((uint64_t*) key.data());
  //  int s = cmp % kNumShards;
  //}
}

void brdb::write_to_mem(ThreadData* td, const int s, const std::string_view& key, const uint64_t value) {
#ifndef BR_STRING_KV
  const size_t kv_size = 2 * sizeof(uint64_t);
#else
  const size_t kv_size = key.size() + sizeof(uint64_t);
#endif
  auto mem = get_writable_memtable(td, s, kv_size, job_mgr_);

  // write_log is very slow at this moment.
  // - POSSIBLE REASON:
  //  In the previous version (zipperdb2), we use different pmempools for
  // different shards. However, in this version, we use the same pmempool for
  // different shards.
  //
  // To test memtable performance only (not logging):
  //  >>>
  //  void* log_ptr = NULL;
  //  <<<
  uint64_t log_moff = write_log(td, s, key, value);
#ifndef BR_STRING_KV
  mem->add(td, *reinterpret_cast<const uint64_t*>(key.data()), value, log_moff);
#else
  mem->add(td, key, value, log_moff);
#endif
}

void brdb::write_to_pmem(ThreadData* td, const int s, const std::string_view& key, const uint64_t value) {
#ifndef BR_STRING_KV
  const size_t kv_size = 2 * sizeof(uint64_t);
#else
  const size_t kv_size = key.size() + sizeof(uint64_t);
#endif

  auto pmem = get_writable_pmemtable(td, s, 0, kv_size, job_mgr_);

#ifndef BR_STRING_KV
  pmem->add(td, td->region, *reinterpret_cast<const uint64_t*>(key.data()), value);
#else
  pmem->add(td, td->region, key, value);
#endif
}

inline uint64_t brdb::write_log(ThreadData* td, const int s, const std::string_view& key, const uint64_t value) {
#ifndef BR_LOG_IUL
  return log_[s]->write_WAL(td, s, key, value);
#else
  return log_[s]->write_IUL(td, s, key, value);
#endif
}

inline uint64_t brdb::store_value(ThreadData* td, const int s, const std::string_view& value) {
  return vlog_[s]->write_value(td, s, value);
}
