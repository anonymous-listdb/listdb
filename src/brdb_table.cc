#include "brdb.h"

// Table Management Functions
// table container: nbr_stack<>

std::shared_ptr<MemTable> brdb::get_writable_memtable(ThreadData* td, const int s, const size_t kv_size, JobManager* mgr) {
  std::shared_ptr<MemTable> mem = mem_[s].table;
  while (true) {
    if (mem == nullptr) {
      mem = mem_[s].table;
      continue;
    }
    size_t before = mem->fetch_add_size(kv_size);
    if (kMemTableSize > 0 && kv_size + before > (size_t) kMemTableSize / kNumShards) {
      std::unique_lock<std::shared_mutex> lk(mem_[s].mu);
      if (mem == mem_[s].table) {
        // Write-Stall
        size_t num_imms = mem_[s].immutables.size();
        size_t dram_usage = (num_imms + 1) * kMemTableSize / kNumShards;
        size_t dram_limit = kDRAMSizeTotal / kNumShards;
        if (dram_usage >= dram_limit) {
          auto stall_begin = std::chrono::steady_clock::now();
          std::unique_lock<std::mutex> ws_lk(ws_mu_[s]);
          ws_cv_[s].wait(ws_lk, [&]{
                size_t ni = mem_[s].immutables.size();
                size_t du = (ni + 1) * kMemTableSize / kNumShards;
                return (du < kDRAMSizeTotal / kNumShards);
              });
          auto stall_end = std::chrono::steady_clock::now();
          std::chrono::duration<double> stall_dur = stall_end - stall_begin;
          fprintf(stderr, "Stall time: %.3lf sec\n", stall_dur.count());
        }
        // New MemTable
        auto mem_old = std::move(mem);
        mem = new_mem_table(s, mem_old);
#ifdef BR_CASCADE
#ifdef BR_LOG_IUL
        mem->init_shortcut_IUL(lpop[s]);
#endif
#endif
        mem->set_seq_order(memtable_seq_[s].fetch_add(1));
        mem_[s].immutables.push_front(mem_old);
        std::atomic_store(&mem_[s].table, mem);
        // immutable ordering
        // immutables.push_front should be done inside a critical section
        auto future_pmem = mem_old->get_future_pmem_table();
        //assert(future_pmem != nullptr);
#ifndef BR_LOG_IUL
        future_pmem->init(ipop[s][0]);
#else
        future_pmem->init(lpop[s]);
#endif
        future_pmem->set_shard(s);
        future_pmem->set_seq_order(mem_old->seq_order());
        future_pmem->cas_mark_full();
        level_[s][0].immutables.push_front(future_pmem);
        lk.unlock();
        enq_mem_compaction(s, mem_old, mgr);
      } else {
        //mem = nullptr;
        mem = mem_[s].table;
        lk.unlock();
      }
      continue;
    }
    break;
  }
  return mem;
}

std::shared_ptr<PmemTable> brdb::get_writable_pmemtable(ThreadData* td, const int s, const int level, const size_t write_size, JobManager* mgr) {
  auto pmem = level_[s][level].table;
  while (true) {
    if (pmem == nullptr) {
      pmem = level_[s][level].table;
      continue;
    }
    size_t before = pmem->fetch_add_size(write_size);
    if (kPmemTableSize[level] > 0 && write_size + before > (size_t) kPmemTableSize[level] / kNumShards) {
      if (pmem->cas_mark_full() == 0) {
        auto pmem_old = std::move(pmem);
        pmem = new_pmem_table(s, level, pmem_old);
        level_[s][level].immutables.push_front(pmem_old);
        std::atomic_store(&(level_[s][level].table), pmem);
        if (level < kNumPmemLevels - 1) {
          enq_pmem_compaction(s, level, pmem_old, mgr);
        }
      } else {
        pmem = nullptr;
      }
      continue;
    }
    break;
  }
  return pmem;
}

std::shared_ptr<MemTable> brdb::new_mem_table(const int s, std::shared_ptr<MemTable> old) {
  auto new_mem = std::make_shared<MemTable>(old);
  new_mem->shard_ = s;
  return new_mem;
}

std::shared_ptr<PmemTable> brdb::new_pmem_table(const int s, const int level, std::shared_ptr<PmemTable> old) {
  auto new_pmem = std::make_shared<PmemTable>(old);
  if (level < kNumLogUnifiedLevels) {
#ifndef BR_LOG_IUL
    new_pmem->init(ipop[s][0]);
#else
    new_pmem->init(lpop[s]);
#endif
  } else {
    new_pmem->init(ipop[s][level]);
  }
  new_pmem->set_shard(s);
  return new_pmem;
}
