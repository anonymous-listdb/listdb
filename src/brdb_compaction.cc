#include "brdb.h"

#include <bitset>

void brdb::enq_mem_compaction(const int s, std::shared_ptr<MemTable>& imm, JobManager* mgr) {
  // Construct a job
  int num_tasks = std::min(kNumWorkers, kCompactionTaskSize);
  auto job = std::make_shared<Job>(-1, imm->seq_order(), num_tasks);
  job->set_before([&, s, imm, job_raw=job.get()]{
        while (imm.use_count() > 3) continue;
        //size_t write_size = kMemTableSize / kNumShards;
        auto future_pmem = imm->get_future_pmem_table();
        future_pmem->set_shard(s);
        assert(future_pmem != nullptr);
        job_raw->upper_ = future_pmem;
      });
  // Construct tasks of the job
  int region_cnt = 0;
  int num = kNumRegions / num_tasks;
  for (int i = 0; i < num_tasks; i++) {
    if (i == num_tasks - 1) {
      num += kNumRegions % num_tasks;
    }
    std::bitset<kMaxNumRegions> bs;
    for (int j = 0; j < num; j++) {
      bs.set(region_cnt++);
    }
    unsigned long rmask = bs.to_ulong();  // region bit masks for a task
    auto task_fn = [&, s, rmask, imm_raw=imm.get(), job_raw=job.get()](ThreadData* td) {
      run_mem_compaction_task(td, s, rmask, imm_raw, job_raw->upper_.get());
    };
    auto task = std::make_shared<Task>(job, task_fn);
    job->add_task(task);
  }
  // Callback of the job
  auto callback_fn = [&, s, imm, job_raw=job.get()]{
    imm->mark_persist();
    mem_[s].immutables.pop_backs_if([&](std::shared_ptr<MemTable>& t) {
          return t->is_persist();
        });
    if (kNumPmemLevels > 1) {
      enq_pmem_compaction(s, 0, job_raw->upper_, job_raw->job_mgr());
    }
    job_raw->upper_ = nullptr;
    ws_cv_[s].notify_all();
  };
  job->set_callback(callback_fn);

  mgr->enqueue(job);
}

void brdb::run_mem_compaction_task(ThreadData* td, const int s,
                                   const unsigned long rmask,
                                   MemTable* imm, PmemTable* pmem) {
  auto begin = std::chrono::steady_clock::now();
#ifndef BR_LOG_IUL
  pmem->merge_WAL(td, rmask, imm);
#else
  pmem->merge_IUL(td, rmask, imm);
#endif
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> dur = end - begin;
  td->mem_compaction_dur += dur.count();
}

void brdb::enq_pmem_compaction(const int s, const int lower_level,
                               std::shared_ptr<PmemTable>& lower,
                               JobManager* mgr) {
  int upper_level = lower_level + 1;
  if (upper_level < kNumLogUnifiedLevels) {
    enq_zipper_compaction(s, lower_level, upper_level, lower, mgr);
  } else {
    enq_log_structured_compaction(s, lower_level, upper_level, lower, mgr);
  }
}

void brdb::enq_zipper_compaction(const int s, const int lower_level,
                                 const int upper_level,
                                 std::shared_ptr<PmemTable>& lower,
                                 JobManager* mgr) {
  // Construct a job
  int num_tasks = std::min(kNumWorkers, kCompactionTaskSize);
  auto job = std::make_shared<Job>(lower_level, 0, num_tasks);
  job->set_before([&, s, lower_level, upper_level, lower, job_raw=job.get()]{
        while (lower.use_count() > 4/* FOR TEST */) { /*fprintf(stderr,"TEST %p use_count = %d\n", lower.get(), lower.use_count());*/ continue; }
        size_t write_size = kPmemTableSize[lower_level] / kNumShards;
        job_raw->upper_ = get_writable_pmemtable(NULL, s, upper_level, write_size, job_raw->job_mgr());
      });
  // Construct tasks of the job
  int region_cnt = 0;
  int num = kNumRegions / num_tasks;
  for (int i = 0; i < num_tasks; i++) {
    if (i == num_tasks - 1) {
      num += kNumRegions % num_tasks;
    }
    std::bitset<kMaxNumRegions> bs;
    for (int j = 0; j < num; j++) {
      bs.set(region_cnt++);
    }
    unsigned long rmask = bs.to_ulong();
    auto task = std::make_shared<Task>(job,
        [&, s, rmask, lower_level, upper_level, lower_raw=lower.get(), job_raw=job.get()](ThreadData* td) {
          run_zipper_compaction_task(td, s, rmask, lower_level, upper_level, lower_raw, job_raw->upper_.get());
        });
    job->add_task(task);
  }
  job->set_callback([&, s, lower_level, lower, job_raw=job.get()]{
        lower->mark_merged_down();
        level_[s][lower_level].immutables.pop_backs_if([&](std::shared_ptr<PmemTable>& t) {
              return t->is_merged_down();
            });
        job_raw->upper_ = nullptr;
      });
  mgr->enqueue(job);
}

void brdb::run_zipper_compaction_task(ThreadData* td, const int s, const unsigned long rmask,
                                      const int lower_level,
                                      const int upper_level,
                                      PmemTable* lower,
                                      PmemTable* upper) {
  // Do compaction
  upper->zipper_compaction(td, rmask, lower_level, lower);
}

void brdb::enq_log_structured_compaction(const int s, const int lower_level,
                                         const int upper_level,
                                         std::shared_ptr<PmemTable>& lower,
                                         JobManager* mgr) {
  // Construct a job
  int num_tasks = std::min(kNumWorkers, kCompactionTaskSize);
  auto job = std::make_shared<Job>(lower_level, 0, num_tasks);
  job->set_before([&, s, lower_level, upper_level, lower, job_raw=job.get()]{
        while (lower.use_count() > 3) { /*fprintf(stderr,"TEST %d\n", lower.use_count());*/ continue; }
        size_t write_size = kPmemTableSize[lower_level] / kNumShards;
        job_raw->upper_ = get_writable_pmemtable(NULL, s, upper_level, write_size, job_raw->job_mgr());
      });
  // Construct tasks of the job
  int region_cnt = 0;
  int num = kNumRegions / num_tasks;
  for (int i = 0; i < num_tasks; i++) {
    if (i == num_tasks - 1) {
      num += kNumRegions % num_tasks;
    }
    std::bitset<kMaxNumRegions> bs;
    for (int j = 0; j < num; j++) {
      bs.set(region_cnt++);
    }
    unsigned long rmask = bs.to_ulong();
    auto task = std::make_shared<Task>(job,
        [&, s, rmask, lower_level, upper_level, lower_raw=lower.get(), job_raw=job.get()](ThreadData* td) {
          run_log_structured_compaction_task(td, s, rmask, lower_level, upper_level, lower_raw, job_raw->upper_.get());
        });
    job->add_task(task);
  }
  job->set_callback([&, s, lower_level, lower, job_raw=job.get()]{
        lower->mark_merged_down();
        level_[s][lower_level].immutables.pop_backs_if([&](std::shared_ptr<PmemTable>& t) {
              return t->is_merged_down();
            });
        job_raw->upper_ = nullptr;
      });
  mgr->enqueue(job);
}

void brdb::run_log_structured_compaction_task(ThreadData* td, const int s, const unsigned long rmask,
                                              const int lower_level,
                                              const int upper_level,
                                              PmemTable* lower,
                                              PmemTable* upper) {
  // Do compaction
  upper->log_structured_compaction(td, rmask, lower_level, lower);
}

void brdb::enq_manual_compaction(const int s, const int level, JobManager* mgr) {
  if (level < 0 && kMemTableSize >= 0) {
    enq_manual_mem_compaction(s, mgr);
  } else if (level >= 0 && kPmemTableSize[level] >= 0) {
    enq_manual_pmem_compaction(s, level, mgr);
  }
}

void brdb::enq_manual_mem_compaction(const int s, JobManager* mgr) {
  if (kNumPmemLevels < 1) return;
  // immutables were automatically enqueued.
  // enqueue the current mutable table of this level
  auto mut = mem_[s].table;
  if (mut) {
    std::unique_lock<std::shared_mutex> lk(mem_[s].mu);
    if (mut == mem_[s].table && mut->size() > 0) {
      if (mut->cas_mark_full() == 0) {
        auto old = std::move(mut);
        mut = new_mem_table(s, old);
#ifdef BR_LOG_IUL
        mut->init_shortcut_IUL(lpop[s]);
#endif
        mut->set_seq_order(memtable_seq_[s].fetch_add(1));
        mem_[s].immutables.push_front(old);
        std::atomic_store(&(mem_[s].table), mut);
        // immutable ordering
        // immutables.push_front should be done inside a critical section
        auto future_pmem = old->get_future_pmem_table();
        future_pmem->set_seq_order(old->seq_order());
#ifndef BR_LOG_IUL
        future_pmem->init(ipop[s][0]);
#else
        future_pmem->init(lpop[s]);
#endif
        future_pmem->set_shard(s);
        future_pmem->cas_mark_full();
        level_[s][0].immutables.push_front(future_pmem);
        lk.unlock();
        enq_mem_compaction(s, old, mgr);
      }
    }
  }
}

void brdb::enq_manual_pmem_compaction(const int s, const int level, JobManager* mgr) {
  // immutables were automatically enqueued.
  // enqueue the current mutable table of this level
  auto mut = level_[s][level].table;  // a mutable table
  if (mut && mut->size() > 0 && mut->cas_mark_full() == 0) {
assert(level!=0);
    auto old = std::move(mut);
    mut = new_pmem_table(s, level, old);
    level_[s][level].immutables.push_front(old);
    std::atomic_store(&(level_[s][level].table), mut);
    enq_pmem_compaction(s, level, old, mgr);
  }
}
