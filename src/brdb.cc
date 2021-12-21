#include "brdb.h"

#include <cstdio>
#include <cstring>
#include <shared_mutex>
#include <thread>

#include <numa.h>

#include "util.h"
#include "pmem.h"

brdb::brdb(const DBConf& conf)
    : conf_(conf),
      stop_(false) {
  common_init_global_variables(conf);

  printf("======= DB configuration =======\n");
  printf("num_shards: %d\n", kNumShards);
  printf("num_regions: %d\n", kNumRegions);
  printf("memtable size: %ld\n", kMemTableSize);
  printf("DRAM: %zu (%zu memtable+immutables)\n", kDRAMSizeTotal, kDRAMSizeTotal / kMemTableSize);
  printf("num_workers: %d\n", kNumWorkers);
  printf("Logging: %s\n", kLoggingModeString);
  printf("num_pmem_levels: %d\n", kNumPmemLevels);
  printf("L0->L1 zipper?: %d\n", (kNumLogUnifiedLevels >= 2));
  printf("-------------------------------\n");

  init_pmem_pools(conf_.use_existing);

  // *** Init Shards ***
  for (int i = 0; i < kNumShards; i++) {
    // Init Log
    log_[i] = new Log();
    log_[i]->init(i);
    // Init ValueLog
    vlog_[i] = new ValueLog();
    vlog_[i]->init(i);
    // Init MemTable
    memtable_seq_[i].store(1);
    mem_[i].table = new_mem_table(i);
    mem_[i].table->set_seq_order(memtable_seq_[i].fetch_add(1));
    // Init PmemLevels
    for (int j = 1; j < kNumPmemLevels; j++) {  // FOR TEST
    //for (int j = 0; j < kNumPmemLevels; j++) {
      level_[i][j].table = new_pmem_table(i, j);
    }
  }
  std::atomic_thread_fence(std::memory_order_acquire);

  // *** Launch Background Threads ***
  // Init Compaction Job Manager
  job_mgr_ = new JobManager(kNumWorkers);
  job_mgr_->start();
  int num_avail_cores = sysconf(_SC_NPROCESSORS_ONLN);
  for (int i = 0; i < 2; i++) {
    backup_mgr_[i] = new JobManager(num_avail_cores);
    backup_mgr_[i]->start();
  }

  counter = new Counters[num_avail_cores];

  // Reference Time Point
  tp_begin_ = std::chrono::steady_clock::now();
  // Launch Periodic Compaction Thread
  if (conf_.periodic_compaction_mode != 0) {
    periodic_compaction_thread_ = std::thread(std::bind(&brdb::periodic_compaction_loop, this));
  }
  // Launch Performance Monitor Thread
  if (conf_.performance_monitor_mode != 0) {
    perfmon_thread_ = std::thread(std::bind(&brdb::perfmon_loop, this));
  }
}

brdb::brdb() : brdb(DBConf()) { }

brdb::~brdb() {
  // Stop compaction worker threads
  if (job_mgr_) {
    job_mgr_->stop();
  }
  for (int i = 0; i < 2; i++) {
    if (backup_mgr_[i]) backup_mgr_[i]->stop();
  }
  for (int i = 0; i < 2; i++) {
    if (backup_mgr_[i]) {
      backup_mgr_[i]->join();
    }
  }
  if (job_mgr_) {
    job_mgr_->join();
  }
  stop_.store(true);
  if (periodic_compaction_thread_.joinable()) {
    periodic_compaction_thread_.join();
  }
  if (perfmon_thread_.joinable()) {
    perfmon_thread_.join();
  }
}

void brdb::register_client(const int cid, ThreadData* td) {
  cl_td_[cid] = td;
}

void brdb::periodic_compaction_loop() {
  while (!stop_.load()) {
    for (int i = 0; i < kNumShards; i++) {
      for (int j = -1; j < kNumPmemLevels - 1; j++) {
        enq_manual_compaction(i, j, job_mgr_);
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

void brdb::perfmon_write_message(std::string msg) {
  std::unique_lock<std::mutex> lk(perfmon_mu_);
  perfmon_msg_buf_.swap(msg);
  lk.unlock();
  perfmon_cv_.notify_one();
}

void brdb::perfmon_loop() {
  auto tp_last = tp_begin_;
  size_t mem_compaction_cnt_last = 0;
  size_t L0_compaction_cnt_last = 0;
  size_t put_cnt_last = 0;
  size_t get_cnt_last = 0;
  struct TableCount table_cnt;
  while (!stop_.load()) {
    std::unique_lock<std::mutex> lk(perfmon_mu_);
    if (perfmon_cv_.wait_for(lk, std::chrono::milliseconds(1000), [&]{ return !perfmon_msg_buf_.empty(); })) {
      std::string msg;
      msg.swap(perfmon_msg_buf_);
      lk.unlock();
      fprintf(stdout, "= TIMESTAMP: %.3lf - msg: %s\n", timestamp_double(), msg.c_str());
    } else {
      lk.unlock();
    }
    auto tp_now = std::chrono::steady_clock::now();
    std::chrono::duration<double> dur = tp_now - tp_last;

    // MemTable Compaction
    // L0 Compaction
    size_t mem_compaction_cnt_now = 0;
    size_t L0_compaction_cnt_now = 0;
    for (int i = 0; i < kNumWorkers; i++) {
      mem_compaction_cnt_now += job_mgr_->worker_td(i)->mem_compaction_cnt;
      L0_compaction_cnt_now += job_mgr_->worker_td(i)->pmem_compaction_cnt[0];
    }
    int num_avail_cores = sysconf(_SC_NPROCESSORS_ONLN);
    for (int i = 0; i < num_avail_cores; i++) {
      for (int j = 0; j < 2; j++) {
        mem_compaction_cnt_now += backup_mgr_[j]->worker_td(i)->mem_compaction_cnt;
        L0_compaction_cnt_now += backup_mgr_[j]->worker_td(i)->pmem_compaction_cnt[0];
      }
    }
    size_t mem_compaction_cnt_inc = mem_compaction_cnt_now - mem_compaction_cnt_last;
    size_t L0_compaction_cnt_inc = L0_compaction_cnt_now - L0_compaction_cnt_last;
    double mem_compaction_throughput = (double) mem_compaction_cnt_inc / dur.count();
    double L0_compaction_throughput = (double) L0_compaction_cnt_inc / dur.count();

    // Queries
    size_t put_cnt_now = 0;
    size_t get_cnt_now = 0;
    for (int i = 0; i < num_avail_cores; i++) {
      put_cnt_now += counter[i].put_cnt;
      get_cnt_now += counter[i].get_cnt;
    }
    size_t put_cnt_inc = put_cnt_now - put_cnt_last;
    size_t get_cnt_inc = get_cnt_now - get_cnt_last;
    double put_mops = (double) put_cnt_inc/dur.count()/1000/1000;
    double get_mops = (double) get_cnt_inc/dur.count()/1000/1000;

    get_table_state(&table_cnt);

    fprintf(stdout, "[ TIMESTAMP: %.3lf ]    PUT: %.3lf    GET: %.3lf    MEM_COMPACTION: %.3lf    L0_COMPACTION: %.3lf    | MEM: %zu  L0: %zu  L1: %zu\n", timestamp_double(), put_mops, get_mops, mem_compaction_throughput/1000/1000, L0_compaction_throughput/1000/1000, table_cnt.mem_cnt, table_cnt.pmem_cnt[0], table_cnt.pmem_cnt[1]);

    tp_last = tp_now;
    mem_compaction_cnt_last = mem_compaction_cnt_now;
    L0_compaction_cnt_last = L0_compaction_cnt_now;
    put_cnt_last = put_cnt_now;
    get_cnt_last = get_cnt_now;
  }
}

double brdb::timestamp_double() {
  auto curr = std::chrono::steady_clock::now();
  std::chrono::duration<double> dur = curr - tp_begin_;
  return dur.count();
}

void brdb::table_stats() {
  int imm_cnt = 0;
  int l0_cnt = 1;
  int l1_cnt = 1;
  for (int i = 0; i < conf_.num_shards; i++) {
    imm_cnt += mem_[i].immutables.size();
    l0_cnt += level_[i][0].immutables.size();
    l1_cnt += level_[i][0].immutables.size();
  }
}

void brdb::get_table_state_string(std::string* out_str) {
  // MemTable
  size_t mem_cnt = 0;
  size_t pmem_cnt[kNumPmemLevels] = { 0 };
  for (int s = 0; s < kNumShards; s++) {
    auto mem = std::atomic_load(&mem_[s].table);
    if (mem && mem->size() > 0) {
      mem_cnt++;
    }
    mem_cnt += mem_[s].immutables.size();
  }
  // Pmem Levels
  for (int l = 0; l < kNumPmemLevels; l++) {
    for (int s = 0; s < kNumShards; s++) {
      auto pmem = std::atomic_load(&level_[s][l].table);
      if (pmem && pmem->size() > 0) {
        pmem_cnt[l]++;
      }
      pmem_cnt[l] += level_[s][l].immutables.size();
    }
  }
  out_str->clear();
  char wbuf[200];
  sprintf(wbuf, "MemTable: %zu\n", mem_cnt);
  out_str->append(wbuf);
  for (int l = 0; l <kNumPmemLevels; l++) {
    sprintf(wbuf, "Level %d: %zu\n", l, pmem_cnt[l]);
    out_str->append(wbuf);
  }
}

void brdb::get_table_state(struct TableCount* out) {
  // MemTable
  size_t mem_cnt = 0;
  size_t pmem_cnt[kNumPmemLevels] = { 0 };
  for (int s = 0; s < kNumShards; s++) {
    auto mem = std::atomic_load(&mem_[s].table);
    if (mem && mem->size() > 0) {
      mem_cnt++;
    }
    mem_cnt += mem_[s].immutables.size();
  }
  // Pmem Levels
  for (int l = 0; l < kNumPmemLevels; l++) {
    for (int s = 0; s < kNumShards; s++) {
      auto pmem = std::atomic_load(&level_[s][l].table);
      if (pmem && pmem->size() > 0) {
        pmem_cnt[l]++;
      }
      pmem_cnt[l] += level_[s][l].immutables.size();
    }
  }
  out->mem_cnt = mem_cnt;
  for (int l = 0; l < kNumPmemLevels; l++) {
    out->pmem_cnt[l] = pmem_cnt[l];
  }
}

void brdb::perf_stats() {
  //job_mgr_->busy_wait_for_ready_state();

  for (int i = 0; i < kNumWorkers; i++) {
    auto td = job_mgr_->worker_td(i);
    fprintf(stdout, "cw_%d: avg_mem_compaction_throughput: %.3lf Mops/s\n", i, (double) td->mem_compaction_cnt/td->mem_compaction_dur/1000/1000);
  }
}

void brdb::wait_compaction() {
  job_mgr_->wait();
}

void brdb::stabilize() {
  //for (int i = 0; i < kNumShards; i++) {
  //  for (int j = -1; j < kNumPmemLevels - 1; j++) {
  //    enq_manual_compaction(i, j, job_mgr_);
  //  }
  //}
  //job_mgr_->wait();
  fprintf(stdout, "Stabilizing DB...\n");
  //return;
  JobManager* my_mgr[2];
  for (int i = 0; i < 2; i++) {
    //JobManager* mgr = new JobManager(num_avail_cores, 0.0);
    my_mgr[i] = backup_mgr_[i];
    //my_mgr->start();
  }

  auto jobs = job_mgr_->steal_pending_jobs();
  flogf(stdout, "Stole %zu jobs from main workers", jobs.size());

  bool low_ind = 0;
  int low = -1;
  int high = kNumPmemLevels - 1;
  for (auto& job : jobs) {
    my_mgr[(job->level()!=low)^low_ind]->enqueue(job);
  }
  jobs.clear();

  // level range: [low, high)
  while (low < high) {
    for (int l = low; l < high; l++) {
      for (int s = 0; s < kNumShards; s++) {
//fprintf(stderr, "MANUAL COMP!");
        enq_manual_compaction(s, l, my_mgr[(l!=low)^low_ind]);
      }
    }

    my_mgr[low_ind]->wait();
    flogf(stdout, "Done L%d -> L%d", low, low + 1);
    low++;
    low_ind ^= 1;
  }
  my_mgr[low_ind]->wait();

  job_mgr_->wait();
  //flogf(stdout, "Main Workers done all jobs");

  flogf(stdout, "Backup Workers done all jobs");
  fprintf(stdout, "DB is stabilized!\n");

  //my_mgr[0]->stop();
  //my_mgr[1]->stop();
  //my_mgr[0]->join();
  //my_mgr[1]->join();
  //delete my_mgr[0];
  //delete my_mgr[1];
}
