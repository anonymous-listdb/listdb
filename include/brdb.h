#ifndef BRDB_H_
#define BRDB_H_

#include <condition_variable>
#include <functional>
#include <queue>
#include <deque>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <vector>

#include "common.h"
#include "ds/nbr_stack.h"
#include "jobs/job_manager.h"
#include "log.h"
#include "memtable.h"
#include "pmemtable.h"
#include "table_queue.h"
#include "value_log.h"

using std::string;
class brdb {
  struct TableCount {
    size_t mem_cnt = 0;
    size_t pmem_cnt[kMaxNumPmemLevels] = { 0 };
  };
 public:
  brdb(const DBConf& conf);
  brdb();
  ~brdb();

  // Client interfaces
  void put(ThreadData* td, const std::string_view& key, const std::string_view& value);
  void put_batch(ThreadData* td, std::vector<std::pair<std::string_view, std::string_view>>& kvbatch);
  bool get(ThreadData* td, const std::string_view& key, std::string* value_out = NULL);
  void register_client(const int cid, ThreadData* td);
  double timestamp_double();
  void table_stats();
  void get_table_state_string(std::string* out_str);
  void get_table_state(struct TableCount* out);
  void perf_stats();
  void wait_compaction();
  void stabilize();

  bool get_cascade(ThreadData* td, const std::string_view& sv_key, std::string* value_out);
  bool get_cascade_l0(ThreadData* td, const std::string_view& sv_key, std::string* value_out,
    UINT64_node* mem_pred_a, UINT64_pnode* shortcut, std::shared_ptr<MemTable>& mem);

  bool scan(ThreadData* td, const std::string_view& key, const uint64_t len, std::vector<void*>* result);

  void perfmon_write_message(std::string msg);

 private:
  // Write functions
  // value can be either integer value or offset to value log pointer
  void write_to_mem(ThreadData* td, const int s, const std::string_view& key, const uint64_t value);
  void write_to_pmem(ThreadData* td, const int s, const std::string_view& key, const uint64_t value);
  uint64_t write_log(ThreadData* td, const int s, const std::string_view& key, const uint64_t value);
  uint64_t store_value(ThreadData* td, const int s, const std::string_view& value);
  // Read functions
  bool read_from_mem(ThreadData* td, const int s, const std::string_view& key, std::string* value_out);
  bool read_from_imms(ThreadData* td, const int s, const std::string_view& key, std::string* value_out);
  bool read_from_pmem_level(ThreadData* td, const int s, const int level, const std::string_view& key, std::string* value_out);

bool read_from_mem_casc(ThreadData* td, const int s, const std::string_view& key, std::string* value_out);
bool read_from_imms_casc(ThreadData* td, const int s, const std::string_view& key, std::string* value_out);
bool read_from_pmem_level_casc(ThreadData* td, const int s, const int level, const std::string_view& key, std::string* value_out);

 private:
  // Table Management Functions
  std::shared_ptr<MemTable> get_writable_memtable(ThreadData* td, const int s, const size_t my_size, JobManager* mgr);
  std::shared_ptr<PmemTable> get_writable_pmemtable(ThreadData* td, const int s, const int level, const size_t write_size, JobManager* mgr);
  std::shared_ptr<MemTable> new_mem_table(const int s, std::shared_ptr<MemTable> old = nullptr);
  std::shared_ptr<PmemTable> new_pmem_table(const int s, const int level, std::shared_ptr<PmemTable> old = nullptr);
  // Compaction functions
  void cworker_loop(const int cworker_id);
  void enq_mem_compaction(const int s, std::shared_ptr<MemTable>& imm, JobManager* mgr);
  //void run_mem_compaction_task(ThreadData* td, const int s, std::shared_ptr<MemTable> imm);
  void run_mem_compaction_task(ThreadData* td, const int s, const unsigned long rmask, MemTable* imm, PmemTable* pmem);
  void enq_pmem_compaction(const int s, const int lower_level,
                           std::shared_ptr<PmemTable>& lower,
                           JobManager* mgr);
  void enq_zipper_compaction(const int s, const int lower_level,
                             const int upper_level,
                             std::shared_ptr<PmemTable>& lower,
                             JobManager* mgr);
  void run_zipper_compaction_task(ThreadData* td, const int s, const unsigned long rmask,
                                  const int lower_level,
                                  const int upper_level,
                                  PmemTable* lower,
                                  PmemTable* upper);
  void enq_log_structured_compaction(const int s, const int lower_level,
                                     const int upper_level,
                                     std::shared_ptr<PmemTable>& lower,
                                     JobManager* mgr);
  void run_log_structured_compaction_task(ThreadData* td, const int s, const unsigned long rmask,
                                          const int lower_level,
                                          const int upper_level,
                                          PmemTable* lower,
                                          PmemTable* upper);
  void enq_manual_compaction(const int s, const int level, JobManager* mgr);
  void enq_manual_mem_compaction(const int s, JobManager* mgr);
  void enq_manual_pmem_compaction(const int s, const int level, JobManager* mgr);

  // Periodic Compaction Loop
  void periodic_compaction_loop();
  // Performance Monitor Loop
  void perfmon_loop();

 private:
  const DBConf conf_;
  std::chrono::time_point<std::chrono::steady_clock> tp_begin_;
  std::atomic<bool> stop_;
  std::mutex mu_;  // global mutex

  // PMEM Pool and Log
  Log* log_[kMaxNumShards];
  ValueLog* vlog_[kMaxNumShards];

  template<class T>
  struct TableList {
    std::shared_ptr<T> table;
    nbr_stack<std::shared_ptr<T>> immutables;
    std::shared_mutex mu;
  };
  // DRAM Layer
  // Current memtables and immutables
  //std::vector<std::shared_ptr<MemTable>> memtable_;
  //std::vector<nbr_stack<std::shared_ptr<MemTable>>*> immutables_;
  TableList<MemTable> mem_[kMaxNumShards];
  std::atomic<uint64_t> memtable_seq_[kMaxNumShards];
  std::mutex ws_mu_[kMaxNumShards];
  std::condition_variable ws_cv_[kMaxNumShards];

  // PMEM Layer
  TableList<PmemTable> level_[kMaxNumShards][kMaxNumPmemLevels];

  // Compaction Queue
  //enum TaskType {
  //  kMemTableCompactionTask,
  //  kL0CompactionTask,
  //  kL1CompactionTask,
  //  kL2CompactionTask,
  //};
  //struct TaskAndCallback {
  //  TaskType task_type;
  //  uint64_t table_seq;
  //  std::function<void(ThreadData*)> task;
  //  std::function<void()> callback;
  //  TaskAndCallback(TaskType tt, uint64_t ts, std::function<void(ThreadData*)> t, std::function<void()> td)
  //     : task_type(tt), table_seq(ts), task(t), callback(td) { }
  //};
  //using Job = std::shared_ptr<TaskAndCallback>;
  //struct pq_cmp {
  //  bool operator()(Job& lhs, Job& rhs) {
  //    return (lhs->task_type > rhs->task_type)
  //        || (lhs->task_type == rhs->task_type
  //            && lhs->table_seq > rhs->table_seq);
  //  }
  //};
  //std::priority_queue<Job, std::vector<Job>, pq_cmp> cq_;
  //std::condition_variable cq_cv_;
  //std::vector<std::thread> cw_threads_;
  //std::vector<int> cstate_;
  //std::mutex cq_mu_;

  JobManager* job_mgr_ = nullptr;
  JobManager* backup_mgr_[2];

  TableQueue<MemTable> prepared_mem_tables_;
  TableQueue<PmemTable> prepared_tables_[kMaxNumPmemLevels];

  std::thread table_server_thread_;
  std::thread periodic_compaction_thread_;

  std::string perfmon_msg_buf_;
  std::mutex perfmon_mu_;
  std::condition_variable perfmon_cv_;

  std::thread perfmon_thread_;

  //ThreadData* cw_td_[kMaxNumWorkers];  // for compaction workers
  ThreadData* cl_td_[kMaxNumClients];  // for clients
};

#endif  // BRDB_H_
