#pragma once

#include "brdb.h"
#include "ycsb/core/core_workload.h"

namespace ycsbc {

class YCSBRunner {
 public:
   YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
              DBConf conf,
              BenchOptions bopts,
              brdb* db);
   void run_all();
 private:
  const int num_threads_;
  std::vector<CoreWorkload*> workloads_;
  DBConf conf_;
  BenchOptions bopts_;
  brdb* db_ = NULL;
};

YCSBRunner::YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
                       DBConf conf,
                       BenchOptions bopts,
                       brdb* db)
    : num_threads_(num_threads),
      workloads_(workloads),
      conf_(conf),
      bopts_(bopts),
      db_(db) {
}

void YCSBRunner::run_all() {
  size_t cnt = 0;
  for (auto& wl : workloads_) {
    WorkloadProxy wp(wl);
    BrDBClient brdb_client(&wp, num_threads_, conf_, bopts_, db_);
    brdb_client.run();
    if (cnt < workloads_.size() - 1) {
      db_->stabilize();
    }
    cnt++;
  }
}

}  // namespace ycsbc
