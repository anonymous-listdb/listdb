#pragma once

#include <deque>
#include <map>
#include <memory>
#include <unordered_set>

#include "jobs/job.h"
#include "jobs/worker.h"

class Job;
class Worker;

class JobManager {
 public:
  JobManager(const int num_workers, const double high_priority_rate = 0.8);
  //void config_workers(const int num_workers, ThreadData* tds[]);
  void start();
  void stop();
  void wait();
  //bool idle();
  void join();
  bool enqueue(std::shared_ptr<Job> job);
  //void complete(std::shared_ptr<Job> job);
  void complete_task(std::shared_ptr<Task> task);
  ThreadData* worker_td(const int id);

  bool ready() { return ready_.load(); }
  //void busy_wait_for_ready_state();
  std::deque<std::shared_ptr<Job>> steal_pending_jobs();

 private:
  static bool jobPriorityComparator(const std::shared_ptr<Job>& a, const std::shared_ptr<Job>& b);
  void start_workers();
  void wrapup_running_jobs();
  void stop_workers();
  void job_process_loop();

 private:
  const int num_workers_;
  int num_high_priority_workers_;
  bool stop_;
  std::deque<std::shared_ptr<Job>> jq_;  // job submission queue
  std::deque<std::shared_ptr<Job>> pq_;
  std::unordered_set<std::shared_ptr<Job>> running_jobs_;
  std::unordered_set<std::shared_ptr<Job>> aborted_jobs_;
  std::deque<Task*> tcq_;  // task completion queue
  //std::deque<std::shared_ptr<Job>> cq_;  // job completion queue

  static bool workerComparator(Worker* a, Worker* b);
  std::vector<std::shared_ptr<Worker>> workers_;
  //std::vector<std::shared_ptr<Worker>> workers_[kMaxNumRegions];
  //std::vector<std::shared_ptr<Worker>> non_bound_workers_;
  //std::vector<std::shared_ptr<Worker>> high_priority_workers_;
  std::map<int, std::weak_ptr<Worker>> id_to_worker_;

  // Thread-unsafe
  std::unordered_map<Task*, Worker*> task_to_worker_;
  std::unordered_map<Worker*, int> worker_tasks_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::condition_variable wait_cv_;

  std::thread main_thread_;

  std::atomic<bool> ready_;
};
