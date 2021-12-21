#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include <numa.h>

#include "jobs/job_manager.h"
#include "jobs/task.h"

class JobManager;
class Task;

class Worker {
 public:
  Worker(const int id, JobManager* job_mgr, const int numa = -1, const int cpu = -1);
  void start();
  void stop(bool force = false);
  void wait();
  void join();
  void enqueue(std::shared_ptr<Task> task);
  size_t size();  // the number of enqueued tasks
  ThreadData* td() { return td_; }

  bool ready() { return ready_.load(); }
  bool idle();
  void busy_wait_for_ready_state();

 private:
  void set_thread_affinity();
  void task_process_loop();

 private:
  const int worker_id_;
  const int numa_;
  const int cpu_;
  JobManager* job_mgr_;
  ThreadData* td_;
  bool is_stop_ = false;
  bool force_stop_ = false;
  std::weak_ptr<Task> running_task_;
  std::queue<std::shared_ptr<Task>> q_;
  std::mutex mu_;

  std::condition_variable cv_;
  std::condition_variable wait_cv_;
  std::thread work_thread_;

  std::atomic<bool> ready_;
};
