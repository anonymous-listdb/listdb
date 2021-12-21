#pragma once

#include "jobs/job_manager.h"
#include "jobs/task.h"
#include "pmemtable.h"

class Task;

class Job {
 public:
  Job(const int level, const int seq_num, const int size);

  void set_job_manager(JobManager* job_mgr);
  void set_before(std::function<void()> before_fn);
  void set_callback(std::function<void()> callback_fn);
  void add_task(std::shared_ptr<Task> task);
  int fetch_add_completion(const int nc);

  void before();
  void callback();

  int level();
  int seq_num();
  size_t size();
  JobManager* job_mgr();
  std::shared_ptr<Task> task(const int i);

  void set_args(void* args);
  void* get_args();

 protected:
  int level_;  // table level
  int seq_num_;
  size_t size_;  // num tasks
  void* args_ = nullptr;
  std::function<void()> before_fn_;
  std::function<void()> job_cb_fn_;
  std::vector<std::shared_ptr<Task>> tasks_;

  std::atomic<JobManager*> job_mgr_;
  std::atomic<int> num_comp_tasks_;

  bool bootstrapped_ = false;
  std::mutex mu_;

 public:
  std::shared_ptr<PmemTable> upper_ = nullptr;
};
