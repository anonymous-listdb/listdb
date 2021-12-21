#pragma once

#include <functional>

#include "common.h"
#include "jobs/job.h"
#include "jobs/job_manager.h"

class JobManager;
class Job;

class Task {
 public:
  Task(std::shared_ptr<Job> job, std::function<void(ThreadData*)> task_fn,
       std::function<void(ThreadData*)> cb_fn = nullptr);
  void run(ThreadData* td);
  void callback(ThreadData* td);
  Job* job();

 private:
  Job* job_;
  std::function<void(ThreadData*)> task_fn_;
  std::function<void(ThreadData*)> cb_fn_;
  //JobManager* job_mgr_;
};
