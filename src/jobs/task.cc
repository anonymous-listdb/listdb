#include "jobs/task.h"

Task::Task(std::shared_ptr<Job> job, std::function<void(ThreadData*)> task_fn,
           std::function<void(ThreadData*)> cb_fn)
    : job_(job.get()), task_fn_(task_fn), cb_fn_(cb_fn) {
  //job_mgr_ = job_->job_mgr();
}

void Task::run(ThreadData* td) {
  task_fn_(td);
}

void Task::callback(ThreadData* td) {
  if (cb_fn_) {
    cb_fn_(td);
  }

  //job_->complete(this);
  //int num_comp_tasks = job_->fetch_add_completion(1);
  //if ((size_t) num_comp_tasks + 1 == job_->size()) {  // This is the last task
  //  job_mgr_->complete(job_);
  //}
}

Job* Task::job() { return job_; }
