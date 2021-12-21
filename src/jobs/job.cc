#include "jobs/job.h"

Job::Job(const int level, const int seq_num, const int size)
    : level_(level), seq_num_(seq_num), size_(size), job_mgr_(NULL), num_comp_tasks_(0) {
  //num_comp_tasks_.store(0);
}

void Job::set_job_manager(JobManager* job_mgr) {
  job_mgr_.store(job_mgr);
}

void Job::set_before(std::function<void()> before_fn) {
  before_fn_ = before_fn;
}

void Job::set_callback(std::function<void()> cb_fn) {
  job_cb_fn_ = cb_fn;
}

void Job::add_task(std::shared_ptr<Task> task) {
  tasks_.push_back(task);
}

int Job::fetch_add_completion(const int nc) {
  return num_comp_tasks_.fetch_add(nc);
}

void Job::before() {
  std::lock_guard<std::mutex> guard(mu_);
  if (!bootstrapped_) {
    if (before_fn_) {
      before_fn_();
    }
    bootstrapped_ = true;
  }
}

void Job::callback() {
  if (job_cb_fn_) {
    job_cb_fn_();
  }
}

size_t Job::size() {
  return size_;
}

int Job::level() {
  return level_;
}

int Job::seq_num() {
  return seq_num_;
}

JobManager* Job::job_mgr() {
  return job_mgr_.load();
}

std::shared_ptr<Task> Job::task(const int i) {
  return tasks_[i];
}

//void Job::set_args(void* args) {
//  args_ = args;
//}

//void* Job::get_args() {
//  return args_;
//}
