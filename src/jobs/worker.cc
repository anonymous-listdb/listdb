#include "jobs/worker.h"

Worker::Worker(const int id, JobManager* job_mgr, const int numa, const int cpu)
    : worker_id_(id), numa_(numa), cpu_(cpu), job_mgr_(job_mgr) {
  ready_.store(false);
}

void Worker::start() {
  work_thread_ = std::thread(std::bind(&Worker::task_process_loop, this));
}

void Worker::stop(bool force) {
  std::unique_lock<std::mutex> lk(mu_);
  is_stop_ = true;
  force_stop_ = force;
  lk.unlock();
  cv_.notify_all();
}

void Worker::wait() {
  std::unique_lock<std::mutex> lk(mu_);
  wait_cv_.wait(lk, [&]{ return running_task_.expired() && q_.empty(); });
}

bool Worker::idle() {
  std::lock_guard<std::mutex> guard(mu_);
  return running_task_.expired() && q_.empty();
}

void Worker::join() {
  if (work_thread_.joinable()) {
    work_thread_.join();
  }
}

void Worker::enqueue(std::shared_ptr<Task> task) {
  std::unique_lock<std::mutex> lk(mu_);
  q_.push(task);
  lk.unlock();
  cv_.notify_all();
}

size_t Worker::size() {
  std::lock_guard<std::mutex> guard(mu_);
  return q_.size();
}

void Worker::busy_wait_for_ready_state() {
  while (!ready_.load()) continue;
}

void Worker::set_thread_affinity() {
  if (cpu_ >= 0) {
    int cpu = cpu_ % sysconf(_SC_NPROCESSORS_ONLN);
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
  } else if (numa_ >= 0) {
    numa_run_on_node(numa_);
  }
}

void Worker::task_process_loop() {
  set_thread_affinity();
  td_ = new ThreadData();
  td_->cpu = cpu_;
  td_->numa = numa_;
  ready_.store(true);

  while (true) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [&]{ return is_stop_ || !q_.empty(); });
    if (is_stop_) {
      if (q_.empty() || force_stop_) {
        break;
      }
    }
    auto task = q_.front();
    q_.pop();
    running_task_ = task;
    lk.unlock();

    task->job()->before();
    task->run(td_);
    task->callback(td_);

    lk.lock();
    running_task_.reset();
    lk.unlock();
    job_mgr_->complete_task(task);
    wait_cv_.notify_all();
  }

  std::unique_lock<std::mutex> lk(mu_);
  std::queue<std::shared_ptr<Task>> empty_queue;
  q_.swap(empty_queue);
  lk.unlock();
  wait_cv_.notify_all();
}
