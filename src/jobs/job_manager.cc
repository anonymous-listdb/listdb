#include "jobs/job_manager.h"

JobManager::JobManager(const int num_workers, const double high_priority_rate) : num_workers_(num_workers) {
  stop_ = false;
  ready_.store(false);
  int id_cnt = 0;
  for (int i = 0; i < num_workers_; i++) {
    auto worker = std::make_shared<Worker>(id_cnt, this);
    workers_.push_back(worker);
    id_to_worker_.emplace(id_cnt, worker);
    worker_tasks_[worker.get()] = 0;
    //id_to_size_.emplace(id_cnt, 0);
    id_cnt++;
  }
}

void JobManager::start() {
  //FOR_ALL_WORKERS(start());
  main_thread_ = std::thread(std::bind(&JobManager::job_process_loop, this));
  std::unique_lock<std::mutex> lk(mu_);
  wait_cv_.wait(lk, [&]{ return ready_.load(); });
}

void JobManager::start_workers() {
  for (auto& w : workers_) {
    w->start();
  }
}

void JobManager::stop() {
//flogf(stdout, "STOPSTOPSTOPSTOPSTOPSTOPSTOPSTOPSTOPSTOP");
  for (auto& w : workers_) {
    w->stop();
  }
  std::unique_lock<std::mutex> lk(mu_);
  stop_ = true;
  lk.unlock();
  cv_.notify_all();
}

void JobManager::wait() {
flogf(stderr, "jq_.size() = %zu, pending = %zu, running_jobs_.size() = %zu", jq_.size(), pq_.size(), running_jobs_.size());
  cv_.notify_all();
  while (true) {
    std::unique_lock<std::mutex> lk(mu_);
    //wait_cv_.wait(lk, [&]{ return jq_.empty() && pq_.empty() && running_jobs_.empty(); });
    wait_cv_.wait(lk, [&]{ 
        return jq_.empty() && pq_.empty() && running_jobs_.empty(); });
    if (/*all workers are idle=?*/true) {
      break;
    }
  }
}

//bool JobManager::idle() {
//  std::lock_guard<std::mutex> guard(mu_);
//  return jq_.empty() && running_jobs_.empty();
//}

void JobManager::join() {
  for (auto& w : workers_) {
    w->join();
  }
  if (main_thread_.joinable()) {
    main_thread_.join();
  }
}

bool JobManager::enqueue(std::shared_ptr<Job> job) {
  std::unique_lock<std::mutex> lk(mu_);
  if (stop_) return false;
  jq_.push_back(job);
  lk.unlock();
  cv_.notify_all();
  return true;
}

//void JobManager::complete(std::shared_ptr<Job> job) {
//  std::unique_lock<std::mutex> lk(mu_);
//  cq_.push_back(job);
//  lk.unlock();
//  cv_.notify_all();
//}

void JobManager::complete_task(std::shared_ptr<Task> task) {
  std::unique_lock<std::mutex> lk(mu_);
  tcq_.push_back(task.get());
  lk.unlock();
  cv_.notify_all();
}

ThreadData* JobManager::worker_td(const int id) {
  auto& wp = id_to_worker_[id];
  if (auto sp = wp.lock()) {
    return sp->td();
  } else {
    return NULL;
  }
}

//void JobManager::busy_wait_for_ready_state() {
//  while (!ready_.load()) continue;
//}

std::deque<std::shared_ptr<Job>> JobManager::steal_pending_jobs() {
  std::unique_lock<std::mutex> lk(mu_);
  std::deque<std::shared_ptr<Job>> stolen;
  stolen.swap(pq_);
  lk.unlock();
  wait_cv_.notify_all();
  return stolen;
}
//void JobManager::job_process_loop() {
//  FOR_ALL_WORKERS(busy_wait_for_ready_state());
//  ready_.store(true);
//  while (true) {
//    std::unique_lock<std::mutex> lk(mu_);
//    cv_.wait(lk, [&]{ return stop_ || !jq_.empty() || !cq_.empty(); });
//    if (stop_) break;
//     
//    std::deque<std::shared_ptr<Job>> jobs;
//    std::deque<std::shared_ptr<Job>> comp_jobs;
//    jobs.swap(jq_);
//    for (auto& j : jobs) {
//      running_jobs_.insert(j);
//    }
//    comp_jobs.swap(cq_);
//    lk.unlock();
//
//    // Process incoming jobs
//    std::sort(jobs.begin(), jobs.end(), jobPriorityComparator);
//    for (auto& job : jobs) {
//      if (job->level() < 0) {
//        int remain = job->size();
//        int task_cnt = 0;
//        while (remain > 0) {
//          Worker* worker = NULL;
//          for (auto& w : non_bound_workers_) {
//            if (!worker || w->size() < worker->size()) {
//              worker = w.get();
//            }
//          }
//          for (auto& w : high_priority_workers_) {
//            if (!worker || w->size() < worker->size()) {
//              worker = w.get();
//            }
//          }
//          worker->enqueue(job->task(task_cnt++));
//          remain--;
//        }
//      } else {
//        int remain = job->size();
//        int task_cnt = 0;
//        //PARALLEL EXECUTION NOT WORKING
//        //while (remain >= kNumRegions) {
//        //  int non_empty_regions = 0;
//        //  for (int i = 0; i < kNumRegions; i++) {
//        //    if (workers_[i].size() > 0) {
//        //      non_empty_regions++;
//        //    }
//        //  }
//        //  if (non_empty_regions < kNumRegions) {
//        //    break;
//        //  }
//        //  for (int i = 0; i < kNumRegions; i++) {
//        //    Worker* worker = NULL;
//        //    for (auto& w : workers_[i]) {
//        //      if (!worker || w->size() < worker->size()) {
//        //        worker = w.get();
//        //      }
//        //    }
//        //    worker->enqueue(job->task(task_cnt++));
//        //    remain--;
//        //  }
//        //}
//        if (remain > 0) {
//          if (non_bound_workers_.size() > 0) {
//            while (remain > 0) {
//              Worker* worker = NULL;
//              for (auto& w : non_bound_workers_) {
//                if (!worker || w->size() < worker->size()) {
//                  worker = w.get();
//                }
//              }
//              worker->enqueue(job->task(task_cnt++));
//              remain--;
//            }
//          } else {
//            std::vector<Worker*> workers;
//            for (int i = 0; i < kNumRegions; i++) {
//              for (int j = 0; j < workers_[i].size(); j++) {
//                workers.push_back(workers_[i][j].get());
//              }
//            }
//            while (remain > 0) {
//              Worker* worker = NULL;
//              for (auto& w : workers) {
//                if (!worker || w->size() < worker->size()) {
//                  worker = w;
//                }
//              }
//              worker->enqueue(job->task(task_cnt++));
//              remain--;
//            }
//          }
//        }
//      }
//    }
//    // Process completed jobs
//    for (auto& j : comp_jobs) {
//      j->callback();
//    }
//    lk.lock();
//    for (auto& j : comp_jobs) {
//      running_jobs_.erase(j);
//    }
//    lk.unlock();
//    wait_cv_.notify_all();
//  }
//}

// Wake up when
//  1. job submission
//  2. task completion
//  3. stop signal
//  4. job migration signal
// Responsible for
//  1. task scheduling
void JobManager::job_process_loop() {

  start_workers();

  ready_.store(true);
  wait_cv_.notify_all();

  std::vector<Worker*> workers;
  for (auto& w : workers_) {
    workers.push_back(w.get());
  }

  while (true) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [&] {
          return stop_ || !jq_.empty() || !tcq_.empty();
        });
    if (stop_) {
      break;
    }
    for (auto& j : jq_) {
      pq_.push_back(j);
    }
    jq_.clear();
    //std::deque<std::shared_ptr<Job>> jobs(pq_);
    std::deque<Task*> tasks;
    tasks.swap(tcq_);
    lk.unlock();

    // Process requests

    // Task Completion
    std::unordered_set<Job*> comp_jobs;
    for (auto& t : tasks) {
      auto wit = task_to_worker_.find(t);
      assert(wit != task_to_worker_.end());
      worker_tasks_[wit->second]--;
      task_to_worker_.erase(wit);
      size_t num_comps = t->job()->fetch_add_completion(1);
      if (num_comps + 1 == t->job()->size()) {
        comp_jobs.insert(t->job());
      }
    }
    for (auto& j : comp_jobs) {
      j->callback();
    }
    lk.lock();
    auto iter = running_jobs_.begin();
    while (iter != running_jobs_.end()) {
      if (comp_jobs.find((*iter).get()) != comp_jobs.end()) {
        iter = running_jobs_.erase(iter);
      } else {
        iter++;
      }
    }
    lk.unlock();

    // Job Schedule
    //std::sort(jobs.begin(), jobs.end(), jobPriorityComparator);
    std::sort(workers.begin(), workers.end(), [&](const auto& a, const auto& b) {
          return worker_tasks_[a] < worker_tasks_[b];
        });
    int idle_cnt = 0;
    for (auto& w : workers) {
      if (worker_tasks_[w] == 0) {
        idle_cnt++;
      }
    }
    lk.lock();
    std::sort(pq_.begin(), pq_.end(), jobPriorityComparator);
    std::vector<std::shared_ptr<Job>> jobs;
    for (int i = 0; i < idle_cnt && !pq_.empty(); i++) {
      jobs.push_back(pq_.front());
      pq_.pop_front();
    }
    running_jobs_.insert(jobs.begin(), jobs.end());
    lk.unlock();

    // Schedule all tasks of a job at least one worker is idle
    auto it = jobs.begin();
    auto worker_iter = workers.begin();
    while (it != jobs.end()) {
      auto& job = *it;
      auto& w = *worker_iter;
      job->set_job_manager(this);
      w->enqueue(job->task(0));
      task_to_worker_[job->task(0).get()] = w;
      worker_tasks_[w]++;
      it++;
    }

    wait_cv_.notify_all();
  }
flogf(stderr, "TERMINATING!");

  wrapup_running_jobs();

  stop_workers();
}

void JobManager::wrapup_running_jobs() {
  // Wrapping up remaining tasks before stop
  // if there are running jobs, wait
  if (!jq_.empty()) {
    for (auto& j : jq_) {
      pq_.push_back(j);
    }
    jq_.clear();
  }
  if (!running_jobs_.empty()) {
    while (true) {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [&] { return !tcq_.empty(); });
      std::deque<Task*> tasks;
      tasks.swap(tcq_);
      lk.unlock();

      std::unordered_set<Job*> comp_jobs;
      for (auto& t : tasks) {
        auto wit = task_to_worker_.find(t);
        assert(wit != task_to_worker_.end());
        worker_tasks_[wit->second]--;
        task_to_worker_.erase(wit);
        size_t num_comps = t->job()->fetch_add_completion(1);
        if (num_comps + 1 == t->job()->size()) {
          comp_jobs.insert(t->job());
        }
      }
      //lk.lock();
      auto iter = running_jobs_.begin();
      while (iter != running_jobs_.end()) {
        if (comp_jobs.find((*iter).get()) != comp_jobs.end()) {
          running_jobs_.erase(iter);
        }
        iter++;
      }
      //std::erase_if(running_jobs_, [&comp_jobs](auto const& sp) {
      //      return comp_jobs.contains(sp.get());
      //    });
      //lk.unlock();
      if (running_jobs_.size() == 0) {
        break;
      }
      //lk.unlock();
    }
  }
}

void JobManager::stop_workers() {
  for (auto& w : workers_) {
    w->stop();
  }
}

bool JobManager::jobPriorityComparator(const std::shared_ptr<Job>& a, const std::shared_ptr<Job>& b) {
  return (a->level() < b->level())
      || (a->level() == b->level() && a->seq_num() > b->seq_num());
}

bool JobManager::workerComparator(Worker* a, Worker* b) {
  if (b == NULL) {
    return true;
  }
  return a->size() < b->size();
}
