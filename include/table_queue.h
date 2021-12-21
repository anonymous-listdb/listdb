#ifndef TABLE_QUEUE_H_
#define TABLE_QUEUE_H_

#include <queue>
#include <mutex>

// Table queue for serving prepared tables
template<class T>
class TableQueue {
 public:
  std::shared_ptr<T> pop_front() {
    std::lock_guard<std::mutex> guard(mu_);
    if (!q_.empty()) {
      std::shared_ptr<T> table = q_.front();
      q_.pop();
      return table;
    } else {
      return nullptr;
    }
  }

 private:
  std::queue<std::shared_ptr<T>> q_;
  std::mutex mu_;
};

#endif  // TABLE_QUEUE_H_
