#pragma once

class TaskManager() {

 private:
  std::deque<std::shared_ptr<Task>> tq_;

  std::mutex mu_;
  std::condition_variable cv_;
};

