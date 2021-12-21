#ifndef DS_NON_BLOCKING_READ_STACK_H_
#define DS_NON_BLOCKING_READ_STACK_H_

#include <atomic>
#include <cstdlib>
#include <vector>

template<typename T>
class nbr_stack {
  struct Node {
    T t;
    std::shared_ptr<Node> next;
  };
 public:
  class Iterator {
    friend class nbr_stack;
   public:
    Iterator(nbr_stack* st) { 
      node_ = std::atomic_load_explicit(&st->head_, std::memory_order_relaxed);
    }
    bool valid() { return node_ != nullptr; }
    void next() {
      assert(valid());
      node_ = std::atomic_load_explicit(&node_->next, std::memory_order_relaxed);
    }

    T& operator*() {
      return node_->t;
    }

   private:
    std::shared_ptr<Node> node_;
  };

 public:
  nbr_stack() : head_(nullptr), num_nodes_(0) { }
  // Element access
  //auto front() const {
  //  auto p =  std::atomic_load(&head_);
  //  return p->t;
  //}
  //auto back() const {
  //  auto p = std::atomic_load(&head_);
  //  while (p) {
  //    // For p->next:
  //    //  - something -> nullptr: happen
  //    //  - nullptr -> something: not happen
  //    auto np = std::atomic_load(&p->next);
  //    if (np == nullptr) {
  //      break;
  //    }
  //    p = np;
  //  }
  //  return p->t;
  //}

  // Iterators
  Iterator begin() { return Iterator(this); }

  // Capacity
  bool empty() {
    std::shared_lock<std::shared_mutex> slk(smu_);
    auto ret = num_nodes_;
    slk.unlock();
    return ret == 0;
  }
  size_t size() {
    std::shared_lock<std::shared_mutex> slk(smu_);
    auto ret = num_nodes_;
    slk.unlock();
    return ret;
  }

  // Modifiers
  void push_front(const T& t) {
    std::unique_lock<std::shared_mutex> lk(smu_);
    auto p = std::make_shared<Node>();
    p->t = t;
    p->next = std::atomic_load(&head_);
    while (!std::atomic_compare_exchange_weak(&head_, &p->next, p)) { }
    num_nodes_++;
    lk.unlock();
  }
  void pop_backs_if(std::function<bool(T&)> crit) {
    std::unique_lock<std::shared_mutex> lk(smu_);
    std::vector<std::shared_ptr<Node>> last;
    auto p = std::atomic_load(&head_);
    std::shared_ptr<Node> pred = nullptr;
    unsigned int last_cnt = 0;
    while (p) {
      auto np = std::atomic_load(&p->next);
      if (crit(p->t)) {
        last_cnt++;
      } else {
        last_cnt = 0;
        pred = std::move(p);
      }
      p = std::move(np);
    }
    if (pred) {
      std::shared_ptr<Node> nptr = nullptr;
      std::atomic_store(&pred->next, nptr);
    } else if (last_cnt > 0) {
      head_ = nullptr;
    }
    assert(num_nodes_ >= last_cnt);
    num_nodes_ -= last_cnt;
    lk.unlock();
  }
  void pop_backs_if_with_callback(std::function<bool(T&)> crit, std::function<void(T&)> cb){
    std::unique_lock<std::shared_mutex> lk(smu_);

    std::vector<Node*> nodes;
    auto p = std::atomic_load(&head_);
    while (p) {
      if (crit(p->t)) {
        nodes.push_back(p.get());
      } else {
        nodes.clear();
      }
      p = std::atomic_load(&p->next);
    }

    size_t last_cnt = nodes.size();
    for (int i = last_cnt - 1; i >= 0; i--) {
      cb(nodes[i]->t);
      if (i > 0) {
        std::shared_ptr<Node> nptr = nullptr;
        std::atomic_store(&(nodes[i - 1]->next), nptr);
      } else {
        head_ = nullptr;
      }
    }
    assert(num_nodes_ >= last_cnt);
    num_nodes_ -= last_cnt;
    lk.unlock();

    //std::vector<std::shared_ptr<Node>> last;
    //auto p = std::atomic_load(&head_);
    //std::shared_ptr<Node> pred = nullptr;
    //unsigned int last_cnt = 0;
    //while (p) {
    //  auto np = std::atomic_load(&p->next);
    //  if (crit(p->t)) {
    //    last_cnt++;
    //  } else {
    //    last_cnt = 0;
    //    pred = std::move(p);
    //  }
    //  p = std::move(np);
    //}
    //if (pred) {
    //  // ...->O->O->X(pred)->O->...
    //  std::shared_ptr<Node> nptr = nullptr;
    //  std::atomic_store(&pred->next, nptr);
    //} else if (last_cnt > 0) {
    //  head_ = nullptr;
    //}
    //assert(num_nodes_ >= last_cnt);
    //num_nodes_ -= last_cnt;
    //lk.unlock();
  }

 private:
  std::shared_ptr<Node> head_;
  size_t num_nodes_;
  std::shared_mutex smu_;
};

#endif  // DS_NON_BLOCKING_READ_STACK_H_
