#ifndef MEMTABLE_H_
#define MEMTABLE_H_

#include <atomic>
#include <functional>
#include <map>

#include "arena.h"
#include "common.h"
#include "pmem.h"
#include "ds/lockfree_skiplist.h"
#include "ds/lockfree_pskiplist.h"
#include "pmemtable.h"

class casc_mem_table_iterator;
class PmemTable;

class MemTable {
#ifndef BR_STRING_KV
  typedef uint64_t Key;
  typedef struct UINT64_node Node;
  typedef struct UINT64_cmp NodeCmp;
#else
  typedef std::string_view Key;
  typedef struct VARSTR_node Node;
  typedef struct VARSTR_cmp NodeCmp;
#endif
  friend class CascadeMemTableIterator;

 public:
  //MemTable() {
  //  skiplist_ = new lockfree_skiplist();
  //  reserved_skiplist_ = new lockfree_skiplist();
  //  ref_cnt_.store(0);
  //  size_.store(0);
  //  state_.store(0);
  //}
  MemTable(std::shared_ptr<MemTable> next_table = nullptr, std::shared_ptr<PmemTable> upper_table = nullptr) {
    assert(!(next_table&&upper_table));
    if (next_table) {
      // IUL: create shortcut node here?
      skiplist_ = next_table->reserved_skiplist();
      next_ = next_table;
      upper_ = next_table->get_future_pmem_table();
    } else {
      skiplist_ = new lockfree_skiplist();
      if (upper_table) {
        upper_ = upper_table;
      }
    }
    //A  B  C
    //A' B' C'
    //A->next = B
    //A->upper = B'
    //A'->next = B'
    reserved_skiplist_ = new lockfree_skiplist();
    if (auto upper_sp = upper_.lock()) {
      future_pmem_table_ = std::make_shared<PmemTable>(upper_sp);
    } else {
      future_pmem_table_ = std::make_shared<PmemTable>(nullptr);
    }
    ref_cnt_.store(0);
    size_.store(0);
    state_.store(0);
  }

  //MemTable(std::shared_ptr<PmemTable> upper_table) {
  //  if (next_table) {
  //    skiplist_ = next_table->reserved_skiplist();
  //    next_ = next_table;
  //  } else {
  //    skiplist_ = new lockfree_skiplist();
  //    if (upper_table) {
  //      upper_ = upper_table;
  //    }
  //  }
  //  reserved_skiplist_ = new lockfree_skiplist();
  //  future_pmem_table_ = std::make_shared<PmemTable>();
  //  ref_cnt_.store(0);
  //  size_.store(0);
  //  state_.store(0);
  //}

  ~MemTable() {
    delete skiplist_;
  }

  void init_shortcut_IUL(PMEMobjpool* pops[]) {
    Node* pred = skiplist_->head();
    while (pred) {
      Node* curr = (Node*) pred->next[0].load();
      if (curr && curr->type() == kTypeShortcut) {
        uint64_t casc_dst_moff = curr->log_moff;
        int16_t r = casc_dst_moff & 0xffff;
        //int r = it->region();
        //int height = random_height(td->rnd);
        TOID(char) new_node_buf;
        size_t palloc_size = pNode::compute_alloc_size(curr->key(), curr->height);
        POBJ_ALLOC(pops[r], &new_node_buf, char, palloc_size, NULL, NULL);
        pNode* new_node = pNode::init_node(D_RW(new_node_buf), curr->key(), curr->tag(), casc_dst_moff, curr->height);
        curr->log_moff = (((uint64_t) new_node - (uint64_t) pops[r])<<16)|r;
      }
      pred = curr;
    }
  }

  void add(ThreadData* td, const Key& key, const uint64_t value, const uint64_t log_moff) {
    // Random Height
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((td->rnd.Next() % kBranching) == 0)) {
      height++;
    }

    const size_t alloc_size = Node::compute_alloc_size(key, height);
    void* buf = aligned_alloc(8, alloc_size);
    Node* node = Node::init_node((char*) buf, key, (seq_order_<<8|kTypeValue), value, height);
    node->log_moff = log_moff;

    //td->visit_cnt = 0;
    skiplist_->insert(td, node);
    // sc_node->value => node
    // sc_node->log_moff => future pmem node of the current node (log_moff)
#ifdef BR_CASCADE
    if(false && td->visit_cnt > 5) {  // FOR TEST
    if (height >= kMaxHeight - 5) {
//fprintf(stderr,"TSET visit cnt = %zu\n", td->visit_cnt);
      void* sc_buf = aligned_alloc(8, alloc_size);
      Node* sc_node = Node::init_node((char*) sc_buf, key, kTypeShortcut, (uint64_t) buf, height);
#ifdef BR_LOG_IUL
      sc_node->log_moff = log_moff;
#else
      sc_node->log_moff = 0;
#endif
      reserved_skiplist_->insert(td, sc_node);
    }
    }
#endif
  }
  bool get(ThreadData* td, const Key& key, std::string* value_out) {
    auto node = skiplist_->find(td, key);
    if (node) {
      if (value_out) {
        uint64_t value = node->value();
        value_out->assign((char*) &value, 8);
      }
      return true;
    }
    return false;
  }
  //void merge(std::shared_ptr<MemTable> other) {
  //  skiplist_->merge(other->skiplist_);
  //}
  /* void ref() { ref_cnt_.fetch_add(1); } */
  /* void unref() { ref_cnt_.fetch_add(-1); } */
  size_t fetch_add_size(const size_t size) { return size_.fetch_add(size); }
  size_t size() { return size_.load(); }
  int cas_mark_full() {
    int expected = 0;
    state_.compare_exchange_strong(expected, 1);
    return expected;
  }
  void mark_persist() { state_.store(-1); }
  bool is_persist() { return state_.load() == -1; }
  lockfree_skiplist* skiplist() { return skiplist_; }
  lockfree_skiplist* reserved_skiplist() { return reserved_skiplist_; }
  std::shared_ptr<PmemTable> get_future_pmem_table() { return future_pmem_table_; }

  void set_seq_order(const uint64_t seq_order) {
    seq_order_ = seq_order;
    //future_pmem_table_->seq_seq_order(seq_order_);
  }
  uint64_t seq_order() { return seq_order_; }
#ifdef BR_CASCADE
  //static casc_mem_table_iterator* new_casc_iterator(std::shared_ptr<MemTable> memtable) {
  //  return new casc_mem_table_iterator(memtable);
  //}
#endif
  //casc_iterator* new_iterator() {
  //  auto new_iter = new casc_iterator(skiplist_);
  //  return new_iter;
  //}
  std::weak_ptr<MemTable> next() { return next_; }
  std::weak_ptr<PmemTable> upper() { return upper_; }

 private:
  lockfree_skiplist* skiplist_;
  uint64_t seq_order_;
  std::atomic<size_t> size_;
  std::atomic<uint_fast32_t> ref_cnt_;
  std::atomic<int> state_;
  lockfree_skiplist* reserved_skiplist_;
  std::shared_ptr<PmemTable> future_pmem_table_;
  std::weak_ptr<MemTable> next_;
  std::weak_ptr<PmemTable> upper_;

 public:
  int shard_;
};

#if 1

class casc_mem_table_iterator {
 public:
  using Key = casc_skiplist_iterator::Key;
  using Node = casc_skiplist_iterator::Node;
  using NodeCmp = casc_skiplist_iterator::NodeCmp;

 public:
  casc_mem_table_iterator(std::shared_ptr<MemTable> memtable) : table_(memtable) {
    iter_ = new casc_skiplist_iterator(table_->skiplist());
  }
  void seek(const Key& key, const Node* pred = NULL) {
    iter_->seek(key, pred);
  }
  bool valid() { return iter_->valid(); }
  int cmp() { return iter_->cmp(); }
  const Node* shortcut() { return iter_->shortcut(); }
  const Node* node() { return iter_->node(); }
  const uint64_t pmem_shortcut_offset(const int region) { return iter_->pmem_shortcut_offset(region); }
  MemTable* table_ptr() { return table_.get(); }
  void get_value(std::string* out_str) {
    assert(iter_->valid());
    assert(iter_->cmp()==0);
    auto node = iter_->node();
    if (node->type() == kTypeValue) {
#ifndef BR_WISCKEY
      uint64_t value = node->value();
      out_str->assign((char*) &value, 8);  // value 8-byte
#else
      abort();
      //node->value();  // offset
#endif
    }
  }

 private:
  std::shared_ptr<MemTable> table_;
  casc_skiplist_iterator* iter_;
};

#endif  // BR_CASCADE

#endif  // MEMTABLE_H_
