#ifndef PMEMTABLE_H_
#define PMEMTABLE_H_

#include <atomic>
#include <functional>
#include <map>
#include <memory>

//#include "arena.h"
#include "common.h"
#include "memtable.h"
#include "ds/lockfree_pskiplist.h"

class PmemTable {
 public:
#ifndef BR_STRING_KV
  using Key = uint64_t;
  using Node = UINT64_pnode;
  using NodeCmp = UINT64_pcmp;
#else
  using Key = std::string_view;
  using Node = VARSTR_pnode;
  using NodeCmp = VARSTR_pcmp;
#endif

 public:
  PmemTable(std::shared_ptr<PmemTable> next_table = nullptr);
  void init(PMEMobjpool* pops[]);
  void add(ThreadData* td, const int r, const Key& key, const uint64_t value);
  bool get(ThreadData* td, const Key& key, std::string* value_out);
  void merge_WAL(ThreadData* td, const unsigned long rmask, MemTable* other);
  void merge_IUL(ThreadData* td, const unsigned long rmask, MemTable* other);
  void set_shard(const int s) { shard_ = s; skiplist_->shard_ = s; }

  // Compaction functions.
  // Thread-safe for all
  void zipper_compaction(ThreadData* td, const unsigned long rmask, const int lower_level, PmemTable* lower);
  void log_structured_compaction(ThreadData* td, const unsigned long rmask, const int lower_level, PmemTable* lower);

  /* void ref() { ref_cnt_.fetch_add(1); } */
  /* void unref() { ref_cnt_.fetch_add(-1); } */
  size_t fetch_add_size(const size_t size);
  size_t size();
  int cas_mark_full();
  void mark_merged_down();
  bool is_merged_down();
  lockfree_pskiplist* skiplist();
  Node* get_node_by_offset(const int region, const uint64_t offset);
  Node* get_node_by_moff(const uint64_t moff) { return (Node*) skiplist_->get_node_by_moff(moff); }
  void set_seq_order(const uint64_t seq) {
    seq_order_ = seq;
    //seq_order_.store(seq);
    //fprintf(stderr, "TEST seq_order = %zu\n", seq_order_.load());
  }
  uint64_t seq_order() {
    //return seq_order_.load();
    return seq_order_;
  }
  std::weak_ptr<PmemTable> next();

  void print_debug();

 private:
  lockfree_pskiplist* skiplist_;
  //std::atomic<uint64_t> seq_order_;
  uint64_t seq_order_;
  std::atomic<size_t> size_;
  std::atomic<uint_fast32_t> ref_cnt_;
  std::atomic<int> state_;
  std::weak_ptr<PmemTable> next_;
  int shard_;
};

#if 1

class casc_pmem_table_iterator {
 public:
  using Key = casc_pskiplist_iterator::Key;
  using Node = casc_pskiplist_iterator::Node;
  using NodeCmp = casc_pskiplist_iterator::NodeCmp;

 public:
  casc_pmem_table_iterator(const int region, std::shared_ptr<PmemTable> table);
  void seek(const Key& key, const Node* pred = NULL);
  //void seek_two(const Key& key, Node* pred, const Key& key_a);
  //void seek_braided(const Key& key, Node* pred);
  void next() { iter_->next(); }
  //bool is_ready_new_shortcut();
  //uint64_t pred_a_moff();
  bool valid() const;
  int cmp();
  //Node* pred();
  Node* node() const;
  const Node* shortcut();
  std::shared_ptr<PmemTable> table() { return table_; }
  PmemTable* table_ptr() { return table_.get(); }
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
  const int r_;
  std::shared_ptr<PmemTable> table_;
  casc_pskiplist_iterator* iter_;
};

#endif

#endif  // PMEMTABLE_H_
