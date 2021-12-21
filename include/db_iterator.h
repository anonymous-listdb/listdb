#ifndef BR_DB_ITERATOR_H_
#define BR_DB_ITERATOR_H_

#include "pmemtable.h"

#ifdef BR_CASCADE

class casc_pmem_level_getter;

class casc_mem_level_getter {
 public:
  using TableIter = casc_mem_table_iterator;
  using Key = TableIter::Key;
  using Node = TableIter::Node;
  //using UpperTable = PmemTable;  // -> TableIter::Table::UpperTable
  using UpperTableIter = casc_pmem_table_iterator;
  using UpperLevelGetter = casc_pmem_level_getter;
  //using NodeCmp = casc_mem_table_iterator::NodeCmp;

 public:
  casc_mem_level_getter(TableIter* table_iter);
  //void seek(const Key& key, const uintptr_t pred_uintptr = 0);
  void seek(const Key& key, const Node* pred = NULL);
  bool valid();
  int cmp();
  const Node* node() { return iter_->node(); }
  //const Node* shortcut() { return iter_->shortcut(); }
  uint64_t pmem_shortcut_offset(const int r);
  MemTable* table_ptr() { return iter_->table_ptr(); }
  UpperLevelGetter* new_upper_level_getter(const int r);
  //TableIter* table_iter();
  void get_value(std::string* out_str);

 private:
  //const int r_;  // region
  std::unique_ptr<TableIter> iter_;
};

class casc_pmem_level_getter {
 public:
  using Key = PmemTable::Key;
  using Node = PmemTable::Node;
  //using NodeCmp = PmemTable::NodeCmp;
  using TableIter = casc_pmem_table_iterator;

 public:
  casc_pmem_level_getter(const int r, TableIter* table_iter);
  void seek_with_shortcut_offset(const Key& key, const uint64_t pred_offset);
  void seek(const Key& key, const Node* pred = NULL);
  bool valid();
  int cmp();
  void get_value(std::string* out_str);
  const Node* node() { return iter_->node(); }
  const Node* shortcut() { return iter_->shortcut(); }
  //uint64_t pmem_shortcut_offset(const int r) { return iter_->pmem_shortcut_offset(r); }

 private:
  const int r_;  // region
  std::unique_ptr<TableIter> iter_;
};

class casc_mem_to_l0_getter {
 public:
  using MemLevelGetter = casc_mem_level_getter;
  //using PmemTableIter = casc_pmem_table_iterator;
  using PmemLevelGetter = casc_pmem_level_getter;
  using Key = MemLevelGetter::Key;
  //using MemNode = MemTable::Node;
  //using PmemNode = PmemTable::Node;

 public:
  casc_mem_to_l0_getter(const int region, MemLevelGetter* mem_getter);
  //casc_mem_to_l0_getter(std::shared_ptr<MemTable> memtable);
  void seek(const Key& key);
  bool valid();
  int cmp();
  bool is_not_shortcut();
  void get_value(std::string* out_str);
  void set_l1(std::shared_ptr<PmemTable> l1) { l1_wp_ = l1; }

 private:
  const int r_;
  std::unique_ptr<MemLevelGetter> mem_getter_;
  std::unique_ptr<PmemLevelGetter> pmem_getter_;
  std::weak_ptr<PmemTable> l1_wp_;
};

// Shortcut version 2
// Create when read
// every node has shortcut information
#if 0
class casc_pmem_level_getter_create_shortcuts {
  using Key = PmemTable::Key;
  using Node = PmemTable::Node;
  //using NodeCmp = PmemTable::NodeCmp;

 public:
  casc_pmem_level_getter_create_shortcuts(const int region, std::shared_ptr<PmemTable> b)
      : r_(region), table_a_(nullptr), table_b_(b) {
    iter_a_ = nullptr;
    iter_b_ = std::make_unique<casc_pskiplist_iterator>(table_b_->skiplist(), r_);
  }

  void seek(const Key& key) {
    Node* pred = NULL;

    while (true) {
      if (iter_a_ && iter_a_->valid() && iter_a_->pred()) {
        auto pred_a = iter_a_->pred();
        iter_b_->seek_two(key, pred, pred_a->key());
        if (iter_b_->is_ready_new_shortcut()) {
          assert(pred_a->key() != Node::head_key());
//flogf(stderr, "Setting shortcut!");
          pred_a->set_shortcut(iter_b_->pred_a_moff());
        }
      } else {
        iter_b_->seek(key, pred);
      }

      if (!iter_b_->valid() || iter_b_->cmp() != 0) {
        auto next_wptr = table_b_->next();
        if (auto next = next_wptr.lock()) {
          table_a_ = std::move(table_b_);
          table_b_ = std::move(next);
          pred = iter_b_->shortcut();
if (pred) flogf(stderr,"key: %zu , shortcut: %p = %zu", key, pred, pred->key());
          iter_a_ = std::move(iter_b_);
          iter_b_ = std::make_unique<casc_pskiplist_iterator>(table_b_->skiplist(), r_);
          continue;
        }
      }
      break;
    }
  }

  bool valid() { return iter_b_->valid(); }
  int cmp() { return iter_b_->cmp(); }
  const Node* node() { return iter_b_->node(); }

 private:
  const int r_;
  std::shared_ptr<PmemTable> table_a_;
  std::shared_ptr<PmemTable> table_b_;
  std::unique_ptr<casc_pskiplist_iterator> iter_a_;
  std::unique_ptr<casc_pskiplist_iterator> iter_b_;
};
#endif
#endif  // BR_CASCADE

#endif  // BR_DB_ITERATOR_H_
