#include "db_iterator.h"

#ifdef BR_CASCADE
////////////////////////////////////////////////////////////////////
// Level Getter Impl. that iterates over tables in a given level

// For MemTable level
casc_mem_level_getter::casc_mem_level_getter(TableIter* table_iter) {
  iter_ = std::unique_ptr<TableIter>(table_iter);
}
void casc_mem_level_getter::seek(const Key& key, const Node* pred) {
//fprintf(stderr, "TEST searching key %zu\n", key);
  while (true) {
    iter_->seek(key, pred);
    if (!iter_->valid() || iter_->cmp() != 0 || iter_->node()->type() == kTypeShortcut) {
      // key doesn't match
      pred = iter_->shortcut();
      assert((pred == NULL) || (pred->type() != kTypeShortcut));
      auto next_wptr = iter_->table_ptr()->next();
      if (auto next_table = next_wptr.lock()) {
        // next memtable available
        auto next_iter = std::make_unique<TableIter>(next_table);
        iter_ = std::move(next_iter);
        continue;
      }
    }
    break;
  }
}
bool casc_mem_level_getter::valid() {
  return iter_->valid();
}
int casc_mem_level_getter::cmp() {
  return iter_->cmp();
}
//const Node* node() { return iter_->node(); }
//const Node* shortcut() { return iter_->shortcut(); }
uint64_t casc_mem_level_getter::pmem_shortcut_offset(const int r) {
  return iter_->pmem_shortcut_offset(r);
}
casc_mem_level_getter::UpperLevelGetter* casc_mem_level_getter::new_upper_level_getter(const int r) {
  auto mem_table = iter_->table_ptr();
  assert(mem_table->next().expired());
  auto l0_wp = mem_table->upper();
  if (auto l0_table = l0_wp.lock()) {
    // possible use_count()
    // 1: this ?
    // 2: this + immutables
    // 2: this + future_pmem_table_ (X), because mem_table->next().expired() == true
    // 3 + a: this + immutables + future_pmem_table_
    if (true /* FOR TEST */|| l0_table.use_count() > 1) {
      // L0 table is listed on immutable list (in use)
      auto l0_table_iter = new UpperTableIter(r, l0_table);
      return new UpperLevelGetter(r, l0_table_iter);
    }
  }
  return NULL;
}
void casc_mem_level_getter::get_value(std::string* out_str) {
  return iter_->get_value(out_str);
}

//casc_mem_level_getter::TableIter* casc_mem_level_getter::table_iter() {
//  return iter_.get();
//}

// For PmemTable level (L0)
casc_pmem_level_getter::casc_pmem_level_getter(const int r, TableIter* table_iter)
    : r_(r) {
  iter_ = std::unique_ptr<TableIter>(table_iter);
}
void casc_pmem_level_getter::seek_with_shortcut_offset(const Key& key, const uint64_t pred_offset) {
  //return seek(key, NULL);
  Node* pred = (Node*) iter_->table_ptr()->get_node_by_offset(r_, pred_offset);
  return seek(key, pred);
}
void casc_pmem_level_getter::seek(const Key& key, const Node* pred) {
  while (true) {
    iter_->seek(key, pred);
    if (!iter_->valid() || iter_->cmp() != 0 || iter_->node()->type() == kTypeShortcut) {
      // casc_pmem_table_iterator::shortcut()
      pred = iter_->shortcut();
      auto next_wptr = iter_->table_ptr()->next();
      if (auto next_table = next_wptr.lock()) {
        // next pmemtable available
        //iter_ = PmemTable::new_casc_iterator(pmemtable_);
        auto next_iter = std::make_unique<TableIter>(r_, next_table);
        iter_ = std::move(next_iter);
        continue;
      }
    }
    break;
  }
}
bool casc_pmem_level_getter::valid() {
  return iter_->valid();
}
int casc_pmem_level_getter::cmp() {
  return iter_->cmp();
}
void casc_pmem_level_getter::get_value(std::string* out_str) {
  return iter_->get_value(out_str);
}
//const Node* node() { return iter_->node(); }
//const Node* shortcut() { return iter_->shortcut(); }
//uint64_t pmem_shortcut_offset(const int r) { return iter_->pmem_shortcut_offset(r); }


////////////////////////////////////////
// class casc_mem_to_l0_getter Impl.

casc_mem_to_l0_getter::casc_mem_to_l0_getter(const int region, MemLevelGetter* mem_iter)
    : r_(region) {
  mem_getter_ = std::unique_ptr<MemLevelGetter>(mem_iter);
  pmem_getter_ = nullptr;
  assert((mem_getter_ != nullptr) ^ (pmem_getter_ != nullptr));
}

void casc_mem_to_l0_getter::seek(const Key& key) {
  // MemTable
  //bool go_L1 = false;
  mem_getter_->seek(key);
  uint64_t pred_offset = 0;
  if (!mem_getter_->valid() || mem_getter_->cmp() != 0 || mem_getter_->node()->type() == kTypeShortcut) {
    uint64_t pred_offset = mem_getter_->pmem_shortcut_offset(r_);  // Using this SLOW DOWN search
    auto l0_weak = mem_getter_->table_ptr()->upper();
    if (auto sp = l0_weak.lock()) {
      if (sp.use_count() > 1) {
        auto pmem_table_iter = new casc_pmem_table_iterator(r_, std::move(sp));
        pmem_getter_ = std::unique_ptr<casc_pmem_level_getter>(new casc_pmem_level_getter(r_, pmem_table_iter));
        mem_getter_ = nullptr;
      }
    }
  } else {
    return;
  }
  // When to use pmem_shortcut_offset?
  if (pmem_getter_) {
    pmem_getter_->seek_with_shortcut_offset(key, pred_offset);
    if (kNumPmemLevels > 1 && (!pmem_getter_->valid() || pmem_getter_->cmp() != 0 || pmem_getter_->node()->type() == kTypeShortcut)) {
      auto pred_for_l1 = pmem_getter_->shortcut();
      assert(kNumPmemLevels==2);
      auto l1_table = l1_wp_.lock();
      assert(l1_table);
      auto l1_table_iter = new casc_pmem_table_iterator(r_, std::move(l1_table));
      pmem_getter_ = std::make_unique<casc_pmem_level_getter>(r_, l1_table_iter);
      pmem_getter_->seek(key, pred_for_l1);
    } else {
      return;
    }
  } else if (kNumPmemLevels > 1) {
    assert(kNumPmemLevels==2);
    auto l1_table = l1_wp_.lock();
    assert(l1_table);
    auto l1_table_iter = new casc_pmem_table_iterator(r_, std::move(l1_table));
    pmem_getter_ = std::make_unique<casc_pmem_level_getter>(r_, l1_table_iter);
    pmem_getter_->seek_with_shortcut_offset(key, pred_offset);
  }
}

bool casc_mem_to_l0_getter::valid() {
  assert((mem_getter_ != nullptr) ^ (pmem_getter_ != nullptr));
  return (pmem_getter_) ? pmem_getter_->valid() : mem_getter_->valid();
}

int casc_mem_to_l0_getter::cmp() {
  assert((mem_getter_ != nullptr) ^ (pmem_getter_ != nullptr));
  return (pmem_getter_) ? pmem_getter_->cmp() : mem_getter_->cmp();
}

bool casc_mem_to_l0_getter::is_not_shortcut() {
  if (pmem_getter_) {
    return (pmem_getter_->node()->type() != kTypeShortcut);
  } else {
    return (mem_getter_->node()->type() != kTypeShortcut);
  }
}

void casc_mem_to_l0_getter::get_value(std::string* out_str) {
  assert(out_str != NULL);
  assert((mem_getter_ != nullptr) ^ (pmem_getter_ != nullptr));
  if (pmem_getter_) {
    return pmem_getter_->get_value(out_str);
  } else {
    return mem_getter_->get_value(out_str);
  }
}

#endif
