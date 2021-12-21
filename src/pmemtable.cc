#include "pmemtable.h"

PmemTable::PmemTable(std::shared_ptr<PmemTable> next_table) {
  if (next_table) {
    next_ = next_table;
  }
}

void PmemTable::init(PMEMobjpool* pops[]) {
  skiplist_ = new lockfree_pskiplist(pops);
  skiplist_->init();
  ref_cnt_.store(0);
  size_.store(0);
  state_.store(0);
}

void PmemTable::add(ThreadData* td, const int r, const Key& key, const uint64_t value) {
  auto new_node = skiplist_->new_node(td, r, key, value);
  skiplist_->insert(td, r, new_node, NULL, true);
}

bool PmemTable::get(ThreadData* td, const Key& key, std::string* value_out) {
  auto node = skiplist_->find(td, td->region, key);
  if (node) {
    if (value_out) {
      uint64_t value = node->value();
      value_out->assign((char*) &value, 8);
    }
    return true;
  }
  return false;
}

void PmemTable::merge_WAL(ThreadData* td, const unsigned long rmask, MemTable* other) {
  skiplist_->merge_WAL(td, rmask, other->skiplist());
}
void PmemTable::merge_IUL(ThreadData* td, const unsigned long rmask, MemTable* other) {
  skiplist_->merge_IUL(td, rmask, other->skiplist());
}

void PmemTable::zipper_compaction(ThreadData* td, const unsigned long rmask, const int lower_level, PmemTable* lower) {
  std::bitset<kMaxNumRegions> rbs(rmask);
  skiplist_->zipper_compaction(td, rmask, lower_level, lower->skiplist());
}

void PmemTable::log_structured_compaction(ThreadData* td, const unsigned long rmask, const int lower_level, PmemTable* lower) {
  skiplist_->log_structured_compaction(td, rmask, lower_level, lower->skiplist());
}

size_t PmemTable::fetch_add_size(const size_t size) {
  return size_.fetch_add(size);
}

size_t PmemTable::size() {
  return size_.load();
}

int PmemTable::cas_mark_full() {
  int expected = 0;
  state_.compare_exchange_strong(expected, 1);
  return expected;
}

void PmemTable::mark_merged_down() {
  state_.store(-1);
}

bool PmemTable::is_merged_down() {
  return state_.load() == -1;
}

lockfree_pskiplist* PmemTable::skiplist() {
  return skiplist_;
}

PmemTable::Node* PmemTable::get_node_by_offset(const int region, const uint64_t offset) {
  return skiplist_->get_node_by_offset(region, offset);
}

std::weak_ptr<PmemTable> PmemTable::next() {
  return next_;
}

void PmemTable::print_debug() {
  skiplist_->debug_scan();
}

#if 1
//casc_pskiplist_iterator* PmemTable::new_iterator(const int region) {
//  return new casc_pskiplist_iterator(std::shared_ptr<PmemTable>(this), region);
//}

///* static */
//casc_pmem_table_iterator* PmemTable::new_casc_iterator(std::shared_ptr<PmemTable> table, const int r) {
//  return new casc_pmem_table_iterator(table, r);
//}

casc_pmem_table_iterator::casc_pmem_table_iterator(const int region, std::shared_ptr<PmemTable> table)
    : r_(region), table_(table) {
  iter_ = new casc_pskiplist_iterator(r_, table_->skiplist());
}

void casc_pmem_table_iterator::seek(const Key& key, const Node* pred) {
  iter_->seek(key, pred);
}

bool casc_pmem_table_iterator::valid() const {
  return iter_->valid();
}

int casc_pmem_table_iterator::cmp() {
  return iter_->cmp();
}

//casc_pmem_table_iterator::Node* casc_pmem_table_iterator::pred() {
//  return iter_->pred();
//}

casc_pmem_table_iterator::Node* casc_pmem_table_iterator::node() const {
  return iter_->node();
}

const casc_pmem_table_iterator::Node* casc_pmem_table_iterator::shortcut() {
  // Set from void casc_pskiplist_iterator::seek(const Key& key, const Node* pred)
  return iter_->shortcut();
}

#endif
