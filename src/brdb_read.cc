#include "brdb.h"

#include "db_iterator.h"

bool brdb::get(ThreadData* td, const std::string_view& key, std::string* value_out) {
//***
// Compute shard number
#ifndef BR_STRING_KV
  uint64_t cmp = *((uint64_t*) key.data());
#else
  uint64_t cmp = key_num(key);
#endif
  int s = cmp % kNumShards;

#ifndef BR_STRING_KV
  bool cache_hit = ht_get(s, cmp, NULL);
  if (cache_hit) {
    return true;
  }
#else
  if (ht_get(s, key, NULL)) {
    return true;
  }
#endif


  if (read_from_mem_casc(td, s, key, value_out)) {
    //counter[td->cpu].get_cnt++;
    return true;
  }
  if (read_from_imms_casc(td, s, key, value_out)) {
    //counter[td->cpu].get_cnt++;
    return true;
  }
  if (read_from_pmem_level_casc(td, s, 0, key, value_out)) {
    //counter[td->cpu].get_cnt++;
    return true;
  }
  for (int i = 1; i < kNumPmemLevels - 1; i++) {
    if (read_from_pmem_level(td, s, i, key, value_out)) {
      //counter[td->cpu].get_cnt++;
      return true;
    }
  }
  PmemTable* llt = level_[s][kNumPmemLevels-1].table.get();
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif

  bool get_result = llt->get(td, search_key, value_out);
  //counter[td->cpu].get_cnt++;
  return get_result;
}
  
thread_local uint64_t t_seq_order_last;
thread_local const mNode* t_sc_mem;
thread_local uint64_t t_sc_pmem_off;
thread_local const pNode* t_sc_pmem;
thread_local uint64_t t_sc_seq_order;
thread_local int t_sc_table_type;
thread_local int t_last_table;

bool brdb::read_from_mem_casc(ThreadData* td, const int s, const std::string_view& key, std::string* value_out) {
  auto mem = mem_[s].table;
  while (mem == nullptr) mem = mem_[s].table;
  t_seq_order_last = mem->seq_order();
  t_sc_mem = NULL;
  t_sc_pmem_off = 0;
  t_sc_pmem = NULL;
  t_last_table = -1;
  t_sc_seq_order = 0;
  t_sc_table_type = -1;
  auto it = casc_mem_table_iterator(mem);
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif
  it.seek(search_key);
  if (it.valid() && it.cmp() == 0 && it.node()->type() != kTypeShortcut) {
    if (value_out) {
      uint64_t value = it.node()->value();
      value_out->assign((char*) &value, 8);
    }
    return true;
  }
  t_sc_mem = it.shortcut();
  t_sc_pmem_off = it.pmem_shortcut_offset(td->region);
  t_sc_seq_order = t_seq_order_last;
  return false;
}

bool brdb::read_from_imms_casc(ThreadData* td, const int s, const std::string_view& key, std::string* value_out) {
  if (mem_[s].immutables.empty()) return false;
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif
  auto it = mem_[s].immutables.begin();
  const mNode* pred = NULL;
  while (it.valid()) {
    auto imm = *it;
    if (imm->seq_order() >= t_seq_order_last) {
      it.next();
      continue;
    }
    if (imm->seq_order() == t_sc_seq_order - 1) {
      pred = t_sc_mem;
    } else {
      pred = NULL;
    }
    t_seq_order_last = imm->seq_order();
    auto it2 = casc_mem_table_iterator(imm);
    it2.seek(search_key, pred);
    if (it2.valid() && it2.cmp() == 0 && it2.node()->type() != kTypeShortcut) {
      if (value_out) {
        uint64_t value = it2.node()->value();
        value_out->assign((char*) &value, 8);
      }
      return true;
    }
    t_sc_mem = it2.shortcut();
    t_sc_pmem_off = it2.pmem_shortcut_offset(td->region);
    t_sc_seq_order = t_seq_order_last;
    it.next();
  }
  return false;
}

bool brdb::read_from_pmem_level_casc(ThreadData* td, const int s, const int level, const std::string_view& key, std::string* value_out) {
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif
  if (level == 0) {
    if (level_[s][level].immutables.empty()) return false;
    std::shared_ptr<PmemTable> pmem = nullptr;
    auto it = level_[s][level].immutables.begin();
    const pNode* pred = NULL;
    while (it.valid()) {
      pmem = *it;
      if (pmem->seq_order() >= t_seq_order_last) {
        it.next();
        continue;
      }
      if (pmem->seq_order() == t_sc_seq_order - 1) {
        pred = t_sc_pmem;
        if (t_sc_table_type == -1) {
          //UINT64_pnode* pred = (UINT64_pnode*) pmem->get_node_by_offset(td->region, t_sc_pmem_off);
          //t_sc_pmem = (UINT64_pnode*) pmem->get_node_by_offset(td->region, t_sc_pmem_off);
          pred = NULL;
        }
      } else {
        pred = NULL;
      }
      t_seq_order_last = pmem->seq_order();
      auto it2 = casc_pmem_table_iterator(td->region, pmem);
      //it2.seek(search_key, t_sc_pmem);
      it2.seek(search_key, pred);
      if (it2.valid() && it2.cmp() == 0 && it2.node()->type() != kTypeShortcut) {
        if (value_out) {
          uint64_t value = it2.node()->value();
          value_out->assign((char*) &value, 8);
        }
        return true;
      }
      t_last_table = 0;
      t_sc_pmem = it2.shortcut();
//t_sc_pmem = NULL;
      t_sc_seq_order = t_seq_order_last;
      t_sc_table_type = 0;
      it.next();
    }
    return false;
  } else {
    auto pmem = level_[s][level].table;
    while (pmem == nullptr) pmem = level_[s][level].table;
    auto it2 = casc_pmem_table_iterator(td->region, pmem);
    //if (t_last_table == -1) {
    //  UINT64_pnode* pred = (UINT64_pnode*) pmem->get_node_by_offset(td->region, t_sc_pmem_off);
    //  it2.seek(search_key, pred);
    //  t_last_table = 0;
    //} else {
    //  it2.seek(search_key, t_sc_pmem);
    //}
    it2.seek(search_key, NULL);
    if (it2.valid() && it2.cmp() == 0 && it2.node()->type() != kTypeShortcut) {
      if (value_out) {
        uint64_t value = it2.node()->value();
        value_out->assign((char*) &value, 8);
      }
      return true;
    }
  }
  return false;
}

bool brdb::get_cascade(ThreadData* td, const std::string_view& sv_key, std::string* value_out) {
#if 0
  auto& key = *reinterpret_cast<const uint64_t*>(sv_key.data());
  uint64_t cmp = *((uint64_t*) sv_key.data());
  int s = cmp % kNumShards;
  auto mem = mem_[s].table;
  while (mem == nullptr) mem = mem_[s].table;

  lockfree_skiplist* skiplist = mem->skiplist();
  UINT64_node* pred = skiplist->head();
  UINT64_node* curr = NULL;
  // first MEM
  UINT64_node* pred_a = NULL;
  UINT64_node* shortcut = NULL;
  UINT64_pnode* pmem_shortcut = NULL;
  int h = pred->height;
  int cr = 1;
  for (int l = h - 1; l >= 0; l--) {
    while (true) {
      curr = pred->next[l].load();
      if ((cr = skiplist->cmp_(curr, key)) < 0) {
        if (curr->shortcut != NULL) {
          shortcut = curr->shortcut;
        }
        if (curr->pmem_shortcut != NULL) {
          pmem_shortcut = curr->pmem_shortcut;
        }
        pred = curr;
        continue;
      }
      break;
    }
  }
  pred_a = pred;
  if (curr && cr == 0) {
    uint64_t value = curr->value();
    value_out->assign((char*) &value, 8);
    return true;
  }

  // following MEMs
  while (true) {
    auto next_wptr = mem->next();
    if (auto next_table = next_wptr.lock()) {
      mem = std::move(next_table);
    } else {
      break;
    }
    uint64_t key_a = pred_a->key();
    skiplist = mem->skiplist();
    pred = (shortcut) ? shortcut : skiplist->head();
    int h = pred->height;
    int cr = 1;
    for (int l = h - 1; l >= 0; l--) {
      while (true) {
        curr = pred->next[l].load();
        if (skiplist->cmp_(curr, key_a) < 0) {
          if (h > kMaxHeight - 5) {
            pred_a->shortcut = curr;
          }
        }
        if ((cr = skiplist->cmp_(curr, key)) < 0) {
          if (curr->shortcut != NULL) {
            shortcut = curr->shortcut;
          }
          if (curr->pmem_shortcut != NULL) {
            pmem_shortcut = curr->pmem_shortcut;
          }
          pred = curr;
          continue;
        }
        break;
      }
    }
    pred_a = pred;
    if (curr && cr == 0) {
      uint64_t value = curr->value();
      value_out->assign((char*) &value, 8);
      return true;
    }
  }

  return get_cascade_l0(td, sv_key, value_out, pred_a, pmem_shortcut, mem);
#endif
  return false;
}

bool brdb::get_cascade_l0(ThreadData* td, const std::string_view& sv_key, std::string* value_out,
    UINT64_node* mem_pred_a, UINT64_pnode* shortcut, std::shared_ptr<MemTable>& mem) {
#if 0
  auto& key = *reinterpret_cast<const uint64_t*>(sv_key.data());
  uint64_t cmp = *((uint64_t*) sv_key.data());
  int s = cmp % kNumShards;
  int r = td->region;

  // still holding mem
  // search pmem
  auto l0_wptr = mem->upper();
  std::shared_ptr<PmemTable> pmem;
  UINT64_pnode* curr = NULL;
  UINT64_pnode* pred = NULL;
  UINT64_pnode* pred_a = NULL;
  lockfree_pskiplist* skiplist = NULL;
  if (auto l0_table = l0_wptr.lock()) {
    pmem = std::move(l0_table);
  } else {
    return false;
    //return get_cascade_l1(td, sv_key, value_out, mem_pred_a, shortcut, mem);
  }
  uint64_t key_a = mem_pred_a->key();
  skiplist = pmem->skiplist();
  pred = (shortcut) ? shortcut : skiplist->head(r);
  int h = pred->height;
  int cr = 1;
  for (int l = h - 1; l >= 1; l--) {
    while (true) {
      curr = (UINT64_pnode*) skiplist->offset_to_ptr(pred->next[l].load(), r);
      if (skiplist->cmp_(curr, key_a) < 0) {
        if (h > kMaxHeight - 5) {
          if (mem_pred_a->log_moff & 0xffff == r) {
            mem_pred_a->pmem_shortcut = curr;
          }
        }
      }
      if (skiplist->cmp_(curr, key) < 0) {
        if (curr->shortcut != NULL) {
          shortcut = curr->shortcut;
        }
        pred = curr;
        continue;
      }
      break;
    }
  }
  pred_a = pred;
  // braided
  {
    while (true) {
      curr = (UINT64_pnode*) skiplist->moff_to_ptr(pred->next[0].load());
      if ((cr = skiplist->cmp_(curr, key)) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
  }
  if (curr && cr == 0) {
    uint64_t value = curr->value();
    value_out->assign((char*) &value, 8);
    return true;
  }

  mem = nullptr;

  while (true) {
    cr = 1;
    auto l0_wptr = pmem->next();
    if (auto l0_table = l0_wptr.lock()) {
      pmem = std::move(l0_table);
    } else {
      return false;
      //return get_cascade_l1(td, sv_key, value_out, mem_pred_a, shortcut, mem);
    }
    uint64_t key_a = pred_a->key();
    skiplist = pmem->skiplist();
    pred = (shortcut) ? shortcut : skiplist->head(r);
    int h = pred->height;
    for (int l = h - 1; l >= 1; l--) {
      while (true) {
        curr = (UINT64_pnode*) skiplist->offset_to_ptr(pred->next[l].load(), r);
        //if (skiplist->cmp_(curr, key_a) < 0) {
        //  if (h > kMaxHeight - 5) {
        //    pred_a->shortcut = curr;
        //  }
        //}
        if (skiplist->cmp_(curr, key) < 0) {
          if (curr->shortcut != NULL) {
            shortcut = curr->shortcut;
          }
          pred = curr;
          continue;
        }
        break;
      }
    }
    pred_a = pred;
    // braided
    {
      while (true) {
        curr = (UINT64_pnode*) skiplist->moff_to_ptr(pred->next[0].load());
        if ((cr = skiplist->cmp_(curr, key)) < 0) {
          pred = curr;
          continue;
        }
        break;
      }
    }
    if (curr && cr == 0) {
      uint64_t value = curr->value();
      value_out->assign((char*) &value, 8);
      return true;
    }
  }

  return false;
#endif
  return false;
}

//bool brdb::get_cascade_l1(ThreadData* td, const std::string_view& sv_key, std::string* value_out,
//    UINT64_node* mem_pred_a, UINT64_pnode* l0_pred_a, UINT64_pnode* shortcut, std::shared_ptr<MemTable>& mem, std::shared_ptr<PmemTable>& l0) {
//  auto& key = *reinterpret_cast<const uint64_t*>(sv_key.data());
//  uint64_t cmp = *((uint64_t*) sv_key.data());
//  int s = cmp % kNumShards;
//  int r = td->region;
//
//  if ()
//
//  auto l0_wptr = mem->upper();
//  std::shared_ptr<PmemTable> pmem;
//  UINT64_pnode* curr = NULL;
//  UINT64_pnode* pred = NULL;
//  UINT64_pnode* pred_a = NULL;
//  lockfree_pskiplist* skiplist = NULL;
//  if (auto l0_table = l0_wptr.lock()) {
//    pmem = std::move(l0_table);
//  } else {
//    return false;
//    //return get_cascade_l1(td, sv_key, value_out, mem_pred_a, shortcut, mem);
//  }
//  uint64_t key_a = mem_pred_a->key();
//  skiplist = pmem->skiplist();
//  pred = (shortcut) ? shortcut : skiplist->head(r);
//  int h = pred->height;
//  int cr = 1;
//  for (int l = h - 1; l >= 1; l--) {
//    while (true) {
//      curr = (UINT64_pnode*) skiplist->offset_to_ptr(pred->next[l].load(), r);
//      if (skiplist->cmp_(curr, key_a) < 0) {
//        if (h > kMaxHeight - 5) {
//          if (mem_pred_a->log_moff & 0xffff == r) {
//            mem_pred_a->pmem_shortcut = curr;
//          }
//        }
//      }
//      if (skiplist->cmp_(curr, key) < 0) {
//        if (curr->shortcut != NULL) {
//          shortcut = curr->shortcut;
//        }
//        pred = curr;
//        continue;
//      }
//      break;
//    }
//  }
//  pred_a = pred;
//  // braided
//  {
//    while (true) {
//      curr = (UINT64_pnode*) skiplist->moff_to_ptr(pred->next[0].load());
//      if ((cr = skiplist->cmp_(curr, key)) < 0) {
//        pred = curr;
//        continue;
//      }
//      break;
//    }
//  }
//  if (curr && cr == 0) {
//    uint64_t value = curr->value();
//    value_out->assign((char*) &value, 8);
//    return true;
//  }
//
//  mem = nullptr;
//
//  while (true) {
//    cr = 1;
//    auto l0_wptr = pmem->next();
//    if (auto l0_table = l0_wptr.lock()) {
//      pmem = std::move(l0_table);
//    } else {
//      return false;
//      //return get_cascade_l1(td, sv_key, value_out, mem_pred_a, shortcut, mem);
//    }
//    uint64_t key_a = pred_a->key();
//    skiplist = pmem->skiplist();
//    pred = (shortcut) ? shortcut : skiplist->head(r);
//    int h = pred->height;
//    for (int l = h - 1; l >= 1; l--) {
//      while (true) {
//        curr = (UINT64_pnode*) skiplist->offset_to_ptr(pred->next[l].load(), r);
//        //if (skiplist->cmp_(curr, key_a) < 0) {
//        //  if (h > kMaxHeight - 5) {
//        //    pred_a->shortcut = curr;
//        //  }
//        //}
//        if (skiplist->cmp_(curr, key) < 0) {
//          if (curr->shortcut != NULL) {
//            shortcut = curr->shortcut;
//          }
//          pred = curr;
//          continue;
//        }
//        break;
//      }
//    }
//    pred_a = pred;
//    // braided
//    {
//      while (true) {
//        curr = (UINT64_pnode*) skiplist->moff_to_ptr(pred->next[0].load());
//        if ((cr = skiplist->cmp_(curr, key)) < 0) {
//          pred = curr;
//          continue;
//        }
//        break;
//      }
//    }
//    if (curr && cr == 0) {
//      uint64_t value = curr->value();
//      value_out->assign((char*) &value, 8);
//      return true;
//    }
//  }
//
//  return false;
//}

bool brdb::read_from_mem(ThreadData* td, const int s, const std::string_view& key, std::string* value_out) {
  std::shared_ptr<MemTable> mem = mem_[s].table;
  while (mem == nullptr) {
    //mem = std::atomic_load(&memtable_[s]);
    //mem = std::atomic_load_explicit(&memtable_[s], std::memory_order_relaxed);
    mem = mem_[s].table;
    continue;
  }
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif
  return mem->get(td, search_key, value_out);
}

bool brdb::read_from_imms(ThreadData* td, const int s, const std::string_view& key, std::string* value_out) {
  if (mem_[s].immutables.empty()) return false;
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif
  auto it = mem_[s].immutables.begin();
  while (it.valid()) {
    auto imm = *it;
    if (imm->get(td, search_key, value_out)) {
      return true;
    }
    it.next();
  }
  return false;
}

bool brdb::read_from_pmem_level(ThreadData* td, const int s, const int level, const std::string_view& key, std::string* value_out) {
#ifndef BR_STRING_KV
  auto& search_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& search_key = key;
#endif
  if (level == 0) {
    if (level_[s][level].immutables.empty()) return false;
    std::shared_ptr<PmemTable> pmem = nullptr;
    auto it = level_[s][level].immutables.begin();
    while (it.valid()) {
      pmem = *it;
      if (pmem->get(td, search_key, value_out)) {
        return true;
      }
      it.next();
    }
    return false;
  }
  std::shared_ptr<PmemTable> pmem = level_[s][level].table;
  while (pmem == nullptr) {
    pmem =  level_[s][level].table;
    continue;
  }
  if (pmem->get(td, search_key, value_out)) {
    return true;
  } else {
    if (level_[s][level].immutables.empty()) return false;
    pmem = nullptr;
    auto it = level_[s][level].immutables.begin();
    while (it.valid()) {
      pmem = *it;
      if (pmem->get(td, search_key, value_out)) {
        return true;
      }
      it.next();
    }
  }
  return false;
}

bool brdb::scan(ThreadData* td, const std::string_view& key, const uint64_t len, std::vector<void*>* result) {
#ifndef BR_STRING_KV
  auto& begin_key = *reinterpret_cast<const uint64_t*>(key.data());
#else
  auto& begin_key = key;
#endif
  //using Cmp = PmemTable::NodeCmp;
  std::vector<casc_pmem_table_iterator*> iters;
  for (int i = 0; i < kNumShards; i++) {
    auto it = new casc_pmem_table_iterator(td->region, level_[i][kNumPmemLevels-1].table);
    it->seek(begin_key);
    iters.push_back(it);
  }
  while (result->size() < len) {
    std::sort(iters.begin(), iters.end(), [&](const casc_pmem_table_iterator* a, const casc_pmem_table_iterator* b){
          if (!b->valid()) return true;
          if (!a->valid()) return true;
          return (a->node()->key() < b->node()->key());
        });
    auto& min_iter = iters.front();
    if (min_iter->valid()) {
      result->push_back((void*) min_iter->node());
      min_iter->next();
    } else {
      break;
    }
  }
  for (int i = 0; i < kNumShards; i++) {
    delete iters[i];
  }
  return (result->size() == len);
}
