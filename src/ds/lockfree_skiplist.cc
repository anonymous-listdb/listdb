#include "ds/lockfree_skiplist.h"

#include <cstring>
#include <new>

#include <stdlib.h>

#include "common.h"

lockfree_skiplist::lockfree_skiplist() {
  auto head_key = Node::head_key();
  const size_t alloc_size = Node::compute_alloc_size(head_key, kMaxHeight);
  void* buf = aligned_alloc(8, alloc_size);
  head_ = Node::init_node((char*) buf, head_key, 0xffffffffffffffff, 0, kMaxHeight);
  head_->log_moff = 0x000000000000ffff;  // offset: 0, region: -1
  std::atomic_thread_fence(std::memory_order_release);
}

lockfree_skiplist::Node* lockfree_skiplist::insert(ThreadData* const td, Node* const node, Node* pred)  {
  //td->visit_cnt++;
  //assert(node->key != 0);
  if (pred == NULL) {
    pred = head_;
  }
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  while (true) {
    find_position(td, node, preds, succs, pred);
    for (int l = 1; l < node->height; l++) {
      node->next[l].store(succs[l]);
    }
    node->next[0].store(succs[0]);
    if (!preds[0]->next[0].compare_exchange_strong(succs[0], node)) {
      pred = preds[kMaxHeight - 1];
      continue;
    }

    for (int l = 1; l < node->height; l++) {
      while (true) {
        if (!preds[l]->next[l].compare_exchange_strong(succs[l], node)) {
          find_position(td, node, preds, succs, preds[kMaxHeight - 1], l);
          continue;
        }
        break;
      }
    }
    break;
  }
  return preds[kMaxHeight - 1];
}

lockfree_skiplist::Node* lockfree_skiplist::find(ThreadData* const td, const Key& key, const Node* pred) {
  if (pred == NULL) {
    pred = head_;
  }
  Node* curr;
  int h = pred->height;
  int cr;
  for (int l = h - 1; l >= 0; l--) {
    while (true) {
      curr = pred->next[l].load();
      if ((cr = cmp_(curr, key)) < 0) {
        pred = curr;
        continue;
      }
      break;
    }
  }
  return (cr == 0) ? curr : NULL;
}

//void lockfree_skiplist::merge(lockfree_skiplist* other) {
//  Node* pred = head_;
//  Node* o = other->head_->next[0].load();
//  while (o) {
//    Node* new_node = Node::new_node(o->key(), o->height);
//    memcpy(new_node, o, 20);
//    pred = insert(NULL, new_node, pred);
//    o = o->next[0].load();
//  }
//}

lockfree_skiplist::Node* lockfree_skiplist::head() {
  return head_;
}

void lockfree_skiplist::find_position(ThreadData* const td, Node* node, Node* preds[], Node* succs[], Node* pred, const int min_h) {
  if (pred == NULL) {
    pred = head_;
  }
  Node* curr;
  int h = pred->height;
  for (int l = h - 1; l >= min_h; l--) {
    while (true) {
      curr = pred->next[l].load();
      if (cmp_(curr, node) < 0) {
        //td->visit_cnt++;
        pred = curr;
        continue;
      }
      break;
    }
    preds[l] = pred;
    succs[l] = curr;
  }
}

#if 1

casc_skiplist_iterator::casc_skiplist_iterator(lockfree_skiplist* const skiplist) : skiplist_(skiplist) {
  node_ = skiplist_->head_;
  cmp_ = 0;
  shortcut_ = NULL;
  std::memset(pmem_shortcut_offsets_, 0, 8*kNumRegions);
}

void casc_skiplist_iterator::seek(const Key& key, const Node* pred) {
  if (pred == NULL) {
    pred = skiplist_->head_;
  }
  Node* curr = NULL;
  int h = pred->height;
  int cr = 1;
  for (int l = h - 1; l >= 0; l--) {
    while (true) {
      curr = pred->next[l].load();
      if ((cr = skiplist_->cmp_(curr, key)) < 0) {
        pred = curr;
        if (pred->type() == kTypeShortcut) {
          shortcut_ = (Node*) pred->value();
#ifdef BR_LOG_IUL
          int16_t region = pred->log_moff & 0xffff;
          uint64_t offset = pred->log_moff >> 16;
          pmem_shortcut_offsets_[region] = offset;
#endif
        }
        //if (pred->has_shortcut()) {
        //  shortcut_ = pred;
        //  uint64_t moff = pred->pmem_shortcut_moff();
        //  int16_t region = moff & 0xffff;
        //  uint64_t offset = moff >> 16;
        //  pmem_shortcut_offs_[region] = offset; 
        //}
        continue;
      }
      break;
    }
  }
  //pred_ = pred;
  cmp_ = cr;
  node_ = curr;
}

bool casc_skiplist_iterator::valid() {
  return node_ != NULL;
}

int casc_skiplist_iterator::cmp() {
  return cmp_;
}

const casc_skiplist_iterator::Node* casc_skiplist_iterator::node() {
  return node_;
}

const casc_skiplist_iterator::Node* casc_skiplist_iterator::shortcut() {
  return shortcut_;
}

uint64_t casc_skiplist_iterator::pmem_shortcut_offset(const int r) {
  return pmem_shortcut_offsets_[r];
}

#endif
