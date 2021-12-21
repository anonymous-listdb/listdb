#ifndef DS_LOCK_FREE_SKIP_LIST_H_
#define DS_LOCK_FREE_SKIP_LIST_H_

#include "common.h"

class lockfree_skiplist {
  friend class casc_skiplist_iterator;

 public:
#ifndef BR_STRING_KV
  typedef uint64_t Key;
  typedef struct UINT64_node Node;
  typedef struct UINT64_cmp NodeCmp;
#else
  typedef std::string_view Key;
  typedef struct VARSTR_node Node;
  typedef struct VARSTR_cmp NodeCmp;
#endif

 public:
  lockfree_skiplist();
  // Returns pred
  Node* insert(ThreadData* const td, Node* const node, Node* pred = NULL);
  // Returns (node->key == key) ? node : NULL
  Node* find(ThreadData* const td, const Key& key, const Node* pred = NULL);
  //void merge(lockfree_skiplist* other);
  Node* head();

 private:
  void find_position(ThreadData* const td, Node* node, Node* preds[], Node* succs[], Node* pred = NULL, const int min_h = 0);

 public:
  NodeCmp cmp_;
  Node* head_;
};

#if 1
class casc_skiplist_iterator {
 public:
  using Key = lockfree_skiplist::Key;
  using Node = lockfree_skiplist::Node;
  using NodeCmp = lockfree_skiplist::NodeCmp;
 public:
  casc_skiplist_iterator(lockfree_skiplist* const skiplist);
  void seek(const Key& key, const Node* pred = NULL);
  bool valid();
  int cmp();
  const Node* node();
  const Node* shortcut();
  uint64_t pmem_shortcut_offset(const int r);
 private:
  lockfree_skiplist* const skiplist_;
  const Node* node_;
  int cmp_;  // NodeCmp(node_, search_key)
  const Node* shortcut_;  // NodeCmp(shortcut_, node_) <= 0 holds if shortcut_ != NULL
  uint64_t pmem_shortcut_offsets_[kMaxNumRegions];
};
#endif

#endif  // DS_LOCK_FREE_SKIP_LIST_H_
