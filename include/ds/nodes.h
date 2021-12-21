#ifndef BR_DS_NODES_H_
#define BR_DS_NODES_H_

#include "common.h"

// DRAM Cascade Node (uint64_t)
struct cnode {
  uint64_t k;       // integer value or offset
  uint64_t t;       // tag
  uint64_t v;     // integer value or offset
  uint64_t shortcut_moff;  // shortcut to L0 (or immutable?)
  uint64_t log_moff;  // log marked offset
  int height;
  std::atomic<cnode*> next[1];

  static uint64_t head_key() { return 0ULL; }

  static size_t compute_alloc_size(const uint64_t key, const int height) {
    return aligned_size(8, sizeof(cnode) + (height-1)*8);
  }

  static cnode* init_node(char* buf, const uint64_t key,
                                 const uint64_t tag, const uint64_t value,
                                 const int height, bool init_next_arr = true) {
    cnode* node = (cnode*) buf;
    node->k = key;
    node->t = tag;
    node->v = value;
    node->shortcut_moff = 0;
    node->height = height;
    if (init_next_arr) {
      memset(node->next, 0, height * 8);
    }
    return node;
  }

  size_t alloc_size() const { return sizeof(cnode) + (height-1)*8; }
  uint64_t key() const { return k; }
  uint64_t tag() const { return t; }
  uint8_t type() const { return ValueType(t & 0xff); }
  uint64_t value() const { return v; }
  char* data() const { return (char*) this; }
};

// Persistent Cascade Node (uint64_t)
struct pcnode {
  uint64_t k;       // integer value or offset
  uint64_t t;       // tag
  uint64_t v;     // integer value or offset
  uint64_t shortcut_moff;  // shortcut to L0 (or immutable?)
  int height;
  std::atomic<uint64_t> next[1];

  static uint64_t head_key() { return 0ULL; }

  static size_t compute_alloc_size(const uint64_t key, const int height) {
    return aligned_size(8, sizeof(pcnode) + (height-1)*8);
  }

  static pcnode* init_node(char* buf, const uint64_t key,
                                 const uint64_t tag, const uint64_t value,
                                 const int height, bool init_next_arr = true) {
    pcnode* node = (pcnode*) buf;
    node->k = key;
    node->t = tag;
    node->v = value;
    node->shortcut_moff = 0;
    node->height = height;
    if (init_next_arr) {
      memset(node->next, 0, height * 8);
    }
    return node;
  }

  static pcnode* load_node(char* buf) {
    return (pcnode*) buf;
  }

  size_t alloc_size() const { return sizeof(pcnode) + (height-1)*8; }
  uint64_t key() const { return k; }
  uint64_t tag() const { return t; }
  uint8_t type() const { return ValueType(t & 0xff); }
  uint64_t value() const { return v; }
  void set_shortcut(const uint64_t moff) { shortcut_moff = moff; }
  char* data() const { return (char*) this; }
};

template <typename T>
struct UINT64_node_cmp {
  int operator()(const T* a, const T* b) const {
    assert(b != NULL);
    if (a == NULL || a->key() > b->key()) {
      return 1;
    } else if (a->key() < b->key()) {
      return -1;
    } else {
      if (a->tag() < b->tag()) {
        return 1;
      } else if (a->tag() > b->tag()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
  int operator()(const T* a, const uint64_t b) const {
    if (a == NULL || a->key() > b) {
      return 1;
    } else if (a->key() < b) {
      return -1;
    } else {
      return 0;
    }
  }
};

#endif  // BR_DS_NODES_H_
