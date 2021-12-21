#include "value_log.h"

void ValueLog::init(const int s) {
  for (int i = 0; i < kNumRegions; i++) {
    TOID(LogBase) base;
    POBJ_ZALLOC(vpop[s][i], &base, LogBase, sizeof(LogBase));
    log_base_[i] = base;

    TOID(LogBlockBase) nbb;  // new block base
    POBJ_ZALLOC(vpop[s][i], &nbb, LogBlockBase, sizeof(LogBlockBase) + (kLogBlockSize - 1));
    D_RW(log_base_[i])->head_block_base = nbb;
    D_RW(log_base_[i])->npp = nbb;
    pmemobj_persist(vpop[s][i], D_RW(log_base_[i]), sizeof(LogBlockBase));

    void* buf = malloc(sizeof(LogBlock));
    LogBlock* nb = new (buf) LogBlock(i);  // new block
    nb->load(nbb);
    blocks_[i] = nb;
  }
}

LogBlock* ValueLog::get_available_log_block(ThreadData* td, const int s,
                                            const int r, const size_t my_size,
                                            size_t* before) {
  LogBlock* b = blocks_[r];
  while (true) {
    if (b == NULL) {
      b = blocks_[r];
      continue;
    }
    *before = b->fetch_add_size(my_size);
    if (my_size + *before > kLogBlockSize) {
      std::unique_lock<std::mutex> lk(mu_);
      if (b == blocks_[r]) {
        TOID(LogBlockBase) nbb;
        POBJ_ZALLOC(vpop[s][r], &nbb, LogBlockBase, sizeof(LogBlockBase) + (kLogBlockSize - 1));
        LogBlockBase* nbbp = D_RW(nbb);
        nbbp->p = 0;
        nbbp->npp = 0;
        D_RW(b->base())->next = nbb;
        pmemobj_persist(vpop[s][r], &(D_RW(b->base())->next), sizeof(TOID(LogBlockBase)));
        void* buf = malloc(sizeof(LogBlock));
        LogBlock* nb = new (buf) LogBlock(r);
        nb->load(nbb);
        //std::atomic_store(&blocks_[r], nb);
        blocks_[r] = nb;
        b = blocks_[r];
      } else {
        b = blocks_[r];
      }
      lk.unlock();
      continue;
    }
    break;
  }
  return b;
}

uint64_t ValueLog::write_value(ThreadData* td, const int s, const std::string_view& value) {
  const int r = td->region;
  size_t my_size = 4 + value.length();
  size_t before = 0;
  auto b = get_available_log_block(td, s, r, my_size, &before);

  void* begin = b->data() + before;
  char* p = (char*) begin;
  *((uint32_t*) p) = value.length();
  p += 4;
  memcpy(p, value.data(), value.length());
  pmemobj_persist(vpop[s][r], begin, sizeof(uint32_t) + value.length());
  uint64_t value_moff = (((uintptr_t) begin - (uintptr_t) vpop[s][r]) << 16) | r;
  return value_moff;
}
