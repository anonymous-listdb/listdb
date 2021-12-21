#ifndef VALUE_LOG_H_
#define VALUE_LOG_H_

#include "log.h"

class ValueLog {
 public:
  void init(const int s);
  uint64_t write_value(ThreadData* td, const int s, const std::string_view& value);

 private:
  LogBlock* get_available_log_block(ThreadData* td, const int s,
                                    const int r, const size_t my_size,
                                    size_t* before_out);

 private:
  LogBlock* blocks_[kMaxNumRegions];
  TOID(LogBase) log_base_[kMaxNumRegions];
  std::mutex mu_;
};

#endif  // VALUE_LOG_H_
