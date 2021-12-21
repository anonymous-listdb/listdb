#ifndef UTIL_H_
#define UTIL_H_

#include <cstdint>
#include <string_view>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#if BR_VERBOSITY > 0
#define flogf(strm, fmt, ...) fprintf_with_file_and_line(__FILE__, __LINE__, strm, fmt, ##__VA_ARGS__)
#else
#define flogf(strm, fmt, ...)
#endif

void fprintf_with_file_and_line(const char* file, const int line, FILE* strm, const char* fmt, ...);

class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

inline uint64_t key_num(const std::string_view& key) {
  unsigned char buf[sizeof(uint64_t)];
  memset(buf, 0, sizeof(buf));
  memmove(buf, key.data(), std::min(key.size(), sizeof(uint64_t)));
  uint64_t number;
  number = static_cast<uint64_t>(buf[0]) << 56
        | static_cast<uint64_t>(buf[1]) << 48
        | static_cast<uint64_t>(buf[2]) << 40
        | static_cast<uint64_t>(buf[3]) << 32
        | static_cast<uint64_t>(buf[4]) << 24
        | static_cast<uint64_t>(buf[5]) << 16
        | static_cast<uint64_t>(buf[6]) << 8
        | static_cast<uint64_t>(buf[7]);
  return number;
}

inline size_t aligned_size(const size_t align, const size_t size) {
  int mod = size % align;
  return (mod == 0) ? size : size + (align - mod);
}

#endif  // UTIL_H_
