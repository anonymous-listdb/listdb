#ifndef KEY_TYPES_H_
#define KEY_TYPES_H_

#include <string_view>

class StringKey {
 public:
  StringKey(char* buf, const std::string_view& key, const uint64_t tag, const uint64_t value) : data_(buf) {
    char* p = data_;
    *reinterpret_cast<uint32_t*>(p) = key.length();
    p += sizeof(uint32_t);
    memcpy(p, key.data(), key.length());
    p += key.length();
    *reinterpret_cast<uint64_t*>(p) = tag;
    p += sizeof(uint64_t);
    *reinterpret_cast<uint64_t*>(p) = value;
  }

  StringKey(char* buf) : data_(buf) { }

  static size_t compute_alloc_size(const std::string_view& key) {
    return aligned_size(8, sizeof(uint32_t) + key.length() + sizeof(uint64_t) + sizeof(uint64_t));  // key_len, key, tag, value
  }

  uint32_t length() const {
    return *reinterpret_cast<uint32_t*>(data_);
  }

  std::string_view key() const {
    std::string_view sv(data_ + sizeof(uint32_t), this->length());
    return sv;
  }

  uint64_t tag() const {
    return *reinterpret_cast<uint64_t*>(data_ + sizeof(uint32_t) + this->length());
  }

  uint64_t value() const {
    return *reinterpret_cast<uint64_t*>(data_ + sizeof(uint32_t) + this->length() + sizeof(uint64_t));
  }

  size_t alloc_size() const {
    return aligned_size(8, sizeof(uint32_t) + this->length() + 2*sizeof(uint64_t));
  }

 private:
  char* data_;
};

#endif  // KEY_TYPES_H_
