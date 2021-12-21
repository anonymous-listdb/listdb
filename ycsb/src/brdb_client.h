#ifndef BRDB_CLIENT_H
#define BRDB_CLIENT_H
#include <iterator>
#include "cmath"
#include "thread"
#include "sys/mman.h"
#include "sys/syscall.h"
#include "unistd.h"
#include "workloadwrapper.h"
#include <sys/time.h>
#include "common.h"
#include "brdb.h"
#include <experimental/filesystem>
using namespace ycsbc;
namespace fs = std::experimental::filesystem::v1;

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION(start, end) (std::chrono::duration<double>((end)-(start)).count() * 1000 * 1000)
typedef std::chrono::high_resolution_clock::time_point TimePoint;

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)  (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

inline void *AllocBuffer(size_t capacity){
  void *buffer = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  assert(buffer != MAP_FAILED);
  return buffer;
}

inline void DeallocBuffer(void *buffer, size_t capacity){
  assert(munmap(buffer, capacity) == 0);
}

using client_data = ThreadData;
//struct client_data {
//  uint64_t total_finished_requests;
//  uint64_t write_finished;
//  uint64_t read_finished;
//  //double write_time_total;
//  //double write_func_time_total;
//  //double read_time_total;
//  //double read_func_time_total;
//  //uint64_t query_total;
//  //uint64_t write_total;
//  //uint64_t read_total;
//  //uint64_t core_id;
//};

struct BenchOptions {
  bool write_stats_to_file = false;
};

class BrDBClient{
 public:
  BrDBClient(WorkloadProxy *workload_proxy, const int num_threads,
             DBConf conf, BenchOptions bopts, brdb* db);
  ~BrDBClient();
  void Load();
  void Work();
  void run();

 private:
  const int num_threads_;
  const uint64_t load_num_;
  const uint64_t request_num_;
  DBConf conf_;
  BenchOptions bopts_;
  brdb* db_ = nullptr;

  WorkloadWrapper *workload_wrapper_ = nullptr;
  WorkloadProxy *workload_proxy_ = nullptr;

  double wal_time_;
  double wait_time_;
  double complete_memtable_time_;
  double block_read_time_;
  double write_delay_time_;
  double write_memtable_time_;
  uint64_t submit_time_;

  std::mutex mutex_;

  std::atomic<bool> stop_;

  bool write_stats_to_file_ = false;
  uint64_t load_index_;

  struct TimeRecord{
    private:
    double *data_;
    const uint64_t capacity_;
    uint64_t size_;
    bool sorted;
    public:
    TimeRecord(uint64_t capacity):
      data_(nullptr),
      capacity_(capacity),
      size_(0),
      sorted(false){
      data_ = (double *)AllocBuffer(capacity_ * sizeof(double));
      assert(data_ != nullptr);
      }
    ~TimeRecord(){
      assert(size_ <= capacity_);
      if(data_ != nullptr)
      DeallocBuffer(data_, capacity_ * sizeof(double));
    }
    void Insert(double time){
      data_[size_++] = time;
      assert(size_ <= capacity_);
      sorted = false;
    }
    double *Data(){
      return data_;
    }
    void Join(TimeRecord* time){
      assert(data_ != nullptr);
      assert(time->Data() != nullptr);
      uint64_t pos = __sync_fetch_and_add(&size_, time->Size());
      assert(size_ <= capacity_);
      memcpy(data_ + pos, time->Data(), sizeof(double) * time->Size());
      sorted = false;
    }
    double Sum(){
      assert(data_ != nullptr);
      return std::accumulate(data_, data_ + size_, 0.0);
    }
    uint64_t Size(){
      return size_;
    }
    double Tail(double f){
      assert(data_ != nullptr);
      if(!sorted){
      std::sort(data_, data_ + size_);
      sorted = true;
      }
      return data_[(uint64_t)(size_ * f)];
    }
    void WriteToFile(const std::string& prefix, const std::string& name) {
      char user_name[100];
      getlogin_r(user_name, 100);
      std::string logdir("/scratch/");
      logdir.append(user_name).append("/logs/brdb/");
      fs::create_directories(logdir);

      std::string logpath = logdir + prefix + "." + name;
      std::string finalpath = logpath;
      int num = 0;
      while (fs::exists(finalpath)) {
        finalpath = logpath + "." + std::to_string(num++);
      }
      std::ofstream outfile(finalpath);

      std::copy(data_, data_ + size_, std::ostream_iterator<double>(outfile, "\n"));
      outfile.close();
      fprintf(stdout, "Latency distribution is written to: %s\n", finalpath.c_str());
    }
  };

  TimeRecord *request_time_  = nullptr;
  TimeRecord *read_time_  = nullptr;
  TimeRecord *update_time_ = nullptr;
  TimeRecord *iops_ = nullptr;
  TimeRecord *insert_failed_ = nullptr;

  //client_data* cd_[kMaxNumClients];

private:
  inline void SetAffinity(int coreid){
    coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(coreid, &mask);
    int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
    if (!rc) {
      exit(1);
    }
  }

  static std::string GetDayTime(){
    const int kBufferSize = 100;
    char buffer[kBufferSize];
    struct timeval now_tv;
    gettimeofday(&now_tv, nullptr);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    snprintf(buffer, kBufferSize,
      "%04d/%02d/%02d-%02d:%02d:%02d.%06d",
      t.tm_year + 1900,
      t.tm_mon + 1,
      t.tm_mday,
      t.tm_hour,
      t.tm_min,
      t.tm_sec,
      static_cast<int>(now_tv.tv_usec));
    return std::string(buffer);
  }

  void BrDBWorker(uint64_t num, int coreid, bool is_master);
  void BrDBLoader(uint64_t num, int coreid);
  void Reset();
  void PrintArgs();
};

#endif // BRDB_CLIENT_H
