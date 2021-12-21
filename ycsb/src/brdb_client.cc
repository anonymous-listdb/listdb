#include "ycsb/src/brdb_client.h"
#include "algorithm"
#include "math.h"
#include <numa.h>
#include <chrono>
using namespace ycsbc;


BrDBClient::BrDBClient(WorkloadProxy *workload_proxy, const int num_threads,
                       DBConf conf,
                       BenchOptions bopts,
                       brdb* db) :
    num_threads_(num_threads),
    load_num_(workload_proxy->record_count()),
    request_num_(workload_proxy->operation_count()),
    conf_(conf),
    bopts_(bopts),
    db_(db),
    workload_proxy_(workload_proxy),
    stop_(false) {
  write_stats_to_file_ = bopts.write_stats_to_file;
  if (!db_) {
    abort();
  }
  Reset();
}

BrDBClient::~BrDBClient() {
  if(request_time_ != nullptr)
    delete request_time_;
  if(read_time_ != nullptr)
    delete read_time_;
  if(update_time_ != nullptr)
    delete update_time_;
  if(workload_wrapper_ != nullptr)
    delete workload_wrapper_;
}

void BrDBClient::run() {
  if (workload_proxy_->is_load()) {
    Load();
  } else {
    Work();
  }
}

void BrDBClient::Load() {
  Reset();
  assert(update_time_ == nullptr);
  update_time_ = new TimeRecord(load_num_ + 1);
  int base_coreid = 0;

  //if (workload_wrapper_ == nullptr){
  //  workload_wrapper_ = new WorkloadWrapper(workload_proxy_, load_num_, true);
  //}

  uint64_t num = load_num_ / num_threads_;
  std::vector<std::thread> threads;
  auto fn = std::bind(&BrDBClient::BrDBLoader, this,
      std::placeholders::_1, std::placeholders::_2);
  auto start = TIME_NOW;
  for(int i=0; i<num_threads_; i++){
    if(i == num_threads_ - 1)
      num = num + load_num_ % num_threads_;
    threads.emplace_back(fn, num, (base_coreid + i));
  }
  for(auto &t : threads)
    t.join();
  double time = TIME_DURATION(start, TIME_NOW);
  stop_.store(true);

  printf("==================================================================\n");
  PrintArgs();
  printf("Load %ld requests in %.3lf seconds.\n", load_num_, time/1000/1000);
  printf("Load latency: %.3lf us\n", update_time_->Sum()/update_time_->Size());
  printf("Load IOPS: %.3lf M\n", load_num_/time*1000*1000/1000/1000);
  printf("Load median latency: %.3lf us\n", update_time_->Tail(0.5));
  printf("Load P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
      update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
        update_time_->Tail(0.90), update_time_->Tail(0.75));
  printf("------------------------------------------------------------------\n");
  printf("Load %c - IOPS: %.3lf M\n", workload_proxy_->name_str().back(), load_num_/time*1000*1000/1000/1000);
  printf("==================================================================\n");

  // Record results to a file

  if (write_stats_to_file_) {
    update_time_->WriteToFile(workload_proxy_->filename_prefix(), "write_latency");
  }
}

void BrDBClient::Work() {
  Reset();
  assert(request_time_ == nullptr);
  assert(read_time_ == nullptr);
  assert(update_time_ == nullptr);
  request_time_ = new TimeRecord(request_num_ + 1);
  read_time_ = new TimeRecord(request_num_ + 1);
  update_time_ = new TimeRecord(request_num_ + 1);

  int base_coreid = 0;

  if (workload_wrapper_ == nullptr){
    workload_wrapper_ = new WorkloadWrapper(workload_proxy_, request_num_, false);
  }

  uint64_t num = request_num_ / num_threads_;
  std::vector<std::thread> threads;
  std::function< void(uint64_t, int, bool)> fn;
  fn = std::bind(&BrDBClient::BrDBWorker, this, 
      std::placeholders::_1, std::placeholders::_2,
      std::placeholders::_3);
  printf("start time: %s\n", GetDayTime().c_str());
  auto start = TIME_NOW;
  for(int i=0; i<num_threads_; i++){
    if(i == num_threads_ - 1)
      num = num + request_num_ % num_threads_;
    threads.emplace_back(fn, num, (base_coreid + i), i==0);
  }
  for(auto &t : threads)
    t.join();
  double time = TIME_DURATION(start, TIME_NOW);
  printf("end time: %s\n", GetDayTime().c_str());
  stop_.store(true);

  fflush(stdout);

  assert(request_time_->Size() == request_num_);
  printf("==================================================================\n");
  PrintArgs();
  printf("Finish %ld requests in %.3lf seconds.\n", request_num_, time/1000/1000);
  if(read_time_->Size() != 0){
    printf("read num: %ld, read avg latency: %.3lf us, read median latency: %.3lf us\n", 
        read_time_->Size(), read_time_->Sum()/read_time_->Size(), read_time_->Tail(0.50));
    printf("read P999: %.3lf us, P99: %.3lf us, P95: %.3lf us, P90: %.3lf us, P75: %.3lf us\n",
        read_time_->Tail(0.999), read_time_->Tail(0.99), read_time_->Tail(0.95),
        read_time_->Tail(0.90), read_time_->Tail(0.75));
  }else{
    printf("read num: 0, read avg latency: 0 us, read median latency: 0 us\n");
    printf("read P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
  }
  if(update_time_->Size() != 0){
    printf("update num: %ld, update avg latency: %.3lf us, update median latency: %.3lf us\n", 
          update_time_->Size(), update_time_->Sum()/update_time_->Size(), update_time_->Tail(0.50));
    printf("update P999: %.3lf us, P99: %.3lf us, P95: %.3lfus, P90: %.3lf us, P75: %.3lf us\n",
        update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
        update_time_->Tail(0.90), update_time_->Tail(0.75));
  }else{
    printf("update num: 0, update avg latency: 0 us, update median latency: 0 us\n");
    printf("update P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
  }
  printf("Work latency: %.3lf us\n", request_time_->Sum()/request_time_->Size());
  printf("Work IOPS: %.3lf M\n", request_num_/time*1000*1000/1000/1000);
  printf("Work median latency: %.3lf us\n", request_time_->Tail(0.5));
  printf("Work P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
      request_time_->Tail(0.999), request_time_->Tail(0.99), request_time_->Tail(0.95),
        request_time_->Tail(0.90), request_time_->Tail(0.75));
  printf("submit_time: %.3lf\n", submit_time_ / 1000.0 / request_num_);
  printf("------------------------------------------------------------------\n");
  printf("%s %s - IOPS: %.3lf M\n", workload_proxy_->name_str().c_str(), workload_proxy_->distribution_str().c_str(), request_num_/time*1000*1000/1000/1000);
  printf("==================================================================\n");
  fflush(stdout);

  if (write_stats_to_file_) {
    request_time_->WriteToFile(workload_proxy_->filename_prefix(), "request_latency");
    update_time_->WriteToFile(workload_proxy_->filename_prefix(), "write_latency");
    read_time_->WriteToFile(workload_proxy_->filename_prefix(), "read_latency");
  }
}

void BrDBClient::BrDBWorker(uint64_t num, int coreid, bool is_master){
  ThreadData td;
  unsigned long a,d,c;
  __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  int chip = (c & 0xFFF000)>>12;
  int core = c & 0xFFF;
  td.cpu = core;
  td.numa = chip;
  td.region = td.numa % kNumRegions;

  TimeRecord request_time(num + 1);
  TimeRecord read_time(num + 1);
  TimeRecord update_time(num + 1);

  if(is_master){
    printf("starting requests...\n");
  }

  static char w_value_buf[4096];
  memset(w_value_buf, 'a', 4096);

  for(uint64_t i=0; i<num/10; i++){
    WorkloadWrapper::Request** reqs = workload_wrapper_->GetNextRequests();
    for (int j = 0; j < 10; j++) {
      WorkloadWrapper::Request *req = reqs[j];
      ycsbc::Operation opt = req->Type();
      assert(req != nullptr);
      auto start = TIME_NOW;
      if(opt == READ){
        db_->get(&td, req->Key(), NULL);
      }else if(opt == UPDATE){
        std::string_view w_value(w_value_buf, req->Length());
        db_->put(&td, req->Key(), w_value);
      }else if(opt == INSERT){
        std::string_view w_value(w_value_buf, req->Length());
        db_->put(&td, req->Key(), w_value);
      }else if(opt == READMODIFYWRITE){
        db_->get(&td, req->Key(), NULL);
        std::string_view w_value(w_value_buf, req->Length());
        db_->put(&td, req->Key(), w_value);
      }else if(opt == SCAN){
        std::vector<void*> result;
        db_->scan(&td, req->Key(), req->Length(), &result);
      }else{
        throw utils::Exception("Operation request is not recognized!");
      }
      double time =  TIME_DURATION(start, TIME_NOW);
      request_time.Insert(time);
      if(opt == READ || opt == SCAN){
        read_time.Insert(time);
      }else if(opt == UPDATE || opt == INSERT || opt == READMODIFYWRITE){
        update_time.Insert(time);
      }else{
        assert(0);
      }
    }
  }

  mutex_.lock();
  request_time_->Join(&request_time);
  read_time_->Join(&read_time);
  update_time_->Join(&update_time);
  mutex_.unlock();
} // BrDBWorker

void BrDBClient::BrDBLoader(uint64_t num, int coreid){
  ThreadData td;
  td.cpu = coreid;
  set_affinity(td.cpu);
  td.numa = numa_node_of_cpu(td.cpu);
  td.region = td.numa % kNumRegions;

  TimeRecord request_time(num + 1);
  TimeRecord update_time(num + 1);

  char w_value_buf[4096];
  memset(w_value_buf, 'a', 4096);

  for(uint64_t i=0; i<num/10; i++){
    std::array<std::string, 10> keys;
    size_t value_len;
    workload_proxy_->LoadInsertArgss(keys, value_len);
    std::string_view w_value(w_value_buf, value_len);
    for (int j = 0; j < 10; j++) {
      auto start = TIME_NOW;
      db_->put(&td, keys[j], w_value);
      double time = TIME_DURATION(start, TIME_NOW);
      request_time.Insert(time);
      update_time.Insert(time);
    }
  }

  mutex_.lock();
  update_time_->Join(&update_time);
  mutex_.unlock();
} // BrDBLoader

void BrDBClient::Reset(){
  load_index_ = 0;
  wait_time_ = wal_time_ = 0;
  block_read_time_ = 0;
  write_delay_time_ = 0;
  write_memtable_time_ = 0;
  submit_time_ = 0;

  stop_.store(false);
}

void BrDBClient::PrintArgs(){
  printf("-----------configuration------------\n");
  printf("Clients: %d\n", num_threads_);
  printf("Logging: %s\n", kLoggingModeString);
  printf("Shards: %d\n", conf_.num_shards);
  printf("Regions: %d\n", conf_.num_regions);
  if (conf_.dram_limit >= (1ull<<30)) {
	  printf("Max_dram_size: %.3lf GB\n", conf_.dram_limit * 1.0/(1ull<<30));
  } else {
	  printf("Max_dram_size: %.3lf MB\n", conf_.dram_limit * 1.0/(1ull<<20));
  }
	printf("Max_background_jobs: %d\n", conf_.num_workers / conf_.task_size);
	printf("Task threads: %d\n", conf_.num_workers);
	if((size_t) conf_.mem_size >= (1ull << 30)){
		printf("Write_buffer_size: %.3lf GB\n", conf_.mem_size * 1.0/(1ull<<30));
	}else{
		printf("Write_buffer_size: %.3lf MB\n", conf_.mem_size * 1.0/(1ull<<20));
	}
  printf("-------------------------------------\n");
  fflush(stdout);
}
