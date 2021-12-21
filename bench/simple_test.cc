#include <chrono>
#include <cstdio>
#include <vector>
#include <thread>
#include <random>

#include <numa.h>

#include "brdb.h"
#include "util.h"


#include "bench/bench.h"

thread_local char buf[4096];

void load(brdb* db, const int num_threads, const int num_ops_per_thread) {
  uint64_t num_ops = num_threads * num_ops_per_thread;

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> workload;

  std::vector<std::thread> loaders;
  std::chrono::time_point<std::chrono::steady_clock> begin = std::chrono::steady_clock::now();
  for (int i = 0; i < num_threads; i++) {
    loaders.emplace_back([&, id=i]{
          std::seed_seq sseq{id, id + 1};
          std::mt19937 gen(sseq);
          //std::uniform_int_distribution<uint64_t> dist(1, num_threads * num_ops_per_thread);
          std::uniform_int_distribution<uint64_t> dist(1, std::numeric_limits<uint64_t>::max() - 1);
          uint64_t value = 1234;
          ThreadData* td = new ThreadData();
          td->cpu = id;
          set_affinity(td->cpu);
          td->numa = numa_node_of_cpu(td->cpu);
          td->region = td->numa % kNumRegions;
          //numa_run_on_node(td->numa);
          db->register_client(id, td);
          for (int j = 0; j < num_ops_per_thread; j++) {
            uint64_t key = dist(gen);
#ifndef BR_STRING_KV
            db->put(td, (char*) &key, (char*) &value);
#else
            sprintf(buf, "user%zu", key);
            db->put(td, buf, (char*) &value);
#endif
          }
        });
  }
  for (auto& t : loaders) {
    if (t.joinable()) {
      t.join();
    }
  }
  auto end_tp = std::chrono::steady_clock::now();
  std::chrono::duration<double> dur = end_tp - begin;
  double dur_sec = dur.count();

  fprintf(stdout, "Load IOPS: %.3lf M\n", num_ops/dur_sec/1000000);
  fprintf(stdout, "elapsed (client): %.3lf sec\n", dur_sec);
  fprintf(stdout, "---------------------------------------------\n");
  //fprintf(stdout, "Wait for all compactions done...\n");
  //db->wait_compaction();
  //fprintf(stdout, "All Compaction tasks are done.\n");
  //db->table_stats();
  //fprintf(stdout, "---------------------------------------------\n");
  //end_tp = std::chrono::steady_clock::now();
  //dur = end_tp - begin;
  //double dur_sec_compact = dur.count();
  //fprintf(stdout, "elapsed (until compaction done): %.3lf sec\n", dur_sec_compact);
  //fprintf(stdout, "=============================================\n");
}

void search_random(brdb* db, const int num_threads, const int num_ops_per_thread) {
  uint64_t num_ops = num_threads * num_ops_per_thread;

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> workload;

  std::vector<std::thread> loaders;
  std::chrono::time_point<std::chrono::steady_clock> begin = std::chrono::steady_clock::now();
  //ThreadData* tds[100];
  for (int i = 0; i < num_threads; i++) {
    loaders.emplace_back([&, id=i]{
          //set_affinity(id);
          std::seed_seq sseq{id, id + 1};
          std::mt19937 gen(sseq);
          //std::uniform_int_distribution<uint64_t> dist(1, num_threads * num_ops_per_thread);
          std::uniform_int_distribution<uint64_t> dist(1, std::numeric_limits<uint64_t>::max() - 1);
          ThreadData* td = new ThreadData();
          //tds[id] = td;
          td->cpu = id;
          set_affinity(td->cpu);
          td->numa = numa_node_of_cpu(td->cpu);
          td->region = td->numa % kNumRegions;
          //numa_run_on_node(td->numa);
          int found = 0;
          for (int j = 0; j < num_ops_per_thread; j++) {
            uint64_t key = dist(gen);
#ifndef BR_STRING_KV
            if (db->get(td, (char*) &key)) {
              found++;
            }
#else
            sprintf(buf, "user%zu", key);
            db->get(td, buf);
#endif
          }
          fprintf(stderr, "id=%d found=%d\n", id, found);
        });
  }
  for (auto& t : loaders) {
    if (t.joinable()) {
      t.join();
    }
  }
  //for (int i = 0; i < num_threads; i++) {
  //  fprintf(stdout, "client %d pmem node visit cnt: %zu\n", i, tds[i]->visit_cnt);
  //} 
  auto end_tp = std::chrono::steady_clock::now();
  std::chrono::duration<double> dur = end_tp - begin;
  double dur_sec = dur.count();
  fprintf(stdout, "Search IOPS: %.3lf M\n", num_ops/dur_sec/1000000);
  fprintf(stdout, "elapsed: %.3lf sec\n", dur_sec);
}

int main(int argc, char* argv[]) {
  // Parse command line args
  DBConf conf;
  BenchConf bconf;
  std::vector<std::string> workloads;
  parse_command_line_arguments(argc, argv, &conf, &bconf, &workloads);

  // Init db
  brdb* db = new brdb(conf);
  fprintf(stdout, "=============================================\n");
  conf.print();
  bconf.print();
  fprintf(stdout, "total KV size: %zu MB\n", (size_t) bconf.num_threads*bconf.num_ops*16/1000/1000);
  fprintf(stdout, "=============================================\n");

  // Run Workloads
  for (auto& wl : workloads) {
    if (wl.compare("putrandom") == 0) {
      load(db, bconf.num_threads, bconf.num_ops);
    } else if (wl.compare("getrandom") == 0) {
      search_random(db, bconf.num_threads, bconf.num_ops);
    } else if (wl.compare("stabilize") == 0) {
      db->stabilize();
      std::string table_state_str;
      db->get_table_state_string(&table_state_str);
      fprintf(stdout, "%s", table_state_str.c_str());
    } else if (wl.compare("printstate") == 0) {
      std::string table_state_str;
      db->get_table_state_string(&table_state_str);
      fprintf(stdout, "%s", table_state_str.c_str());
    } else {
      fprintf(stderr, "Invalid workload type: %s\n", wl.c_str());
    }
  }
  fprintf(stdout, "---------------------------------------------\n");

  // Print Performance Statistics
  db->perf_stats();
  fprintf(stdout, "=============================================\n");
  return 0;
}
