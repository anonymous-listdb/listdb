#ifndef BENCH_H_
#define BENCH_H_

#include <string>
#include <getopt.h>

#include "common.h"

struct BenchConf {
  int num_ops;
  int num_threads;

  void print() {
    fprintf(stdout, "num_ops (per thread): %d\n", num_ops);
    fprintf(stdout, "num_threads: %d\n", num_threads);
  }
};

void parse_command_line_arguments(int argc, char* argv[], DBConf* conf, BenchConf* bconf, std::vector<std::string>* wl) {
  int c;
  struct option long_options[] = {
    { "num", required_argument, 0, 'n' },
    { "threads", required_argument, 0, 't' },
    { "workloads", required_argument, 0, 'w' },
    { "mem", required_argument, 0, 'm' },
    { "dram", required_argument, 0, 'd' },
    { "compaction_workers", required_argument, 0, 'c' },
    { "task_size", required_argument, 0, 0 },
    { "shards", required_argument, 0, 's' },
    { "l0_mem_merges", required_argument, 0, 0 },
    { "performance_monitor_mode", required_argument, 0, 0 },
    { "num_pmem_levels", required_argument, 0, 0 },
    { "num_regions" , required_argument, 0, 0 },
    { "num_shards", required_argument, 0, 0 },
    { 0, 0, 0, 0 }
  };

  // Default DBConf
  *conf = DBConf();

  // Default BenchConf
  bconf->num_ops = 1000000;
  bconf->num_threads = 1;

  // Workloads
  wl->emplace_back("putrandom");

  while (true) {
    int idx;
    c = getopt_long(argc, argv, "n:t:w:m:d:c:s:", long_options, &idx);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 0: {
        if (long_options[idx].flag != 0) {
          break;
        }
        if (optarg) {
          if (strcmp(long_options[idx].name, "task_size") == 0) {
            conf->task_size = std::stoi(optarg);
            break;
          } else if (strcmp(long_options[idx].name, "l0_mem_merges") == 0) {
            conf->l0_mem_merges = std::stoi(optarg);
            break;
          } else if (strcmp(long_options[idx].name, "performance_monitor_mode") == 0) {
            conf->performance_monitor_mode = std::stoi(optarg);
            break;
          } else if (strcmp(long_options[idx].name, "num_pmem_levels") == 0) {
            conf->num_pmem_levels = std::stoi(optarg);
            break;
          } else if (strcmp(long_options[idx].name, "num_regions") == 0) {
            conf->num_regions = std::stoi(optarg);
            break;
          } else if (strcmp(long_options[idx].name, "num_shards") == 0) {
            conf->num_shards = std::stoi(optarg);
            break;
          }
          printf(" with arg %s", optarg);
          printf("\n");
          break;
        }
      }
      case 'n': {
        bconf->num_ops = std::stoi(optarg);
        break;
      }
      case 't': {
        bconf->num_threads = std::stoi(optarg);
        break;
      }
      case 'w': {
        wl->clear();
        std::string s(optarg);
        std::string token;
        size_t begin = 0;
        size_t end = s.find(',');
        while (end != std::string::npos) {
          token = s.substr(begin, end - begin);
          wl->push_back(token);
          begin = end + 1;
          end = s.find(',', begin);
        }
        token = s.substr(begin, end);
        wl->push_back(token);
        break;
      }
      case 'm': {
        conf->mem_size = atoll(optarg);
        break;
      }
      case 'd': {
        conf->dram_limit = std::stoull(optarg);
        break;
      }
      case 'c': {
        conf->num_workers = std::stoi(optarg);
        break;
      }
      case 's': {
        conf->num_shards = std::stoi(optarg);
        break;
      }
      default: {
        abort();
      }
    }
  }
}

#endif  // BENCH_H_
