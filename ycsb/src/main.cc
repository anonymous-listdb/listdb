#include "ycsb/src/brdb_client.h" 
#include "common.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "ycsb/src/ycsb_runner.h"
#include "ycsb/src/utils.h"

int main(int argc, char* argv[]){
	utils::Properties common_props;
  std::vector<char> wl_chars;
  parse_command_line_arguments(argc, argv, &common_props, &wl_chars);

  // Workload
  std::vector<CoreWorkload*> workloads;
  for (auto& wl_char : wl_chars) {
    auto wl = new CoreWorkload();
    if (wl_char == 'l') {
      auto wl_props = gen_workload_props('a', common_props);
      wl->Init(wl_props, /*is_load=*/true);
    } else {
      auto wl_props = gen_workload_props(wl_char, common_props);
      wl->Init(wl_props, /*is_load=*/false);
    }
    workloads.push_back(wl);
  }

	DBConf conf;
	conf.num_regions = std::stoi(common_props.GetProperty("num_regions", std::to_string(kNumRegionsDefault)));
	conf.num_shards = std::stoi(common_props.GetProperty("num_shards", std::to_string(kNumShardsDefault)));
	conf.mem_size = std::stoll(common_props.GetProperty("mem_size", std::to_string(kMemTableSizeDefault)));
	conf.dram_limit = std::stoull(common_props.GetProperty("dram_limit", std::to_string(kDRAMSizeTotalDefault)));
	conf.num_workers = std::stoi(common_props.GetProperty("num_workers", std::to_string(kNumWorkersDefault)));
	conf.use_existing = false;
  conf.periodic_compaction_mode = std::stoi(common_props.GetProperty("periodic_compaction_mode", std::to_string(kPeriodicCompactionModeDefault)));
  conf.performance_monitor_mode = 0;
  conf.num_pmem_levels = std::stoi(common_props.GetProperty("num_pmem_levels", std::to_string(kNumPmemLevelsDefault)));
  conf.num_log_unified_levels = 2;
  BenchOptions bopts;
  bopts.write_stats_to_file = std::stoi(common_props.GetProperty("write_stats_to_file", "0"));

  // Open DB
  brdb* db = new brdb(conf);

  // Init and Run Workloads
  int num_threads = std::stoi(common_props.GetProperty("threadcount"));
  YCSBRunner runner(num_threads, workloads, conf, bopts, db);
  runner.run_all();

	fflush(stdout);
	return 0;
}
