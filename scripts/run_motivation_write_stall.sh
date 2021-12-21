#!/usr/bin/env bash

set -e
set -o pipefail

num_ops=20000000
mem=$((64*1000*1000))
dram=$((256*1000*1000))
threads=1

echo "===================================="
echo "-----------configuration------------"
echo "num_ops=$num_ops"
echo "mem=$mem"
echo "dram=$dram"
echo "threads=$threads"
echo "------------------------------------"

## (a)
#mkdir -p ../build
#cd ../build
#cmake -DDEBUG=OFF -DVERBOSE_LOG=OFF -DIUL=OFF ..
#cmake --build . -- -j
#cd -
#echo "*** (a) WAL - Single L0 ***"
#./clear_db.sh
#set -x
#../build/simple_test \
#  --num=${num_ops} \
#  --threads=${threads} \
#  --workloads="putrandom" \
#  --mem=${mem} \
#  --dram=${dram} \
#  --performance_monitor_mode=1 \
#  --num_pmem_levels=1 \
#  --l0_mem_merges=9999999
#set +x

# (b)
cd ../build
cmake -DDEBUG=OFF -DVERBOSE_LOG=OFF -DIUL=OFF ..
cmake --build . -- -j
cd -
echo "*** (a) WAL ***"
./clear_db.sh
set -x
../build/simple_test \
  --num=${num_ops} \
  --threads=${threads} \
  --workloads="putrandom" \
  --mem=${mem} \
  --dram=${dram} \
  --performance_monitor_mode=1 \
  --num_pmem_levels=1 \
  --l0_mem_merges=1
set +x

# (c)
cd ../build
cmake -DDEBUG=OFF -DVERBOSE_LOG=OFF -DIUL=ON ..
cmake --build . -- -j
cd -
echo "*** (b) IUL ***"
./clear_db.sh
set -x
../build/simple_test \
  --num=${num_ops} \
  --threads=${threads} \
  --workloads="putrandom" \
  --mem=${mem} \
  --dram=${dram} \
  --performance_monitor_mode=1 \
  --num_pmem_levels=1 \
  --l0_mem_merges=1
set +x
