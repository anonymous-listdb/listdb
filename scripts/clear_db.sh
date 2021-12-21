#!/usr/bin/env bash

set -e
set -o pipefail

username=`whoami`
configpath="/tmp/brdb_${username}/config"
num_regions=`cat $configpath | while read line; do if [[ "$line" =~ "NUM_REGIONS" ]]; then echo ${line/NUM_REGIONS /}; break; fi; done`
primary_region=`cat $configpath | while read line; do if [[ "$line" =~ "PRIMARY_REGION" ]]; then echo ${line/PRIMARY_REGION /}; break; fi; done`
num_shards=`cat $configpath | while read line; do if [[ "$line" =~ "NUM_SHARDS" ]]; then echo ${line/NUM_SHARDS /}; break; fi; done`
db_paths=( `cat $configpath | \
  while read line; do \
    if [[ "$line" =~ "NUM_SHARDS" ]]; then \
      num_shards=${line/NUM_SHARDS /}; \
      for((i=0;i<${num_regions};i++)); do \
        read sub_line; \
        echo ${sub_line}; \
      done; \
      break; \
    fi; \
  done` )
db_root="${db_paths[${primary_region}]}/root"
rm -f ${db_root}
for ((i=0;i<${num_regions};i++)); do
  for t in "log" "value"; do
    for ((j=0;j<${num_shards};j++)); do
      rm -f "${db_paths[${i}]}"/${t}_${j}/*
    done
  done
  for ((l=0;l<3;l++)); do
    for ((j=0;j<${num_shards};j++)); do
      rm -f "${db_paths[${i}]}"/level_${l}/index_${j}/*
    done
  done
done
