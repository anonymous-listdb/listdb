cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(brdb)

set(CMAKE_CXX_COMPILER "g++")
#set(CMAKE_VERBOSE_MAKEFILE ON)

option(DEBUG "build debug mode." OFF)
option(VERBOSE_LOG "log verbosity." ON)
option(IUL "use Index-Unified Logging." OFF)
option(STRING_KV "use variable lengthed string for KVs." OFF)
option(CASCADE "Cascading Search." OFF)

if(DEBUG)
  set(CMAKE_C_FLAGS "-Wall -Wsign-compare -g")
else()
  set(CMAKE_C_FLAGS "-Wall -Wsign-compare -O3 -g -DNDEBUG")
endif(DEBUG)

if(VERBOSE_LOG)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBR_VERBOSITY=1")
endif(VERBOSE_LOG)

if(IUL)
  message("[O] IUL ENABLED.")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBR_LOG_IUL")
else()
  message("[X] IUL disabled (use WAL).")
endif(IUL)

if(STRING_KV)
  message("[O] STRING KV ENABLED.")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBR_STRING_KV")
else()
  message("[X] STRING KV disabled.")
endif(STRING_KV)

if(CASCADE)
  message("[O] CASCADE SEARCH ENABLED.")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DBR_CASCADE")
else()
  message("[X] CASCADE SEARCH disabled.")
endif(CASCADE)

##
# Compiler options
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wfatal-errors ")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17 -march=native ")

##
# Link options
set(LINK_FLAGS "-lpthread")

##
# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR})

##
# libbrdb
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cc)
#add_library(brdb SHARED ${COMMON_FILE})
add_library(brdb STATIC ${COMMON_FILE})
target_link_libraries(brdb
  pthread
  pmemobj
  pmempool
  numa
  tcmalloc
  )

##
# Benchmarks
file(GLOB BENCHMAKR_SRC ${PROJECT_SOURCE_DIR}/bench/*.cc)
foreach (BENCHMARK ${BENCHMAKR_SRC})
  get_filename_component(BENCHMARK_NAME ${BENCHMARK} NAME_WE)
  add_executable(${BENCHMARK_NAME} ${BENCHMARK})
  target_link_libraries(${BENCHMARK_NAME} ${LINK_FLAGS})
  target_link_libraries(${BENCHMARK_NAME} brdb)
endforeach()

##
# YCSB 
function(ycsb_benchmark ycsb_test_file)
	# get_filename_component(ycsb_test_target_name "${ycsb_test_file}" NAME_WE)
	get_filename_component(ycsb_test_target_name "ycsb" NAME_WE)
	add_executable("${ycsb_test_target_name}" "")
	target_sources("${ycsb_test_target_name}"
		PRIVATE
		"ycsb/src/brdb_client.cc"
		"ycsb/core/core_workload.cc"
		"${ycsb_test_file}"
		)
	target_link_libraries("${ycsb_test_target_name}" brdb)
  target_link_libraries(${ycsb_test_target_name} stdc++fs)
endfunction(ycsb_benchmark)
ycsb_benchmark("${PROJECT_SOURCE_DIR}/ycsb/src/main.cc")
