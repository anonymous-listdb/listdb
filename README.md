ListDB
=========
ListDB - a novel key-value store that leverages the byte-addressability and high-performance of DCPM to resolve the write amplification and write stall problem.

# Build
```shell
mkdir build
cd build
cmake ..
cmake --build . -- -j
```

# Configure pmemobj poolset
```shell
cd scripts
./configure
```
Type the number of sockets.
Press Enter to configure the rest to default settings.

# Run sample scripts
```shell
cd scripts
./test_write_stall.sh
./test_ycsb.sh
```

# CMake options
* Enable/Disable Index Unified Logging (IUL).
```shell
cd build
cmake -DIUL={ON|OFF} ..
```
