# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec")

add_library(Exec STATIC
  aggregation-node.cc
  aggregation-node-ir.cc
  base-sequence-scanner.cc
  catalog-op-executor.cc
  cross-join-node.cc
  data-sink.cc
  delimited-text-parser.cc
  exec-node.cc
  exchange-node.cc
  hash-join-node.cc
  hash-join-node-ir.cc
  hash-table.cc
  hbase-table-sink.cc
  hbase-table-writer.cc
  hdfs-scan-node.cc
  hdfs-scanner.cc
  hdfs-scanner-ir.cc
  hdfs-table-sink.cc
  hdfs-table-writer.cc
  hdfs-rcfile-scanner.cc
  hdfs-sequence-scanner.cc
  hdfs-avro-scanner.cc
  hdfs-avro-scanner-ir.cc
  hdfs-text-scanner.cc
  hdfs-lzo-text-scanner.cc
  hdfs-text-table-writer.cc
  hdfs-parquet-scanner.cc
  hdfs-parquet-table-writer.cc
  hbase-scan-node.cc
  hbase-table-scanner.cc
  blocking-join-node.cc
  merge-node.cc
  read-write-util.cc
  scan-node.cc
  scanner-context.cc
  select-node.cc
  text-converter.cc
  topn-node.cc
  pythia-reader-node.cc
  shm-scan-node.cc
  ../pythia/schema.cpp 
  ../pythia/hash.cpp 
  ../pythia/ProcessorMap.cpp 
  ../pythia/Barrier.cpp
  ../pythia/perfcounters.cpp
  ../pythia/query.cpp
  ../pythia/util/hashtable.cpp
  ../pythia/util/buffer.cpp
  ../pythia/visitors/recursivedestroy.cpp
  ../pythia/visitors/recursivefree.cpp
  ../pythia/visitors/prettyprint.cpp
  ../pythia/visitors/threadinit.cpp
  ../pythia/visitors/threadclose.cpp
  ../pythia/operators/base.cpp
  ../pythia/operators/mapwrapper.cpp
  ../pythia/operators/filter.cpp
  ../pythia/operators/sortlimit.cpp
  ../pythia/operators/genericaggregate.cpp
  ../pythia/operators/aggregatecount.cpp
  ../pythia/operators/aggregatesum.cpp
  ../pythia/operators/scan.cpp
  ../pythia/operators/partitionedscan.cpp
  ../pythia/operators/parallelscan.cpp
  ../pythia/operators/merge.cpp
  ../pythia/operators/join.cpp
  ../pythia/operators/shuffle.cpp
  ../pythia/operators/cycleaccountant.cpp
  ../pythia/util/affinitizer.cpp
  ../pythia/operators/project.cpp
  ../pythia/comparator.cpp
  ../pythia/conjunctionevaluator.cpp
  ../pythia/rawcompfns.cpp
  ../pythia/operators/memsegmentwriter.cpp
  ../pythia/operators/loaders/table.cpp
  ../pythia/operators/loaders/sfmt/SFMT.c
  ../pythia/operators/loaders/parser.cpp
  ../pythia/operators/loaders/loader.cpp
  ../pythia/operators/checker_callstate.cpp
  ../pythia/operators/printer_tuplecount.cpp
  ../pythia/operators/generator_int.cpp
  ../pythia/operators/printer_perfcnt.cpp
  ../pythia/operators/bitentropy.cpp
  ../pythia/operators/threadidprepend.cpp
  ../pythia/util/numaasserts.cpp
  ../pythia/util/numaallocate.cpp
  ../pythia/operators/consume.cpp
  ../pythia/operators/sortandrangepartition.cpp
  ../pythia/operators/partition.cpp
  ../pythia/operators/indexjoin.cpp
)

ADD_BE_TEST(zigzag-test)
ADD_BE_TEST(hash-table-test)
ADD_BE_TEST(delimited-text-parser-test)
ADD_BE_TEST(read-write-util-test)
ADD_BE_TEST(parquet-plain-test)
ADD_BE_TEST(parquet-version-test)
ADD_BE_TEST(row-batch-list-test)
