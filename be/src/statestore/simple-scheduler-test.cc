// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "simple-scheduler.h"

using namespace std;
using namespace boost;
using namespace impala;

DECLARE_string(pool_conf_file);

namespace impala {

class SimpleSchedulerTest : public testing::Test {
 protected:
  SimpleSchedulerTest() {
    // The simple scheduler tries to resolve all backend hostnames to
    // IP addresses to compute locality. For the purposes of this test
    // we need a set of resolvable hostnames, so here we use IP
    // addresses which are always resolvable (to themselves!).

    // Setup hostname_scheduler_
    num_backends_ = 2;
    base_port_ = 1000;
    vector<TNetworkAddress> backends;
    backends.resize(num_backends_);
    backends.at(0).hostname = "127.0.0.0";
    backends.at(0).port = base_port_;
    backends.at(1).hostname = "localhost";
    backends.at(1).port = base_port_;

    hostname_scheduler_.reset(new SimpleScheduler(backends, NULL, NULL, NULL));

    // Setup local_remote_scheduler_
    backends.resize(4);
    int k = 0;
    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 2; ++j) {
        stringstream ss;
        ss << "127.0.0." << i;
        backends.at(k).hostname = ss.str();
        backends.at(k).port = base_port_ + j;
        ++k;
      }
    }
    local_remote_scheduler_.reset(new SimpleScheduler(backends, NULL, NULL, NULL));
  }

  int base_port_;
  int num_backends_;

  // This 2 backends: localhost and 127.0.0.0
  boost::scoped_ptr<SimpleScheduler> hostname_scheduler_;

  // This scheduler has 4 backends; 2 on each ipaddresses and has 4 different ports.
  boost::scoped_ptr<SimpleScheduler> local_remote_scheduler_;
};


TEST_F(SimpleSchedulerTest, LocalMatches) {
  // If data location matches some back end, use the matched backends in round robin.
  vector<TNetworkAddress> data_locations;
  data_locations.resize(5);
  for (int i = 0; i < 5; ++i) {
    data_locations.at(i).hostname = "127.0.0.1";
    data_locations.at(i).port = 0;
  }
  SimpleScheduler::BackendList backends;

  local_remote_scheduler_->GetBackends(data_locations, &backends);

  // Expect 5 round robin backends
  EXPECT_EQ(5, backends.size());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(backends.at(i).address.hostname, "127.0.0.1");
    EXPECT_EQ(backends.at(i).address.port, base_port_ + i % 2);
  }
}

TEST_F(SimpleSchedulerTest, HostnameTest) {
  // This test tests hostname resolution.
  // Both localhost and 127.0.0.1 should match backend 127.0.0.1 only.
  vector<TNetworkAddress> data_locations;
  data_locations.resize(2);
  data_locations.at(0).hostname = "localhost";
  data_locations.at(0).port = 0;
  data_locations.at(1).hostname = "127.0.0.1";
  data_locations.at(1).port = 0;

  SimpleScheduler::BackendList backends;

  hostname_scheduler_->GetBackends(data_locations, &backends);

  // Expect 2 round robin backends
  EXPECT_EQ(2, backends.size());
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(backends.at(i).address.hostname, "127.0.0.1");
    EXPECT_EQ(backends.at(i).address.port, base_port_);
  }
}

TEST_F(SimpleSchedulerTest, NonLocalHost) {
  // If data location doesn't match any of the ipaddress,
  // expect round robin ipaddress/port list
  vector<TNetworkAddress> data_locations;
  data_locations.resize(5);
  for (int i = 0; i < 5; ++i) {
    data_locations.at(i).hostname = "non exists ipaddress";
    data_locations.at(i).port = 0;
  }
  SimpleScheduler::BackendList backends;

  local_remote_scheduler_->GetBackends(data_locations, &backends);

  // Expect 5 round robin on ipaddress, then on port
  // 1. 127.0.0.1:1000
  // 2. 127.0.0.0:1000
  // 3. 127.0.0.1:1001
  // 4. 127.0.0.0:1001
  // 5. 127.0.0.1:1000
  EXPECT_EQ(5, backends.size());
  EXPECT_EQ(backends.at(0).address.hostname, "127.0.0.1");
  EXPECT_EQ(backends.at(0).address.port, 1000);
  EXPECT_EQ(backends.at(1).address.hostname, "127.0.0.0");
  EXPECT_EQ(backends.at(1).address.port, 1000);
  EXPECT_EQ(backends.at(2).address.hostname, "127.0.0.1");
  EXPECT_EQ(backends.at(2).address.port, 1001);
  EXPECT_EQ(backends.at(3).address.hostname, "127.0.0.0");
  EXPECT_EQ(backends.at(3).address.port, 1001);
  EXPECT_EQ(backends.at(4).address.hostname, "127.0.0.1");
  EXPECT_EQ(backends.at(4).address.port, 1000);
}

TEST_F(SimpleSchedulerTest, InitPoolWhiteList) {
  // Check a non-existant configuration
  FLAGS_pool_conf_file = "/I/do/not/exist";
  vector<TNetworkAddress> backends;
  backends.push_back(TNetworkAddress());
  scoped_ptr<SimpleScheduler> sched(new SimpleScheduler(backends, NULL, NULL, NULL));
  EXPECT_FALSE(sched->Init().ok());

  // Check a valid configuration (although one with some malformed lines)
  string impala_home(getenv("IMPALA_HOME"));
  stringstream conf_file;
  conf_file << impala_home << "/be/src/statestore/test-pool-conf";
  FLAGS_pool_conf_file = conf_file.str();
  sched.reset(new SimpleScheduler(backends, NULL, NULL, NULL));
  EXPECT_TRUE(sched->Init().ok());
  const SimpleScheduler::UserPoolMap& pool_map = sched->user_pool_map();
  EXPECT_EQ(3, pool_map.size());
  EXPECT_EQ(2, pool_map.find("admin")->second.size());
  EXPECT_TRUE(pool_map.end() == pool_map.find("root"));
  EXPECT_EQ(2, pool_map.find("*")->second.size());
  EXPECT_EQ("Staging", pool_map.find("*")->second[0]);

  // Check the pool determination logic.
  string pool;
  EXPECT_TRUE(sched->GetYarnPool("admin", TQueryOptions(), &pool).ok());
  // Staging is the default pool
  EXPECT_EQ("Staging", pool);
  EXPECT_TRUE(sched->GetYarnPool("i-want-default", TQueryOptions(), &pool).ok());
  EXPECT_EQ("Staging", pool);

  TQueryOptions options;
  // Only admin can use prod
  options.__set_yarn_pool("prod");
  EXPECT_FALSE(sched->GetYarnPool("user", options, &pool).ok());
  EXPECT_TRUE(sched->GetYarnPool("admin", options, &pool).ok());

  // Everyone can use the default pools
  options.__set_yarn_pool("Staging");
  EXPECT_TRUE(sched->GetYarnPool("user", options, &pool).ok());
  EXPECT_TRUE(sched->GetYarnPool("admin", options, &pool).ok());
  options.__set_yarn_pool("Default");
  EXPECT_TRUE(sched->GetYarnPool("user", options, &pool).ok());
  EXPECT_TRUE(sched->GetYarnPool("admin", options, &pool).ok());
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
