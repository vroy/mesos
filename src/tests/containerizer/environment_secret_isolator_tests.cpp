// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>

#include <stout/gtest.hpp>

#include <process/future.hpp>
#include <process/gtest.hpp>

#include "secret/fetcher.hpp"

#include "tests/environment.hpp"
#include "tests/mesos.hpp"

using process::Future;
using process::Owned;

using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;

using mesos::master::detector::MasterDetector;

using mesos::secret::DefaultSecretFetcher;

using std::string;

namespace mesos {
namespace internal {
namespace tests {

const char SECRET_VALUE[] = "password";
const char SECRET_ENV_NAME[] = "My_SeCrEt";

class EnvironmentSecretIsolatorTest : public MesosTest {};


// This test verifies that the environment secrets are resolved when launching a
// task.
TEST_F(EnvironmentSecretIsolatorTest, FetchSecret)
{
  // Use a normal master.
  Try<Owned<cluster::Master>> master = StartMaster();
  ASSERT_SOME(master);

  // NOTE: Modules and hooks are loaded in the test setup.
  mesos::internal::slave::Flags flags = CreateSlaveFlags();

  Fetcher fetcher;
  DefaultSecretFetcher secretFetcher;
  Try<MesosContainerizer*> containerizer =
    MesosContainerizer::create(flags, false, &fetcher, &secretFetcher);
  EXPECT_SOME(containerizer);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Try<Owned<cluster::Slave>> slave =
    StartSlave(detector.get(), containerizer.get());
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _))
    .Times(1);

  Future<std::vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  TaskInfo task;
  task.set_name("secret");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->CopyFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(
      Resources::parse("cpus:0.1;mem:32").get());

  CommandInfo *command = task.mutable_command();
  command->set_value(
      "echo " + std::string(SECRET_ENV_NAME) +
      ": $" + std::string(SECRET_ENV_NAME) +
      "; test \"$" + std::string(SECRET_ENV_NAME) + "\" = \"" +
      std::string(SECRET_VALUE) + "\"");

  // Request a secret.
  mesos::Environment::Variable *env =
    command->mutable_environment()->add_variables();
  env->set_name(SECRET_ENV_NAME);
  env->set_type(mesos::Environment::Variable::SECRET);

  mesos::Secret* secret = env->mutable_secret();
  secret->set_type(Secret::VALUE);
  secret->mutable_value()->set_data(SECRET_VALUE);

  // NOTE: Successful tasks will output two status updates.
  Future<TaskStatus> statusRunning;
  Future<TaskStatus> statusFinished;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillOnce(FutureArg<1>(&statusFinished));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(statusRunning);
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());
  AWAIT_READY(statusFinished);
  EXPECT_EQ(TASK_FINISHED, statusFinished.get().state());

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
