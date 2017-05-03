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

#include <map>
#include <string>

#include <stout/gtest.hpp>

#include <process/future.hpp>
#include <process/gtest.hpp>

#include "secret/fetcher.hpp"

#include "tests/environment.hpp"
#include "tests/mesos.hpp"

#include "tests/containerizer/docker_archive.hpp"

using process::Future;
using process::Owned;

using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;

using mesos::internal::slave::state::SlaveState;

using mesos::secret::DefaultSecretFetcher;

using mesos::slave::ContainerTermination;

using std::map;
using std::string;

namespace mesos {
namespace internal {
namespace tests {

const char SECRET_VALUE[] = "password";

// Used to parameterize the SecretsTest.
struct SecretTestType
{
  SecretTestType(const std::string& path,
                 bool expectedToSucceedWithoutRootFS)
    : path(path),
      expectedToSucceedWithoutRootFS(expectedToSucceedWithoutRootFS) {}

  std::string path;
  bool expectedToSucceedWithoutRootFS;
};


class VolumeSecretIsolatorTest :
  public MesosTest,
  public ::testing::WithParamInterface<SecretTestType>
{
protected:
  virtual void SetUp()
  {
    SecretTestType param = GetParam();
    secretContainerPath = param.path;
    expectedToSucceedWithoutRootFS = param.expectedToSucceedWithoutRootFS;
    MesosTest::SetUp();
  }

  std::string secretContainerPath;
  bool expectedToSucceedWithoutRootFS;
};


INSTANTIATE_TEST_CASE_P(
    SecretTestType,
    VolumeSecretIsolatorTest,
    ::testing::Values(
        // Expected to succeed with and without root filesystem
        SecretTestType("my_secret", true),
        SecretTestType("some/my_secret", true),
        SecretTestType("/bin/touch", true),

        // Expected to succeed with root file system and fail without.
        SecretTestType("/my_secret", false),
        SecretTestType("/some/my_secret", false),
        SecretTestType("/etc/my_secret", false),
        SecretTestType("/etc/some/my_secret", false)));


TEST_P(VolumeSecretIsolatorTest, ROOT_SecretInVolumeWithoutRootFilesystem)
{
  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "filesystem/linux,volume/secret";

  Fetcher fetcher;
  DefaultSecretFetcher secretFetcher;

  Try<MesosContainerizer*> create = MesosContainerizer::create(
      flags,
      true,
      &fetcher,
      &secretFetcher);

  ASSERT_SOME(create);

  Owned<MesosContainerizer> containerizer(create.get());

  SlaveState state;
  state.id = SlaveID();

  AWAIT_READY(containerizer->recover(state));

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  ContainerInfo containerInfo;
  containerInfo.set_type(ContainerInfo::MESOS);

  Volume* volume = containerInfo.add_volumes();
  volume->set_mode(Volume::RW);
  volume->set_container_path(secretContainerPath);

  Volume::Source* source = volume->mutable_source();
  source->set_type(Volume::Source::SECRET);

  // Request a secret.
  Secret* secret = source->mutable_secret();
  secret->set_type(Secret::VALUE);
  secret->mutable_value()->set_data(SECRET_VALUE);

  CommandInfo command = createCommandInfo(
      "secret=$(cat " + secretContainerPath + "); "
      "test \"$secret\" = \"" + std::string(SECRET_VALUE) + "\"");

  ExecutorInfo executor = createExecutorInfo("test_executor", command);
  executor.mutable_container()->CopyFrom(containerInfo);

  string directory = path::join(flags.work_dir, "sandbox");
  ASSERT_SOME(os::mkdir(directory));

  Future<bool> launch = containerizer->launch(
      containerId,
      None(),
      executor,
      directory,
      None(),
      SlaveID(),
      map<string, string>(),
      false);

  if (!expectedToSucceedWithoutRootFS) {
    AWAIT_FAILED(launch);
    return;
  }

  AWAIT_ASSERT_TRUE(launch);

  Future<Option<ContainerTermination>> wait =
    containerizer->wait(containerId);

  AWAIT_READY(wait);
  ASSERT_SOME(wait.get());
  ASSERT_TRUE(wait.get()->has_status());
  EXPECT_WEXITSTATUS_EQ(!expectedToSucceedWithoutRootFS, wait.get()->status());
}

TEST_P(VolumeSecretIsolatorTest, ROOT_SecretInVolumeWithRootFilesystem)
{
  string registry = path::join(sandbox.get(), "registry");
  AWAIT_READY(DockerArchive::create(registry, "test_image_rootfs"));
  AWAIT_READY(DockerArchive::create(registry, "test_image_volume"));

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation =
    "filesystem/linux,volume/image,docker/runtime,volume/secret";
  flags.docker_registry = registry;
  flags.docker_store_dir = path::join(sandbox.get(), "store");
  flags.image_providers = "docker";

  Fetcher fetcher;
  DefaultSecretFetcher secretFetcher;

  Try<MesosContainerizer*> create = MesosContainerizer::create(
      flags,
      true,
      &fetcher,
      &secretFetcher);

  ASSERT_SOME(create);

  Owned<MesosContainerizer> containerizer(create.get());

  SlaveState state;
  state.id = SlaveID();

  AWAIT_READY(containerizer->recover(state));

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  ContainerInfo containerInfo = createContainerInfo(
      "test_image_rootfs",
      {createVolumeFromDockerImage(
          "rootfs", "test_image_volume", Volume::RW)});

  Volume* volume = containerInfo.add_volumes();
  volume->set_mode(Volume::RW);
  volume->set_container_path(secretContainerPath);

  Volume::Source* source = volume->mutable_source();
  source->set_type(Volume::Source::SECRET);

  // Request a secret.
  Secret* secret = source->mutable_secret();
  secret->set_type(Secret::VALUE);
  secret->mutable_value()->set_data(SECRET_VALUE);

  CommandInfo command = createCommandInfo(
      "secret=$(cat " + secretContainerPath + "); "
      "test \"$secret\" = \"" + std::string(SECRET_VALUE) + "\"");

  ExecutorInfo executor = createExecutorInfo("test_executor", command);
  executor.mutable_container()->CopyFrom(containerInfo);

  string directory = path::join(flags.work_dir, "sandbox");
  ASSERT_SOME(os::mkdir(directory));

  Future<bool> launch = containerizer->launch(
      containerId,
      None(),
      executor,
      directory,
      None(),
      SlaveID(),
      map<string, string>(),
      false);

  AWAIT_ASSERT_TRUE(launch);

  Future<Option<ContainerTermination>> wait =
    containerizer->wait(containerId);

  AWAIT_READY(wait);
  ASSERT_SOME(wait.get());
  ASSERT_TRUE(wait.get()->has_status());
  EXPECT_WEXITSTATUS_EQ(0, wait.get()->status());
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
