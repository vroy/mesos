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

#ifndef __VOLUME_SECRET_ISOLATOR_HPP__
#define __VOLUME_SECRET_ISOLATOR_HPP__

#include <list>
#include <string>
#include <vector>

#include <mesos/secret/fetcher.hpp>

#include <stout/hashmap.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/mesos/isolator.hpp"

namespace mesos {
namespace internal {
namespace slave {

class VolumeSecretIsolatorProcess : public MesosIsolatorProcess
{
public:
  static Try<mesos::slave::Isolator*> create(
      const Flags& flags,
      const Option<SecretFetcher*>& secretFetcher);

  virtual ~VolumeSecretIsolatorProcess() {}

  virtual bool supportsNesting();

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ContainerState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<Option<mesos::slave::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const mesos::slave::ContainerConfig& containerConfig);

private:
  VolumeSecretIsolatorProcess(
      const Flags& flags,
      const Option<SecretFetcher*>& secretFetcher);

  process::Future<Option<mesos::slave::ContainerLaunchInfo>> _prepare(
      const ContainerID& containerId,
      const mesos::slave::ContainerConfig& containerConfig,
      const std::vector<std::string>& secretNames,
      const std::vector<std::string>& targets,
      const std::list<process::Future<Secret::Value>>& futures);

  const Flags flags;
  const Option<SecretFetcher*> secretFetcher;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __VOLUME_SECRET_ISOLATOR_HPP__
