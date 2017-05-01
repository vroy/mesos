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

#include "slave/containerizer/mesos/isolators/environment_secret.hpp"

#include <list>
#include <string>
#include <vector>

#include <mesos/secret/secretfetcher.hpp>

#include <process/collect.hpp>
#include <process/id.hpp>
#include <process/subprocess.hpp>

#include <stout/foreach.hpp>
#include <stout/fs.hpp>
#include <stout/os/write.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include <stout/os/mkdir.hpp>

#ifdef __linux__
#include "linux/ns.hpp"
#endif // __linux__

#include "common/validation.hpp"

#include "linux/fs.hpp"

#include "secret/fetcher.hpp"

using std::list;
using std::string;
using std::vector;

using process::defer;
using process::dispatch;

using process::Failure;
using process::Future;
using process::Owned;
using process::PID;
using process::Subprocess;

using mesos::secret::SecretFetcher;

using mesos::slave::ContainerClass;
using mesos::slave::ContainerConfig;
using mesos::slave::ContainerLaunchInfo;
using mesos::slave::ContainerState;
using mesos::slave::Isolator;

namespace mesos {
namespace internal {
namespace slave {

Try<Isolator*> EnvironmentSecretIsolatorProcess::create(
    const Flags& flags,
    const Option<SecretFetcher*>& secretFetcher)
{
  Owned<MesosIsolatorProcess> process(new EnvironmentSecretIsolatorProcess(
      flags,
      secretFetcher));;

  return new MesosIsolator(process);
}


EnvironmentSecretIsolatorProcess::EnvironmentSecretIsolatorProcess(
    const Flags& _flags,
    const Option<SecretFetcher*>& _secretFetcher)
  : ProcessBase(process::ID::generate("environment-secret-isolator")),
    flags(_flags),
    secretFetcher(_secretFetcher) {}


EnvironmentSecretIsolatorProcess::~EnvironmentSecretIsolatorProcess() {}


bool EnvironmentSecretIsolatorProcess::supportsNesting()
{
  return true;
}


Future<Nothing> EnvironmentSecretIsolatorProcess::recover(
    const std::list<ContainerState>& state,
    const hashset<ContainerID>& orphans)
{
  return Nothing();
}


Future<Option<ContainerLaunchInfo>> EnvironmentSecretIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  Environment environment;

  list<Future<Environment::Variable>> futures;
  foreach (const Environment::Variable& variable,
           containerConfig.command_info().environment().variables()) {
    if (variable.type() != Environment::Variable::SECRET) {
      continue;
    }

    if (secretFetcher.isNone()) {
      return Failure("Error: environment contains secret variable but no "
                     "secret fetcher provided");
    }

    const Secret& secret = variable.secret();

    Option<Error> error = common::validation::validateSecret(secret);
    if (error.isSome()) {
      return Failure(
          "Invalid secret specified in environment: " + error->message);
    }

    Future<Environment::Variable> future =
      secretFetcher.get()->fetch(secret)
      .then([variable](const Secret::Value& secretValue)
            -> Future<Environment::Variable> {
          Environment::Variable result;
          result.set_name(variable.name());
          result.set_value(secretValue.data());
          return result;
        });

    futures.push_back(future);
  }

  return await(futures)
    .then([](const list<Future<Environment::Variable>>& variables)
          -> Future<Option<ContainerLaunchInfo>> {
        ContainerLaunchInfo launchInfo;
        Environment* environment = launchInfo.mutable_environment();
        foreach (const Future<Environment::Variable>& future, variables) {
          if (!future.isReady()) {
            return Failure(
                "Failed to acquire all secrets: " +
                future.isFailed() ? future.failure() : "discarded");
          }
          environment->add_variables()->CopyFrom(future.get());
        }
      launchInfo.mutable_task_environment()->CopyFrom(*environment);
      return launchInfo;
    });
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
