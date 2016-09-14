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

#ifdef __WINDOWS__
#include <process/windows/winsock.hpp>
#endif // __WINDOWS__

#include <process/process.hpp>

#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/subcommand.hpp>

#include "slave/containerizer/mesos/launch.hpp"
#include "slave/containerizer/mesos/mount.hpp"

#ifdef __linux__
#include "slave/containerizer/mesos/isolators/network/cni/cni.hpp"
#endif

using namespace mesos::internal::slave;


int main(int argc, char** argv)
{
  // We need to backup the original value of `LIBPROCESS_SSL_ENABLED`
  // so we can restore it after initializing libprocess. This makes
  // sure that the mesos-containerizer does not try to use any SSL
  // related configuration setup - namely the certificate and key
  // paths.
  Option<std::string> enabled = os::getenv("LIBPROCESS_SSL_ENABLED");

  os::unsetenv("LIBPROCESS_SSL_ENABLED");

  process::initialize();

  if (enabled.isSome()) {
    os::setenv("LIBPROCESS_SSL_ENABLED", enabled.get());
  }

#ifdef __WINDOWS__
  // Initialize the Windows socket stack.
  process::Winsock winsock;
#endif

#ifdef __linux__
  return Subcommand::dispatch(
      None(),
      argc,
      argv,
      new MesosContainerizerLaunch(),
      new MesosContainerizerMount(),
      new NetworkCniIsolatorSetup());
#else
  return Subcommand::dispatch(
      None(),
      argc,
      argv,
      new MesosContainerizerLaunch(),
      new MesosContainerizerMount());
#endif
}
