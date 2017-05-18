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

#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/isolator.hpp>

#include <mesos/slave/flags.hpp>
#include <mesos/slave/isolator.hpp>

#include <process/owned.hpp>

#include <stout/try.hpp>

#include "slave/containerizer/mesos/isolators/posix.hpp"

using namespace mesos;

using mesos::internal::slave::Flags;
using mesos::internal::slave::PosixCpuIsolatorProcess;
using mesos::internal::slave::PosixMemIsolatorProcess;

using mesos::modules::ModuleInfo;

using mesos::slave::Isolator;

using process::Owned;

// The sole purpose of this function is just to exercise the
// compatibility logic.
static bool compatible()
{
  return true;
}


static Try<Owned<Isolator>> createCpuIsolator(const ModuleInfo& moduleInfo)
{
  Flags flags;
  Try<Isolator*> result = PosixCpuIsolatorProcess::create(flags);
  if (result.isError()) {
    return Error("Error creating Posix cpu isolator: " + result.error());
  }
  return Owned<Isolator>(result.get());
}


static Try<Owned<Isolator>> createMemIsolator(const ModuleInfo& moduleInfo)
{
  Flags flags;
  Try<Isolator*> result = PosixMemIsolatorProcess::create(flags);
  if (result.isError()) {
    return Error("Error creating Posix memory isolator: " + result.error());
  }
  return Owned<Isolator>(result.get());
}


// Declares a CPU Isolator module named 'org_apache_mesos_TestCpuIsolator'.
mesos::modules::Module<Isolator> org_apache_mesos_TestCpuIsolator(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test CPU Isolator module.",
    compatible,
    createCpuIsolator);


// Declares a Memory Isolator module named 'org_apache_mesos_TestMemIsolator'.
mesos::modules::Module<Isolator> org_apache_mesos_TestMemIsolator(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Apache Mesos",
    "modules@mesos.apache.org",
    "Test Memory Isolator module.",
    nullptr, // Do not perform any compatibility check.
    createMemIsolator);
