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

#ifndef __MESOS_SCHED_FLAGS_HPP__
#define __MESOS_SCHED_FLAGS_HPP__

#include <string>

#include <mesos/mesos.hpp>

#include <mesos/logging/flags.hpp>

#include <mesos/module/module.hpp>

#include <stout/duration.hpp>
#include <stout/flags.hpp>
#include <stout/option.hpp>

namespace mesos {
namespace internal {
namespace sched {

class Flags : public virtual logging::Flags
{
public:
  Flags();

  Duration authentication_backoff_factor;
  Duration registration_backoff_factor;
  Option<Modules> modules;
  Option<std::string> modulesDir;
  std::string authenticatee;
  Duration authentication_timeout;
};

} // namespace sched {
} // namespace internal {
} // namespace mesos {

#endif // __MESOS_SCHED_FLAGS_HPP__
