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

#include "secret/fetcher.hpp"

#include <string>

#include <mesos/mesos.hpp>

#include <mesos/secret/secretfetcher.hpp>

#include <process/future.hpp>

#include <stout/nothing.hpp>

using std::string;

using process::Future;
using process::Failure;

namespace mesos {
namespace secret {

Future<Secret::Value> DefaultSecretFetcher::fetch(const Secret& secret)
{
  if (secret.has_reference()) {
    return Failure("Default secret fetcher cannot resolve references");
  }

  if (!secret.has_value()) {
    return Failure("Secret has no value");
  }

  return secret.value();
}

} // namespace secret {
} // namespace mesos {
