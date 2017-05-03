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

#ifndef __URI_SCHEMES_DOCKER_HPP__
#define __URI_SCHEMES_DOCKER_HPP__

#include <string>

#include <mesos/uri/uri.hpp>

#include "uri/utils.hpp"

namespace mesos {
namespace uri {
namespace docker {

inline URI image(
    const std::string& repository,
    const std::string& reference, // Either tag or digest.
    const std::string& registry,
    const Option<std::string>& scheme = None(),
    const Option<int>& port = None())
{
  return construct(
      "docker",
      repository,
      registry,
      port,
      reference,
      scheme);
}


inline URI manifest(
    const std::string& repository,
    const std::string& reference, // Either tag or digest.
    const std::string& registry,
    const Option<std::string>& scheme = None(),
    const Option<int>& port = None(),
    const Option<std::string>& principal = None(),
    const Option<std::string>& secret = None())
{
  return construct(
      "docker-manifest",
      repository,
      registry,
      port,
      reference,
      scheme,
      principal,
      secret);
}


inline URI blob(
    const std::string& repository,
    const std::string& digest,
    const std::string& registry,
    const Option<std::string>& scheme = None(),
    const Option<int>& port = None(),
    const Option<std::string>& principal = None(),
    const Option<std::string>& secret = None())
{
  return construct(
      "docker-blob",
      repository,
      registry,
      port,
      digest,
      scheme,
      principal,
      secret);
}

} // namespace docker {
} // namespace uri {
} // namespace mesos {

#endif // __URI_SCHEMES_DOCKER_HPP__
