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
#include <list>
#include <vector>

#include <mesos/authentication/http/combined_authenticator.hpp>

#include <mesos/module/http_authenticator.hpp>

#include <stout/foreach.hpp>

#include "module/manager.hpp"

namespace mesos {
namespace http {
namespace authentication {

using std::list;
using std::string;
using std::vector;

using process::http::Forbidden;
using process::http::Result;
using process::http::Unauthorized;

using process::http::authentication::AuthenticationResult;
using process::http::authentication::Authenticator;


class CombinedAuthenticatorProcess
  : public Process<CombinedAuthenticatorProcess>
{
public:
  CombinedAuthenticatorProcess(
      const string& _realm,
      const vector<Authenticator*>& _authenticators);

  Future<AuthenticationResult> authenticate(const Request& request) override;

private:
  static Option<AuthenticationResult> combineFailedRequests(
      const list<AuthenticationResult>& results);

  vector<Owned<Authenticator>> authenticators;
  const string realm;
};


CombinedAuthenticatorProcess::CombinedAuthenticatorProcess(
    const string& _realm,
    const vector<Authenticator*>& _authenticators);
  : ProcessBase(ID::generate("__combined_authenticator__")),
    realm(_realm)
{
  for (const Authenticator* authenticator, _authenticators) {
    authenticators.push_back(Owned<Authenticator>(authenticator));
  }
}


Option<AuthenticationResult>
    CombinedAuthenticatorProcess::combineFailedRequests(
        const list<AuthenticationResult>& results)
{
  AuthenticationResult mergedResult;
  Option<Unauthorized> mergedUnauthorized;
  Option<Forbidden> mergedForbidden;

  foreach (const AuthenticationResult& result, results) {
    if (result.unauthorized.isSome()) {
      if (mergedUnauthorized.isNone()) {
        mergedUnauthorized = result.unauthorized;
        continue;
      }

      // Combine 'WWW-Authenticate' headers so that we advertise
      // all acceptable authentication schemes to the client.
      if (result.unauthorized->headers.contains("WWW-Authenticate")) {
        mergedUnauthorized->headers["WWW-Authenticate"] +=
          "," + result.unauthorized->headers.at("WWW-Authenticate");
      }

      mergedUnauthorized->body += "\n\n" + result.unauthorized->body;
      continue;
    }

    if (result.forbidden.isSome()) {
      if (mergedForbidden.isNone()) {
        mergedForbidden = result.forbidden;
        continue;
      }

      mergedForbidden->body += "\n\n" + result.forbidden->body;
    }
  }

  // Prefer to return `Unauthorized` if present, otherwise return `Forbidden`.
  // `Forbidden` responses would be returned by authenticators which do not wish
  // to issue a challenge to the client; if we have a challenge to issue, we
  // should do so.
  if (mergedUnauthorized.isSome()) {
    mergedResult.unauthorized = mergedUnauthorized.get();
    return mergedResult;
  } else if (mergedForbidden.isSome()) {
    mergedResult.forbidden = mergedForbidden.get();
    return mergedResult;
  }

  return None();
}


CombinedAuthenticator::CombinedAuthenticator(
    const string& _realm,
    const vector<Authenticator*>& _authenticators)
  : process(new CombinedAuthenticatorProcess(_realm, _authenticators))
{
  spawn(*process_);
}


Future<AuthenticationResult> CombinedAuthenticator::authenticate(
    const Request& request)
{}

} // namespace authentication {
} // namespace http {
} // namespace mesos {
