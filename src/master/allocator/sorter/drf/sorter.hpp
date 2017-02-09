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

#ifndef __MASTER_ALLOCATOR_SORTER_DRF_SORTER_HPP__
#define __MASTER_ALLOCATOR_SORTER_DRF_SORTER_HPP__

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>
#include <mesos/values.hpp>

#include <stout/check.hpp>
#include <stout/hashmap.hpp>
#include <stout/option.hpp>

#include "master/allocator/sorter/drf/metrics.hpp"

#include "master/allocator/sorter/sorter.hpp"


namespace mesos {
namespace internal {
namespace master {
namespace allocator {

// TODO(neilc): Consider renaming this struct; it doesn't correspond
// to a _client_ of the sorter, it corresponds to a node in the role
// tree (either internal or leaf), which may or may not correspond to
// a sorter client.
struct Client
{
  Client(const std::string& _name, const std::string& _path, Client* _parent)
    : name(_name),
      path(_path),
      share(0),
      active(true),
      parent(_parent) {}

  // The label of the edge from this node's parent to the
  // node. "Implicit" leaf nodes are labeled ".".
  //
  // TODO(neilc): Consider naming implicit leaf nodes in a clearer
  // way, e.g., by making `name` an Option?
  std::string name;

  // Complete path from root to node.
  std::string path;

  double share;

  // TODO(neilc): Only meaningful for clients. Replace this with a
  // three-valued `enum`?
  bool active;

  // Compares two clients according to DRF share.
  struct DRFComparator
  {
    bool operator()(const Client* client1, const Client* client2) const {
      if (client1->share != client2->share) {
        return client1->share < client2->share;
      }

      if (client1->allocation.count != client2->allocation.count) {
        return client1->allocation.count < client2->allocation.count;
      }

      return client1->path < client2->path;
    }
  };

  Client* parent;
  std::set<Client*, DRFComparator> children;

  std::set<Client*, DRFComparator>::iterator findChild(Client* child) {
    // NOTE: We use `std::find` rather than `std::set::find`. The
    // former matches on strict (pointer) equality, while the latter
    // would use `DRFComparator`.
    return std::find(children.begin(), children.end(), child);
  }

  void removeChild(Client* child) {
    auto it = findChild(child);
    CHECK(it != children.end());

    children.erase(it);
  }

  void addChild(Client* child) {
    CHECK(findChild(child) == children.end());

    children.insert(child);
  }

  // Allocation for a client.
  struct Allocation {
    Allocation() : count(0) {}

    void add(const SlaveID& slaveId, const Resources& toAdd) {
      // Add shared resources to the allocated quantities when the same
      // resources don't already exist in the allocation.
      const Resources sharedToAdd = toAdd.shared()
        .filter([this, slaveId](const Resource& resource) {
            return !resources[slaveId].contains(resource);
        });

      const Resources quantitiesToAdd =
        (toAdd.nonShared() + sharedToAdd).createStrippedScalarQuantity();

      resources[slaveId] += toAdd;
      scalarQuantities += quantitiesToAdd;

      foreach (const Resource& resource, quantitiesToAdd) {
        totals[resource.name()] += resource.scalar();
      }

      count++;
    }

    void subtract(const SlaveID& slaveId, const Resources& toRemove) {
      CHECK(resources.contains(slaveId));
      CHECK(resources.at(slaveId).contains(toRemove));

      resources[slaveId] -= toRemove;

      // Remove shared resources from the allocated quantities when there
      // are no instances of same resources left in the allocation.
      const Resources sharedToRemove = toRemove.shared()
        .filter([this, slaveId](const Resource& resource) {
            return !resources[slaveId].contains(resource);
        });

      const Resources quantitiesToRemove =
        (toRemove.nonShared() + sharedToRemove).createStrippedScalarQuantity();

      foreach (const Resource& resource, quantitiesToRemove) {
        totals[resource.name()] -= resource.scalar();
      }

      CHECK(scalarQuantities.contains(quantitiesToRemove));
      scalarQuantities -= quantitiesToRemove;

      if (resources[slaveId].empty()) {
        resources.erase(slaveId);
      }
    }

    void update(
        const SlaveID& slaveId,
        const Resources& oldAllocation,
        const Resources& newAllocation) {
      const Resources oldAllocationQuantity =
        oldAllocation.createStrippedScalarQuantity();
      const Resources newAllocationQuantity =
        newAllocation.createStrippedScalarQuantity();

      CHECK(resources[slaveId].contains(oldAllocation));
      CHECK(scalarQuantities.contains(oldAllocationQuantity));

      resources[slaveId] -= oldAllocation;
      resources[slaveId] += newAllocation;

      scalarQuantities -= oldAllocationQuantity;
      scalarQuantities += newAllocationQuantity;

      foreach (const Resource& resource, oldAllocationQuantity) {
        totals[resource.name()] -= resource.scalar();
      }

      foreach (const Resource& resource, newAllocationQuantity) {
        totals[resource.name()] += resource.scalar();
      }
    }

    // We store the number of times this client has been chosen for
    // allocation so that we can fairly share the resources across
    // clients that have the same share. Note that this information is
    // not persisted across master failovers, but since the point is
    // to equalize the 'allocations' across clients of the same
    // 'share' having allocations restart at 0 after a master failover
    // should be sufficient (famous last words.)
    uint64_t count;

    // We maintain multiple copies of each shared resource allocated
    // to a client, where the number of copies represents the number
    // of times this shared resource has been allocated to (and has
    // not been recovered from) a specific client.
    hashmap<SlaveID, Resources> resources;

    // Similarly, we aggregate scalars across slaves and omit information
    // about dynamic reservations, persistent volumes and sharedness of
    // the corresponding resource. See notes above.
    Resources scalarQuantities;

    // We also store a map version of `scalarQuantities`, mapping
    // the `Resource::name` to aggregated scalar. This improves the
    // performance of calculating shares. See MESOS-4694.
    //
    // TODO(bmahler): Ideally we do not store `scalarQuantities`
    // redundantly here, investigate performance improvements to
    // `Resources` to make this unnecessary.
    hashmap<std::string, Value::Scalar> totals;
  } allocation;
};


class DRFSorter : public Sorter
{
public:
  DRFSorter() = default;

  explicit DRFSorter(
      const process::UPID& allocator,
      const std::string& metricsPrefix);

  virtual ~DRFSorter();

  virtual void initialize(
      const Option<std::set<std::string>>& fairnessExcludeResourceNames);

  virtual void add(const std::string& name);

  virtual void remove(const std::string& name);

  virtual void activate(const std::string& name);

  virtual void deactivate(const std::string& name);

  virtual void updateWeight(const std::string& path, double weight);

  virtual void update(
      const std::string& name,
      const SlaveID& slaveId,
      const Resources& oldAllocation,
      const Resources& newAllocation);

  virtual void allocated(
      const std::string& name,
      const SlaveID& slaveId,
      const Resources& resources);

  virtual void unallocated(
      const std::string& name,
      const SlaveID& slaveId,
      const Resources& resources);

  virtual const hashmap<SlaveID, Resources>& allocation(
      const std::string& name) const;

  virtual const Resources& allocationScalarQuantities(
      const std::string& name) const;

  virtual hashmap<std::string, Resources> allocation(
      const SlaveID& slaveId) const;

  virtual Resources allocation(
      const std::string& name,
      const SlaveID& slaveId) const;

  virtual const Resources& totalScalarQuantities() const;

  virtual void add(const SlaveID& slaveId, const Resources& resources);

  virtual void remove(const SlaveID& slaveId, const Resources& resources);

  virtual std::vector<std::string> sort();

  virtual bool contains(const std::string& name) const;

  virtual int count() const;

private:
  void checkInvariants(Client* node) const;

  // Update the dominant resource share for the client and update its
  // position in the node's parent's ordered list of children.
  void updateShare(Client* node);

  // Returns the dominant resource share for the client.
  double calculateShare(const Client& client) const;

  // Resources (by name) that will be excluded from fair sharing.
  Option<std::set<std::string>> fairnessExcludeResourceNames;

  // Returns the weight associated with the given path. If no weight
  // has been configured, the default weight (1.0) is returned.
  double clientWeight(const Client& client) const;

  // Returns the client associated with the given path, or nullptr if
  // no such client was found. If found, the return value will be a
  // leaf node in the role tree.
  Client* find(const std::string& name) const;

  // If true, sort() will recalculate all shares.
  bool dirty = false;

  // The root node in the client tree.
  Client* root = new Client("", "", nullptr);

  // To speed lookups, we also keep a map from client names to the
  // leaf node that is associated with that client. There is an entry
  // in this map for every leaf node in the client tree (except for
  // the root when the tree is empty). Client paths in this map do NOT
  // contain the trailing "." label we use for leaf nodes.
  hashmap<std::string, Client*> clients;

  // Weights associated with paths in the client tree. Setting the
  // weight for a path influences the share of all clients in the
  // subtree rooted at that path. This hashmap might include weights
  // for paths that are not currently in the tree.
  hashmap<std::string, double> weights;

  // Total resources.
  struct Total {
    // We need to keep track of the resources (and not just scalar quantities)
    // to account for multiple copies of the same shared resources. We need to
    // ensure that we do not update the scalar quantities for shared resources
    // when the change is only in the number of copies in the sorter.
    hashmap<SlaveID, Resources> resources;

    // NOTE: Scalars can be safely aggregated across slaves. We keep
    // that to speed up the calculation of shares. See MESOS-2891 for
    // the reasons why we want to do that.
    //
    // NOTE: We omit information about dynamic reservations and persistent
    // volumes here to enable resources to be aggregated across slaves
    // more effectively. See MESOS-4833 for more information.
    //
    // Sharedness info is also stripped out when resource identities are
    // omitted because sharedness inherently refers to the identities of
    // resources and not quantities.
    Resources scalarQuantities;

    // We also store a map version of `scalarQuantities`, mapping
    // the `Resource::name` to aggregated scalar. This improves the
    // performance of calculating shares. See MESOS-4694.
    //
    // TODO(bmahler): Ideally we do not store `scalarQuantities`
    // redundantly here, investigate performance improvements to
    // `Resources` to make this unnecessary.
    hashmap<std::string, Value::Scalar> totals;
  } total_;

  // Metrics are optionally exposed by the sorter.
  friend Metrics;
  Option<Metrics> metrics;
};

} // namespace allocator {
} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // __MASTER_ALLOCATOR_SORTER_DRF_SORTER_HPP__
