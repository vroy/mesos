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

#include "master/allocator/sorter/drf/sorter.hpp"

#include <set>
#include <string>
#include <vector>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>
#include <mesos/values.hpp>

#include <process/pid.hpp>

#include <stout/check.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/strings.hpp>

using std::set;
using std::string;
using std::vector;

using process::UPID;

namespace mesos {
namespace internal {
namespace master {
namespace allocator {


// Returns true if `left` is a superset of `right`.
//
// TODO(neilc): Only used for assertions; remove this or implement via
// operator overloading?
static bool allocationContains(
    const hashmap<SlaveID, Resources>& left,
    const hashmap<SlaveID, Resources>& right)
{
  foreachpair (const SlaveID& slaveId, const Resources& resources, right) {
    if (!left.contains(slaveId)) {
      return false;
    }
    if (!left.at(slaveId).contains(resources)) {
      return false;
    }
  }

  return true;
}


// Returns true if `left` is (structurally) equal to `right`.
//
// TODO(neilc): Only used for assertions; remove this or implement via
// operator overloading?
static bool allocationEqual(
    const hashmap<SlaveID, Resources>& left,
    const hashmap<SlaveID, Resources>& right)
{
  return allocationContains(left, right) && allocationContains(right, left);
}


DRFSorter::DRFSorter(
    const UPID& allocator,
    const string& metricsPrefix)
  : metrics(Metrics(allocator, *this, metricsPrefix)) {}


DRFSorter::~DRFSorter()
{
  std::function<void (Client*)> releaseSubtree =
    [this, &releaseSubtree](Client* node) {
    checkInvariants(node);

    foreach (Client* child, node->children) {
      releaseSubtree(child);
    }

    delete node;
  };

  releaseSubtree(root);
}


void DRFSorter::initialize(
    const Option<set<string>>& _fairnessExcludeResourceNames)
{
  fairnessExcludeResourceNames = _fairnessExcludeResourceNames;
}


void DRFSorter::add(const string& name)
{
  vector<string> nameElements = strings::tokenize(name, "/");
  CHECK(!nameElements.empty());

  Client* current = root;
  Client* lastCreatedNode = nullptr;
  string path;

  foreach (const string& element, nameElements) {
    bool found = false;

    if (path.empty()) {
      path = element;
    } else {
      path = path + "/" + element;
    }

    foreach (Client* child, current->children) {
      if (child->name == element) {
        found = true;
        current = child;
        break;
      }
    }

    if (!found) {
      // We didn't find `element`, so add a new child to `current`. If
      // adding this child would result in turning `current` from a
      // leaf node into an internal node, we need to create an
      // additional child node: `current` must have been associated
      // with a client, and clients must always be associated with
      // leaf nodes.
      //
      // There are two exceptions: if `current` is the root node or it
      // was just created by the current `add()` call, it does not
      // correspond to a client, so we don't create an extra child.
      if (current->children.empty() &&
          current != root &&
          current != lastCreatedNode) {
        Client* parent = CHECK_NOTNULL(current->parent);

        // Create a new internal node to take the position of `current`.
        Client* internal = new Client(current->name, current->path, parent);
        internal->allocation = current->allocation;

        // Create a new leaf node to replace `current`.
        Client* leaf = new Client(".", current->path + "/.", internal);
        leaf->allocation = current->allocation;
        leaf->active = current->active;
        leaf->share = current->share;

        // Update tree structure.
        parent->removeChild(current);
        parent->addChild(internal);
        internal->addChild(leaf);

        // Update entry in lookup table. We add an entry for `leaf`
        // using the path for leaf's parent, which is the "true" path
        // to this leaf node.
        CHECK(clients.contains(current->path));
        CHECK_EQ(current, clients.at(current->path));
        clients[current->path] = leaf;

        delete current;

        current = internal;
      }

      Client* newChild = new Client(element, path, current);
      current->addChild(newChild);

      current = newChild;
      lastCreatedNode = newChild;
    }
  }

  // `current` is the node associated with the last element of the
  // path. If we didn't add `current` to the tree above, create a leaf
  // node now. For example, if the tree contains "a/b" and we add a
  // new client "a", we want to create a new leaf node "a/."  here.
  if (current != lastCreatedNode) {
    Client* newChild = new Client(".", current->path + "/.", current);
    current->addChild(newChild);
    current = newChild;
  }

  // Add a new entry to the lookup table.
  CHECK(!clients.contains(path));
  clients[path] = current;

  if (metrics.isSome()) {
    metrics->add(name);
  }
}


void DRFSorter::remove(const string& name)
{
  Client* current = CHECK_NOTNULL(find(name));

  // Save a copy of the leaf node's allocated resources, because we
  // destroy the leaf node below.
  const hashmap<SlaveID, Resources> leafAllocation =
    current->allocation.resources;

  // Remove the lookup table entry for the client.
  CHECK(clients.contains(name));
  clients.erase(name);

  // To remove a client from the tree, we have to do two things:
  //
  //   (1) Update the tree structure to reflect the removal of the
  //       client. This means removing the client's leaf node, then
  //       walking back up the tree to remove any internal nodes that
  //       are now unnecessary.
  //
  //   (2) Update allocations of ancestor nodes to reflect the removal
  //       of the client.
  //
  // We do both things at once: find the leaf node, remove it, and
  // walk up the tree, updating ancestor allocations and removing
  // ancestors when possible.
  while (current != root) {
    Client* parent = CHECK_NOTNULL(current->parent);

    // Update `parent` to reflect the fact that the resources in the
    // leaf node are no longer allocated to the subtree rooted at
    // `parent`. We skip `root`, because we never update the
    // allocation made to the root node.
    if (parent != root) {
      foreachpair (const SlaveID& slaveId,
                   const Resources& resources,
                   leafAllocation) {
        parent->allocation.subtract(slaveId, resources);
      }
      updateShare(parent);
    }

    if (current->children.empty()) {
      parent->removeChild(current);
      delete current;
    } else if (current->children.size() == 1) {
      // If `current` has only one child that was created to
      // accommodate the insertion of `name` (see `DRFSorter::add()`),
      // we can remove the child node and turn `current` back into a
      // leaf node.
      Client* child = *(current->children.begin());

      if (child->name == ".") {
        CHECK(child->children.empty());
        CHECK(clients.contains(current->path));
        CHECK_EQ(child, clients.at(current->path));
        CHECK(allocationEqual(current->allocation.resources,
                              child->allocation.resources));

        current->active = child->active;
        current->removeChild(child);

        clients[current->path] = current;

        delete child;
      }
    }

    current = parent;
  }

  if (metrics.isSome()) {
    metrics->remove(name);
  }
}


void DRFSorter::activate(const string& name)
{
  Client* client = CHECK_NOTNULL(find(name));
  client->active = true;
}


void DRFSorter::deactivate(const string& name)
{
  Client* client = CHECK_NOTNULL(find(name));
  client->active = false;
}


void DRFSorter::updateWeight(const string& path, double weight)
{
  weights[path] = weight;

  // TODO(neilc): It would be possible to avoid dirtying the tree
  // here, but it doesn't seem worth the complexity.
  dirty = true;
}


void DRFSorter::update(
    const string& name,
    const SlaveID& slaveId,
    const Resources& oldAllocation,
    const Resources& newAllocation)
{
  // TODO(bmahler): Check invariants between old and new allocations.
  // Namely, the roles and quantities of resources should be the same!
  // Otherwise, we need to ensure we re-calculate the shares, as
  // is being currently done, for safety.

  Client* current = CHECK_NOTNULL(find(name));

  // NOTE: We don't currently update the `allocation` for the root
  // node. This is debatable, but the current implementation doesn't
  // require looking at the allocation of the root node.
  while (current != root) {
    current->allocation.update(slaveId, oldAllocation, newAllocation);
    current = CHECK_NOTNULL(current->parent);
  }

  // Just assume the total has changed, per the TODO above.
  dirty = true;
}


void DRFSorter::allocated(
    const string& name,
    const SlaveID& slaveId,
    const Resources& resources)
{
  Client* current = CHECK_NOTNULL(find(name));

  // NOTE: We don't currently update the `allocation` for the root
  // node. This is debatable, but the current implementation doesn't
  // require looking at the allocation of the root node.
  while (current != root) {
    current->allocation.add(slaveId, resources);
    updateShare(current);

    current = CHECK_NOTNULL(current->parent);
  }
}


void DRFSorter::unallocated(
    const string& name,
    const SlaveID& slaveId,
    const Resources& resources)
{
  Client* current = CHECK_NOTNULL(find(name));

  // NOTE: We don't currently update the `allocation` for the root
  // node. This is debatable, but the current implementation doesn't
  // require looking at the allocation of the root node.
  while (current != root) {
    current->allocation.subtract(slaveId, resources);
    updateShare(current);

    current = CHECK_NOTNULL(current->parent);
  }
}


const hashmap<SlaveID, Resources>& DRFSorter::allocation(
    const string& name) const
{
  Client* client = CHECK_NOTNULL(find(name));
  return client->allocation.resources;
}


const Resources& DRFSorter::allocationScalarQuantities(
    const string& name) const
{
  Client* client = CHECK_NOTNULL(find(name));
  return client->allocation.scalarQuantities;
}


hashmap<string, Resources> DRFSorter::allocation(const SlaveID& slaveId) const
{
  // TODO(jmlvanre): We can index the allocation by slaveId to make this faster.
  // It is a tradeoff between speed vs. memory. For now we use existing data
  // structures.

  hashmap<string, Resources> result;

  std::function<void (Client*)> traverseSubtree =
    [slaveId, &result, &traverseSubtree](Client* node) {
    // If this is a leaf node, check if it has allocated resources for
    // the given `slaveId`.
    if (node->children.empty()) {
      if (node->allocation.resources.contains(slaveId)) {
        // It is safe to use `at()` here because we've just checked
        // the existence of the key. This avoids un-necessary copies.
        if (node->name == ".") {
          const string& path = node->parent->path;
          CHECK(!result.contains(path));
          result.emplace(path, node->allocation.resources.at(slaveId));
        } else {
          const string& path = node->path;
          CHECK(!result.contains(path));
          result.emplace(path, node->allocation.resources.at(slaveId));
        }
      }

      return;
    }

    foreach (Client* child, node->children) {
      traverseSubtree(child);
    }
  };

  traverseSubtree(root);

  return result;
}


Resources DRFSorter::allocation(
    const string& name,
    const SlaveID& slaveId) const
{
  Client* client = CHECK_NOTNULL(find(name));

  if (client->allocation.resources.contains(slaveId)) {
    return client->allocation.resources.at(slaveId);
  }

  return Resources();
}


const Resources& DRFSorter::totalScalarQuantities() const
{
  return total_.scalarQuantities;
}


void DRFSorter::add(const SlaveID& slaveId, const Resources& resources)
{
  if (!resources.empty()) {
    // Add shared resources to the total quantities when the same
    // resources don't already exist in the total.
    const Resources newShared = resources.shared()
      .filter([this, slaveId](const Resource& resource) {
        return !total_.resources[slaveId].contains(resource);
      });

    total_.resources[slaveId] += resources;

    const Resources scalarQuantities =
      (resources.nonShared() + newShared).createStrippedScalarQuantity();

    total_.scalarQuantities += scalarQuantities;

    foreach (const Resource& resource, scalarQuantities) {
      total_.totals[resource.name()] += resource.scalar();
    }

    // We have to recalculate all shares when the total resources
    // change, but we put it off until `sort` is called so that if
    // something else changes before the next allocation we don't
    // recalculate everything twice.
    dirty = true;
  }
}


void DRFSorter::remove(const SlaveID& slaveId, const Resources& resources)
{
  if (!resources.empty()) {
    CHECK(total_.resources.contains(slaveId));
    CHECK(total_.resources[slaveId].contains(resources))
      << total_.resources[slaveId] << " does not contain " << resources;

    total_.resources[slaveId] -= resources;

    // Remove shared resources from the total quantities when there
    // are no instances of same resources left in the total.
    const Resources absentShared = resources.shared()
      .filter([this, slaveId](const Resource& resource) {
        return !total_.resources[slaveId].contains(resource);
      });

    const Resources scalarQuantities =
      (resources.nonShared() + absentShared).createStrippedScalarQuantity();

    foreach (const Resource& resource, scalarQuantities) {
      total_.totals[resource.name()] -= resource.scalar();
    }

    CHECK(total_.scalarQuantities.contains(scalarQuantities));
    total_.scalarQuantities -= scalarQuantities;

    if (total_.resources[slaveId].empty()) {
      total_.resources.erase(slaveId);
    }

    dirty = true;
  }
}


vector<string> DRFSorter::sort()
{
  if (dirty) {
    std::function<void (Client*)> updateSubtree =
      [this, &updateSubtree](Client* node) {
      std::set<Client*, Client::DRFComparator> temp;

      foreach (Client* child, node->children) {
        child->share = calculateShare(*child);

        temp.insert(child);
      }

      node->children = temp;

      foreach (Client* child, node->children) {
        updateSubtree(child);
      }
    };

    updateSubtree(root);

    dirty = false;
  }

  // Return the leaf nodes in the tree. The children of each node are
  // already sorted by fair-share.
  vector<string> result;

  std::function<void (Client*)> listSubtree =
    [this, &listSubtree, &result](Client* node) {
    if (node->children.empty()) {
      if (node->active && node != root) {
        if (node->name == ".") {
          result.push_back(node->parent->path);
        } else {
          result.push_back(node->path);
        }
      }
      return;
    }

    foreach (Client* child, node->children) {
      listSubtree(child);
    }
  };

  listSubtree(root);

  return result;
}


bool DRFSorter::contains(const string& name) const
{
  return find(name) != nullptr;
}


int DRFSorter::count() const
{
  return clients.size();
}


void DRFSorter::checkInvariants(Client* node) const
{
  if (node == root) {
    return;
  }

  if (node->name == ".") {
    CHECK(node->children.empty());
  }

  foreach (Client* child, node->children) {
    CHECK_EQ(node, child->parent);
  }

  // The parent should have all the resources in the child. We skip
  // the root node because we currently don't update the root node's
  // `allocation` field.
  if (node->parent == root) {
    return;
  }

  // TODO(neilc): We can make this invariant tighter; the resources in
  // a node's children should _exactly_ sum to the resources in the
  // node.
  Client* parent = node->parent;

  CHECK(allocationContains(parent->allocation.resources,
                           node->allocation.resources));
}


void DRFSorter::updateShare(Client* node)
{
  // Don't bother updating this node's share if we're going to
  // re-calculate the share of all nodes in the tree.
  if (dirty) {
    return;
  }

  node->share = calculateShare(*node);

  // Remove and re-insert the node to ensure its position in its
  // parent's ordered list of children is correct.
  node->parent->removeChild(node);
  node->parent->addChild(node);
}


double DRFSorter::calculateShare(const Client& client) const
{
  double share = 0.0;

  // TODO(benh): This implementation of "dominant resource fairness"
  // currently does not take into account resources that are not
  // scalars.

  foreachpair (const string& resourceName,
               const Value::Scalar& scalar,
               total_.totals) {
    // Filter out the resources excluded from fair sharing.
    if (fairnessExcludeResourceNames.isSome() &&
        fairnessExcludeResourceNames->count(resourceName) > 0) {
      continue;
    }

    if (scalar.value() > 0.0 &&
        client.allocation.totals.contains(resourceName)) {
      const double allocation =
        client.allocation.totals.at(resourceName).value();

      share = std::max(share, allocation / scalar.value());
    }
  }

  return share / clientWeight(client);
}


double DRFSorter::clientWeight(const Client& client) const
{
  Option<double> weight = weights.get(client.path);

  if (weight.isNone()) {
    return 1.0;
  }

  return weight.get();
}


Client* DRFSorter::find(const string& name) const
{
  Option<Client*> client = clients.get(name);

  if (client.isNone()) {
    return nullptr;
  }

  return client.get();
}

} // namespace allocator {
} // namespace master {
} // namespace internal {
} // namespace mesos {
