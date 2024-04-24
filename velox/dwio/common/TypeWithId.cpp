/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwio::common {

using velox::Type;
using velox::TypeKind;

namespace {
std::vector<std::shared_ptr<const TypeWithId>> toShared(
    std::vector<std::unique_ptr<TypeWithId>> nodes) {
  std::vector<std::shared_ptr<const TypeWithId>> result;
  result.reserve(nodes.size());
  for (auto&& node : nodes) {
    result.emplace_back(std::move(node));
  }
  return result;
}

TypePtr adjustNameAsLowerCase(const TypePtr& type) {
  if (auto rowTypePtr = asRowType(type)) {
    std::vector<std::string> names;
    names.reserve(rowTypePtr->names().size());
    std::vector<TypePtr> types = rowTypePtr->children();
    for (const auto& name : rowTypePtr->names()) {
      std::string childName = name;
      folly::toLowerAscii(childName);
      names.emplace_back(childName);
    }
    return TypeFactory<TypeKind::ROW>::create(
        std::move(names), std::move(types));
  }
  return type;
}
} // namespace

TypeWithId::TypeWithId(
    std::shared_ptr<const Type> type,
    std::vector<std::unique_ptr<TypeWithId>>&& children,
    uint32_t id,
    uint32_t maxId,
    uint32_t column)
    : type_{std::move(type)},
      parent_{nullptr},
      id_{id},
      maxId_{maxId},
      column_{column},
      children_{toShared(std::move(children))} {
  for (auto& child : children_) {
    const_cast<const TypeWithId*&>(child->parent_) = this;
  }
}

std::unique_ptr<TypeWithId> TypeWithId::create(
    const std::shared_ptr<const Type>& root,
    uint32_t next) {
  return create(root, next, 0);
}

std::unique_ptr<TypeWithId> TypeWithId::duplicate(bool nameAsLowerCase) const {
  if (children_.empty()) {
    std::vector<std::unique_ptr<TypeWithId>> children;
    return std::make_unique<TypeWithId>(
        nameAsLowerCase ? adjustNameAsLowerCase(type_) : type_,
        std::move(children),
        id_,
        maxId_,
        column_);
  }
  std::vector<std::unique_ptr<TypeWithId>> children;
  children.reserve(children_.size());
  for (const auto& child : children_) {
    children.emplace_back(child->duplicate(nameAsLowerCase));
  }
  return std::make_unique<TypeWithId>(
      nameAsLowerCase ? adjustNameAsLowerCase(type_) : type_,
      std::move(children),
      id_,
      maxId_,
      column_);
}

uint32_t TypeWithId::size() const {
  return children_.size();
}

const std::shared_ptr<const TypeWithId>& TypeWithId::childAt(
    uint32_t idx) const {
  return children_.at(idx);
}

std::unique_ptr<TypeWithId> TypeWithId::create(
    const std::shared_ptr<const Type>& type,
    uint32_t& next,
    uint32_t column) {
  DWIO_ENSURE_NOT_NULL(type);
  const uint32_t myId = next++;
  std::vector<std::unique_ptr<TypeWithId>> children;
  children.reserve(type->size());
  auto offset = 0;
  for (const auto& child : *type) {
    children.emplace_back(create(
        child,
        next,
        (myId == 0 && type->kind() == TypeKind::ROW) ? offset++ : column));
  }
  const uint32_t maxId = next - 1;
  return std::make_unique<TypeWithId>(
      type, std::move(children), myId, maxId, column);
}

} // namespace facebook::velox::dwio::common
