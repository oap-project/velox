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

#include "velox/type/Filter.h"
#include "velox/type/Type.h"

namespace facebook::velox::substrait {

/// Return whether the type is String type.
bool isString(const TypePtr& type);

/// Return the occupied bytes for a variable of this type.
int64_t bytesOfType(const TypePtr& type);

/// Return the Velox type according to the typename.
TypePtr toVeloxType(const std::string& typeName);

#ifndef RANGETRAITS_H
#define RANGETRAITS_H

// Traits used to map type kind to the range used in Filter.
template <TypeKind KIND>
struct RangeTraits {};

template <>
struct RangeTraits<TypeKind::INTEGER> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = int32_t;
};

template <>
struct RangeTraits<TypeKind::BIGINT> {
  using RangeType = common::BigintRange;
  using MultiRangeType = common::BigintMultiRange;
  using NativeType = int64_t;
};

template <>
struct RangeTraits<TypeKind::DOUBLE> {
  using RangeType = common::DoubleRange;
  using MultiRangeType = common::MultiRange;
  using NativeType = double;
};

template <>
struct RangeTraits<TypeKind::VARCHAR> {
  using RangeType = common::BytesRange;
  using MultiRangeType = common::MultiRange;
  using NativeType = std::string;
};

#endif /* RANGETRAITS_H */

} // namespace facebook::velox::substrait
