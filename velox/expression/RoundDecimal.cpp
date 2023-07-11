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

#include "velox/expression/RoundDecimal.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::exec {

const char* const kRoundDecimal = "decimal_round";

TypePtr RoundDecimalCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  VELOX_FAIL("RoundDecimal expressions do not support type resolution.");
}

ExprPtr RoundDecimalCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage) {
  VELOX_USER_CHECK(
      compiledChildren.size() <= 2 && compiledChildren.size() > 0,
      "RoundDecimal statements expect 1 or 2 arguments, received {}",
      compiledChildren.size());
  VELOX_USER_CHECK(
      compiledChildren[0]->type()->isDecimal(),
      "First argument of decimal_round should be decimal");
  if (compiledChildren.size() > 1) {
    VELOX_USER_CHECK_EQ(
        compiledChildren[1]->type()->kind(),
        TypeKind::INTEGER,
        "Second argument of decimal_round should be integer");
  }
  // TODO, wait for PR https://github.com/facebookincubator/velox/pull/5866
  core::QueryConfig config({});
  static auto roundDecimalVectorFunction =
      vectorFunctionFactories().withRLock([&config](auto& functionMap) {
        auto functionIterator = functionMap.find(exec::kRoundDecimal);
        return functionIterator->second.factory(
            exec::kRoundDecimal, {}, config);
      });
  return std::make_shared<Expr>(
      type,
      std::move(compiledChildren),
      roundDecimalVectorFunction,
      "decimal_round",
      trackCpuUsage);
}
} // namespace facebook::velox::exec
