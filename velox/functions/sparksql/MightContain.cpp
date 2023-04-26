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
#include <optional>

#include "velox/functions/sparksql/MightContain.h"

#include "velox/common/base/BloomFilter.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {
class BloomFilterMightContainFunction final : public exec::VectorFunction {
 public:
  explicit BloomFilterMightContainFunction(const char * data) {
    if (data) {
      output_.merge(data);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 2);
    context.ensureWritable(rows, BOOLEAN(), resultRef);
    if (!output_.isSet()) {
      auto localResult = std::make_shared<ConstantVector<bool>>(
        context.pool(), rows.size(), false /*isNull*/, BOOLEAN(), false);
      context.moveOrCopyResult(localResult, rows, resultRef);
      return;
    }
    auto& result = *resultRef->as<FlatVector<bool>>();
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto value = decodedArgs.at(1);

    rows.applyToSelected([&](int row) {
      auto contain = output_.mayContain(
          folly::hasher<int64_t>()(value->valueAt<int64_t>(row)));
      result.set(row, contain);
    });
  }

 private:
  BloomFilter<StlAllocator<uint64_t>> output_;
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> mightContainSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .constantArgumentType("varbinary")
              .argumentType("bigint")
              .build()};
}

std::unique_ptr<exec::VectorFunction> makeMightContain(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  const auto& constantValue = inputArgs[0].constantValue;
  if (constantValue) {
    auto constantInput =
      std::dynamic_pointer_cast<ConstantVector<StringView>>(constantValue);
    if (!constantInput->isNullAt(0)) {
      return std::make_unique<BloomFilterMightContainFunction>(
        constantInput->valueAt(0).data());
    }
  }
  return std::make_unique<BloomFilterMightContainFunction>(nullptr);
}

} // namespace facebook::velox::functions::sparksql
