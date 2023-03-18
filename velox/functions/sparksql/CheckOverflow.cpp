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
#include "velox/functions/sparksql/CheckOverflow.h"

#include "velox/expression/DecodedArgs.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {
template <typename TInput, typename TOutput>
class CheckOverflowFunction final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 3);
    auto fromType = args[0]->type();
    auto toType = args[1]->type();
    context.ensureWritable(rows, toType, resultRef);

    auto result =
        resultRef->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto decimalValue = decodedArgs.at(0);
    VELOX_CHECK(decodedArgs.at(2)->isConstantMapping());
    auto nullOnOverflow = decodedArgs.at(2)->valueAt<bool>(0);

    const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
    const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);
    rows.applyToSelected([&](int row) {
      auto rescaledValue = DecimalUtil::rescaleWithRoundUp<TInput, TOutput>(
          decimalValue->valueAt<TInput>(row),
          fromPrecisionScale.first,
          fromPrecisionScale.second,
          toPrecisionScale.first,
          toPrecisionScale.second,
          nullOnOverflow);
      if (rescaledValue.has_value()) {
        result[row] = rescaledValue.value();
      } else {
        resultRef->setNull(row, true);
      }
    });
  }
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>>
checkOverflowSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("r_precision")
              .integerVariable("r_scale")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(r_precision, r_scale)")
              .argumentType("boolean")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeCheckOverflow(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 3);
  auto fromType = inputArgs[0].type;
  auto toType = inputArgs[1].type;
  if (toType->kind() == TypeKind::SHORT_DECIMAL) {
    if (fromType->kind() == TypeKind::SHORT_DECIMAL) {
      std::cout << "from and to is short decimal" << std::endl;
      return std::make_shared<
          CheckOverflowFunction<UnscaledShortDecimal, UnscaledShortDecimal>>();
    } else {
      return std::make_shared<
          CheckOverflowFunction<UnscaledLongDecimal, UnscaledShortDecimal>>();
    }
  } else {
    if (fromType->kind() == TypeKind::SHORT_DECIMAL) {
      return std::make_shared<
          CheckOverflowFunction<UnscaledShortDecimal, UnscaledLongDecimal>>();
    } else {
      return std::make_shared<
          CheckOverflowFunction<UnscaledLongDecimal, UnscaledLongDecimal>>();
    }
  }
}

} // namespace facebook::velox::functions::sparksql
