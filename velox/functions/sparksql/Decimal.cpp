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
#include "velox/functions/sparksql/Decimal.h"

#include "velox/expression/DecodedArgs.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <typename TInput>
class RoundDecimalFunction final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 2);
    auto fromType = args[0]->type();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto decimalValue = decodedArgs.at(0);
    VELOX_CHECK(decodedArgs.at(1)->isConstantMapping());
    auto scale = decodedArgs.at(1)->valueAt<int32_t>(0);

    const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
    const auto& fromPrecision = fromPrecisionScale.first;
    const auto& fromScale = fromPrecisionScale.second;
    auto toPrecision = fromPrecision;
    auto toScale = fromScale;

    // Calculate the result data type based on spark logic.
    const auto& integralLeastNumDigits = fromPrecision - fromScale + 1;
    if (scale < 0) {
      const auto& newPrecision =
          std::max(integralLeastNumDigits, -fromScale + 1);
      toPrecision = std::min(newPrecision, 38);
      toScale = 0;
    } else {
      toScale = std::min(fromScale, scale);
      toPrecision = std::min(integralLeastNumDigits + toScale, 38);
    }

    rows.applyToSelected([&](int row) {
      if (toPrecision > 18) {
        context.ensureWritable(
            rows,
            DECIMAL(
                static_cast<uint8_t>(toPrecision),
                static_cast<uint8_t>(toScale)),
            resultRef);
        auto rescaledValue = DecimalUtil::rescaleWithRoundUp<TInput, int128_t>(
            decimalValue->valueAt<TInput>(row),
            fromPrecision,
            fromScale,
            toPrecision,
            toScale);
        auto result =
            resultRef->asUnchecked<FlatVector<int128_t>>()->mutableRawValues();
        if (rescaledValue.has_value()) {
          result[row] = rescaledValue.value();
        } else {
          resultRef->setNull(row, true);
        }
      } else {
        context.ensureWritable(
            rows,
            DECIMAL(
                static_cast<uint8_t>(toPrecision),
                static_cast<uint8_t>(toScale)),
            resultRef);
        auto rescaledValue = DecimalUtil::rescaleWithRoundUp<TInput, int64_t>(
            decimalValue->valueAt<TInput>(row),
            fromPrecision,
            fromScale,
            toPrecision,
            toScale);
        auto result =
            resultRef->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();
        if (rescaledValue.has_value()) {
          result[row] = rescaledValue.value();
        } else {
          resultRef->setNull(row, true);
        }
      }
    });
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> roundDecimalSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("r_precision", "min(38, a_precision)")
              .integerVariable("r_scale", "min(38, a_scale)")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("integer")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeRoundDecimal(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  auto fromType = inputArgs[0].type;
  if (fromType->isShortDecimal()) {
    return std::make_shared<RoundDecimalFunction<int64_t>>();
  }
  if (fromType->isLongDecimal()) {
    return std::make_shared<RoundDecimalFunction<int128_t>>();
  }

  switch (fromType->kind()) {
    default:
      VELOX_FAIL(
          "Not support this type {} in round_decimal", fromType->kindName())
  }
}
} // namespace facebook::velox::functions::sparksql
