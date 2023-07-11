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

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/type/DecimalUtil.h"
#include "velox/functions/sparksql/DecimalVectorFunctions.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <class T, bool nullOnOverflow>
class MakeDecimalFunction final : public exec::VectorFunction {
 public:
  explicit MakeDecimalFunction(uint8_t precision) : precision_(precision) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 3);
    context.ensureWritable(rows, outputType, resultRef);
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto unscaledVec = decodedArgs.at(0);
    auto result = resultRef->asUnchecked<FlatVector<T>>()->mutableRawValues();
    if constexpr (std::is_same_v<T, int64_t>) {
      int128_t bound = DecimalUtil::kPowersOfTen[precision_];
      rows.applyToSelected([&](int row) {
        auto unscaled = unscaledVec->valueAt<int64_t>(row);
        if (unscaled <= -bound || unscaled >= bound) {
          // Requested precision is too low to represent this value.
          if constexpr (nullOnOverflow) {
            resultRef->setNull(row, true);
          } else {
            VELOX_USER_FAIL(
                "Unscaled value {} too large for precision {}",
                unscaled,
                static_cast<int32_t>(precision_));
          }
        } else {
          result[row] = unscaled;
        }
      });
    } else {
      rows.applyToSelected([&](int row) {
        int128_t unscaled = unscaledVec->valueAt<int64_t>(row);
        result[row] = unscaled;
      });
    }
  }

 private:
  uint8_t precision_;
};

// Return the unscaled bigint value of a decimal, assuming it
// fits in a bigint. Only short decimal input is accepted.
class UnscaledValueFunction final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK(
        args[0]->type()->isShortDecimal(), "ShortDecimal type is required.");
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto decimalVector = decodedArgs.at(0);
    context.ensureWritable(rows, BIGINT(), result);
    result->clearNulls(rows);
    auto flatResult =
        result->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();
    rows.applyToSelected([&](auto row) {
      flatResult[row] = decimalVector->valueAt<int64_t>(row);
    });
  }
};

class DecimalRoundFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto aType = args[0]->type();
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
    auto [rPrecision, rScale] = getDecimalPrecisionScale(*resultType);
    int32_t scale = 0;
    if (args.size() > 1) {
      VELOX_USER_CHECK(args[1]->isConstantEncoding());
      scale = args[1]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
    }
    if (resultType->isShortDecimal()) {
      if (aType->isShortDecimal()) {
        applyRoundRows<int64_t, int64_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      } else {
        applyRoundRows<int64_t, int128_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      }
    } else {
      if (aType->isShortDecimal()) {
        applyRoundRows<int128_t, int64_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      } else {
        applyRoundRows<int128_t, int128_t>(
            rows,
            args,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale,
            resultType,
            context,
            result);
      }
    }
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

 private:
  template <typename R /* Result type */>
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  template <typename R /* Result */, typename A /* Argument */>
  void applyRoundRows(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t rPrecision,
      uint8_t rScale,
      int32_t scale,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Single-arg deterministic functions receive their only
    // argument as flat or constant only.
    auto rawResults = prepareResults<R>(rows, resultType, context, result);
    if (args[0]->isConstantEncoding()) {
      // Fast path for constant vectors.
      auto constant = args[0]->asUnchecked<ConstantVector<A>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        applyRound<R, A>(
            rawResults[row],
            constant,
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale);
      });
    } else {
      // Fast path for flat.
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        applyRound<R, A>(
            rawResults[row],
            rawA[row],
            aPrecision,
            aScale,
            rPrecision,
            rScale,
            scale);
      });
    }
  }

  template <typename R, typename A>
  inline void applyRound(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t rPrecision,
      uint8_t rScale,
      int32_t scale) const {
    if (scale >= 0) {
      auto rescaledValue = DecimalUtil::rescaleWithRoundUp<A, R>(
          a, aPrecision, aScale, rPrecision, rScale);
      VELOX_DCHECK(rescaledValue.has_value());
      r = rescaledValue.value();
    } else {
      auto reScaleFactor = DecimalUtil::kPowersOfTen[aScale - scale];
      DecimalUtil::divideWithRoundUp<R, A, int128_t>(
          r, a, reScaleFactor, false, 0, 0);
      r = r * DecimalUtil::kPowersOfTen[-scale];
    }
  }
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>>
makeDecimalByUnscaledValueSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("r_precision", "a_precision")
              .integerVariable("r_scale", "a_scale")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("bigint")
              .constantArgumentType("DECIMAL(a_precision, a_scale)")
              .constantArgumentType("boolean")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeMakeDecimalByUnscaledValue(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  VELOX_CHECK_EQ(inputArgs.size(), 3);
  auto type = inputArgs[1].type;
  auto nullOnOverflow =
      inputArgs[2].constantValue->as<ConstantVector<bool>>()->valueAt(0);
  if (type->isShortDecimal()) {
    if (nullOnOverflow) {
      return std::make_shared<MakeDecimalFunction<int64_t, true>>(
          type->asShortDecimal().precision());
    } else {
      return std::make_shared<MakeDecimalFunction<int64_t, false>>(
          type->asShortDecimal().precision());
    }
  } else {
    if (nullOnOverflow) {
      return std::make_shared<MakeDecimalFunction<int128_t, true>>(
          type->asLongDecimal().precision());
    } else {
      return std::make_shared<MakeDecimalFunction<int128_t, false>>(
          type->asLongDecimal().precision());
    }
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
unscaledValueSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision", "min(18, a_precision + 1)")
              .integerVariable("a_scale")
              .returnType("bigint")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .build()};
}

std::unique_ptr<exec::VectorFunction> makeUnscaledValue() {
  return std::make_unique<UnscaledValueFunction>();
}

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_decimal_round,
    std::vector<std::shared_ptr<exec::FunctionSignature>>{},
    std::make_unique<DecimalRoundFunction>());
} // namespace facebook::velox::functions::sparksql
