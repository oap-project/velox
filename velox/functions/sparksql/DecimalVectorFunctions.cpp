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

// Rescale two inputs as the same scale and compare. Returns 0 when a is equal
// with b. Returns -1 when a is less than b. Returns 1 when a is greater than b.
template <typename T>
int32_t rescaleAndCompare(T a, T b, int32_t deltaScale) {
  T aScaled = a;
  T bScaled = b;
  if (deltaScale < 0) {
    aScaled = a * velox::DecimalUtil::kPowersOfTen[-deltaScale];
  } else if (deltaScale > 0) {
    bScaled = b * velox::DecimalUtil::kPowersOfTen[deltaScale];
  }
  if (aScaled == bScaled) {
    return 0;
  } else if (aScaled < bScaled) {
    return -1;
  } else {
    return 1;
  }
}

// Compare two decimals. Rescale one of them When they are of different scales.
int32_t decimalCompare(
    int128_t a,
    uint8_t aPrecision,
    uint8_t aScale,
    int128_t b,
    uint8_t bPrecision,
    uint8_t bScale) {
  int32_t deltaScale = aScale - bScale;
  // Check if we need 256-bits after adjusting the scale.
  bool need256 = (deltaScale < 0 &&
                  aPrecision - deltaScale > LongDecimalType::kMaxPrecision) ||
      (bPrecision + deltaScale > LongDecimalType::kMaxPrecision);
  if (need256) {
    return rescaleAndCompare<int256_t>(
        static_cast<int256_t>(a), static_cast<int256_t>(b), deltaScale);
  }
  return rescaleAndCompare<int128_t>(a, b, deltaScale);
}

// GreaterThan decimal compare function.
class Gt {
 public:
  inline static bool apply(
      int128_t a,
      uint8_t aPrecision,
      uint8_t aScale,
      int128_t b,
      uint8_t bPrecision,
      uint8_t bScale) {
    return decimalCompare(a, aPrecision, aScale, b, bPrecision, bScale) > 0;
  }
};

// GreaterThanOrEqual decimal compare function.
class Gte {
 public:
  inline static bool apply(
      int128_t a,
      uint8_t aPrecision,
      uint8_t aScale,
      int128_t b,
      uint8_t bPrecision,
      uint8_t bScale) {
    return decimalCompare(a, aPrecision, aScale, b, bPrecision, bScale) >= 0;
  }
};

// LessThan decimal compare function.
class Lt {
 public:
  inline static bool apply(
      int128_t a,
      uint8_t aPrecision,
      uint8_t aScale,
      int128_t b,
      uint8_t bPrecision,
      uint8_t bScale) {
    return decimalCompare(a, aPrecision, aScale, b, bPrecision, bScale) < 0;
  }
};

// LessThanOrEqual decimal compare function.
class Lte {
 public:
  inline static bool apply(
      int128_t a,
      uint8_t aPrecision,
      uint8_t aScale,
      int128_t b,
      uint8_t bPrecision,
      uint8_t bScale) {
    return decimalCompare(a, aPrecision, aScale, b, bPrecision, bScale) <= 0;
  }
};

// Equal decimal compare function.
class Eq {
 public:
  inline static bool apply(
      int128_t a,
      uint8_t aPrecision,
      uint8_t aScale,
      int128_t b,
      uint8_t bPrecision,
      uint8_t bScale) {
    return decimalCompare(a, aPrecision, aScale, b, bPrecision, bScale) == 0;
  }
};

// Class for decimal compare operations.
template <typename A, typename B, typename Operation /* Arithmetic operation */>
class DecimalCompareFunction : public exec::VectorFunction {
 public:
  DecimalCompareFunction(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale)
      : aPrecision_(aPrecision),
        aScale_(aScale),
        bPrecision_(bPrecision),
        bScale_(bScale) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    prepareResults(rows, resultType, context, result);

    // Fast path when the first argument is a flat vector.
    if (args[0]->isFlatEncoding()) {
      auto rawA = args[0]->asUnchecked<FlatVector<A>>()->mutableRawValues();

      if (args[1]->isConstantEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row,
              Operation::apply(
                  (int128_t)rawA[row],
                  aPrecision_,
                  aScale_,
                  (int128_t)constantB,
                  bPrecision_,
                  bScale_));
        });
        return;
      }

      if (args[1]->isFlatEncoding()) {
        auto rawB = args[1]->asUnchecked<FlatVector<B>>()->mutableRawValues();
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row,
              Operation::apply(
                  (int128_t)rawA[row],
                  aPrecision_,
                  aScale_,
                  (int128_t)rawB[row],
                  bPrecision_,
                  bScale_));
        });
        return;
      }
    } else {
      // Fast path when the first argument is encoded but the second is
      // constant.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto aDecoded = decodedArgs.at(0);
      auto aDecodedData = aDecoded->data<A>();

      if (args[1]->isConstantEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          auto value = aDecodedData[aDecoded->index(row)];
          result->asUnchecked<FlatVector<bool>>()->set(
              row,
              Operation::apply(
                  (int128_t)value,
                  aPrecision_,
                  aScale_,
                  (int128_t)constantB,
                  bPrecision_,
                  bScale_));
        });
        return;
      }
    }

    // Decode the input in all other cases.
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto aDecoded = decodedArgs.at(0);
    auto bDecoded = decodedArgs.at(1);

    auto aDecodedData = aDecoded->data<A>();
    auto bDecodedData = bDecoded->data<B>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto aValue = aDecodedData[aDecoded->index(row)];
      auto bValue = bDecodedData[bDecoded->index(row)];
      result->asUnchecked<FlatVector<bool>>()->set(
          row,
          Operation::apply(
              (int128_t)aValue,
              aPrecision_,
              aScale_,
              (int128_t)bValue,
              bPrecision_,
              bScale_));
    });
  }

 private:
  void prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
  }

  const uint8_t aPrecision_;
  const uint8_t aScale_;
  const uint8_t bPrecision_;
  const uint8_t bScale_;
};

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalCompareFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& aType = inputArgs[0].type;
  const auto& bType = inputArgs[1].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  if (aType->isShortDecimal()) {
    if (bType->isShortDecimal()) {
      return std::make_shared<
          DecimalCompareFunction<int64_t, int64_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    } else if (bType->isLongDecimal()) {
      return std::make_shared<
          DecimalCompareFunction<int64_t, int128_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    }
  }
  if (aType->isLongDecimal()) {
    if (bType->isShortDecimal()) {
      return std::make_shared<
          DecimalCompareFunction<int128_t, int64_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    } else if (bType->isLongDecimal()) {
      return std::make_shared<
          DecimalCompareFunction<int128_t, int128_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    }
  }
  VELOX_UNREACHABLE();
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalCompareSignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .returnType("boolean")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}
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

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_gt,
    decimalCompareSignature(),
    createDecimalCompareFunction<Gt>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_gte,
    decimalCompareSignature(),
    createDecimalCompareFunction<Gte>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_lt,
    decimalCompareSignature(),
    createDecimalCompareFunction<Lt>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_lte,
    decimalCompareSignature(),
    createDecimalCompareFunction<Lte>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_eq,
    decimalCompareSignature(),
    createDecimalCompareFunction<Eq>);
} // namespace facebook::velox::functions::sparksql
