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
#include "velox/type/DecimalUtil.h"
#include "velox/type/DecimalUtilOp.h"

namespace facebook::velox::functions {
namespace {

template <
    typename R /* Result Type */,
    typename A /* Argument1 */,
    typename B /* Argument2 */,
    typename Operation /* Arithmetic operation */>
class DecimalBaseFunction : public exec::VectorFunction {
 public:
  DecimalBaseFunction(
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      const TypePtr& resultType)
      : aRescale_(aRescale),
        bRescale_(bRescale),
        aPrecision_(aPrecision),
        aScale_(aScale),
        bPrecision_(bPrecision),
        bScale_(bScale),
        rPrecision_(rPrecision),
        rScale_(rScale),
        resultType_(resultType) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType, // cannot used in spark
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto rawResults = prepareResults(rows, context, result);
    if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<SimpleVector<A>>()->valueAt(0);
      auto flatValues = args[1]->asUnchecked<FlatVector<B>>();
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            constant,
            rawValues[row],
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            &overflow);
        if (overflow) {
          result->setNull(row, true);
        }
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto flatValues = args[0]->asUnchecked<FlatVector<A>>();
      auto constant = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            rawValues[row],
            constant,
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            &overflow);
        if (overflow) {
          result->setNull(row, true);
        }
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      auto flatB = args[1]->asUnchecked<FlatVector<B>>();
      auto rawB = flatB->mutableRawValues();

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            rawA[row],
            rawB[row],
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            &overflow);
        if (overflow) {
          result->setNull(row, true);
        }
      });
    } else {
      // Fast path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto a = decodedArgs.at(0);
      auto b = decodedArgs.at(1);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            a->valueAt<A>(row),
            b->valueAt<B>(row),
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            &overflow);
        if (overflow) {
          result->setNull(row, true);
        }
      });
    }
  }

 private:
  R* prepareResults(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Here we can not use `resultType`, because this type derives from
    // substrait plan in spark spark arithmetic result type is left datatype,
    // but velox need new computed type
    context.ensureWritable(rows, resultType_, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  const uint8_t aRescale_;
  const uint8_t bRescale_;
  const uint8_t aPrecision_;
  const uint8_t aScale_;
  const uint8_t bPrecision_;
  const uint8_t bScale_;
  const uint8_t rPrecision_;
  const uint8_t rScale_;
  const TypePtr resultType_;
};

class Addition {
 public:
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      const A& a,
      const B& b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t /* aPrecision */,
      uint8_t /* aScale */,
      uint8_t /* bPrecision */,
      uint8_t /* bScale */,
      uint8_t /* rPrecision */,
      uint8_t /* rScale */,
      bool* overflow)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    int128_t aRescaled;
    int128_t bRescaled;
    if (__builtin_mul_overflow(
            a.unscaledValue(),
            DecimalUtil::kPowersOfTen[aRescale],
            &aRescaled) ||
        __builtin_mul_overflow(
            b.unscaledValue(),
            DecimalUtil::kPowersOfTen[bRescale],
            &bRescaled)) {
      VELOX_ARITHMETIC_ERROR(
          "Decimal overflow: {} + {}", a.unscaledValue(), b.unscaledValue());
    }
    auto res = R(aRescaled).plus(R(bRescaled), overflow);
    if (!*overflow) {
      r = res;
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    return {
        std::min(
            38,
            std::max(aPrecision - aScale, bPrecision - bScale) +
                std::max(aScale, bScale) + 1),
        std::max(aScale, bScale)};
  }

  inline static std::pair<uint8_t, uint8_t> adjustPrecisionScale(
      const uint8_t rPrecision,
      const uint8_t rScale) {
    if (rPrecision <= 38) {
      return {rPrecision, rScale};
    } else if (rScale < 0) {
      return {38, rScale};
    } else {
      int32_t minScale = std::min(static_cast<int32_t>(rScale), 6);
      int32_t delta = rPrecision - 38;
      return {38, std::max(rScale - delta, minScale)};
    }
  }
};

class Subtraction {
 public:
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      const A& a,
      const B& b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t /* aPrecision */,
      uint8_t /* aScale */,
      uint8_t /* bPrecision */,
      uint8_t /* bScale */,
      uint8_t /* rPrecision */,
      uint8_t /* rScale */,
      bool* overflow)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    int128_t aRescaled;
    int128_t bRescaled;
    if (__builtin_mul_overflow(
            a.unscaledValue(),
            DecimalUtil::kPowersOfTen[aRescale],
            &aRescaled) ||
        __builtin_mul_overflow(
            b.unscaledValue(),
            DecimalUtil::kPowersOfTen[bRescale],
            &bRescaled)) {
      *overflow = true;
      return;
    }
    auto res = R(aRescaled).minus(R(bRescaled), overflow);
    if (!*overflow) {
      r = res;
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    return Addition::computeResultPrecisionScale(
        aPrecision, aScale, bPrecision, bScale);
  }
};

class Multiply {
 public:
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      const A& a,
      const B& b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool* overflow) {
    // derive from Arrow
    if (rPrecision < 38) {
      auto res = checkedMultiply<R>(
          R(a).multiply(R(b), overflow),
          R(DecimalUtil::kPowersOfTen[aRescale + bRescale]));
      if (!*overflow) {
        r = res;
      }
    } else if (a.unscaledValue() == 0 && b.unscaledValue() == 0) {
      // Handle this separately to avoid divide-by-zero errors.
      r = R(0);
    } else {
      auto deltaScale = aScale + bScale - rScale;
      if (deltaScale == 0) {
        // No scale down
        auto res = R(a).multiply(R(b), overflow);
        if (!*overflow) {
          r = res;
        }
      } else {
        // scale down
        // It's possible that the intermediate value does not fit in 128-bits,
        // but the final value will (after scaling down).
        int32_t total_leading_zeros =
            a.countLeadingZeros() + b.countLeadingZeros();
        // This check is quick, but conservative. In some cases it will
        // indicate that converting to 256 bits is necessary, when it's not
        // actually the case.
        if (UNLIKELY(total_leading_zeros <= 128)) {
          // needs_int256
          int256_t aLarge = a.unscaledValue();
          int256_t blarge = b.unscaledValue();
          int256_t reslarge = aLarge * blarge;
          reslarge = ReduceScaleBy(reslarge, deltaScale);
          auto res = R::convert(reslarge, overflow);
          if (!*overflow) {
            r = res;
          }
        } else {
          if (LIKELY(deltaScale <= 38)) {
            // The largest value that result can have here is (2^64 - 1) * (2^63
            // - 1), which is greater than BasicDecimal128::kMaxValue.
            auto res = R(a).multiply(R(b), overflow);
            VELOX_DCHECK(!*overflow);
            // Since deltaScale is greater than zero, result can now be at most
            // ((2^64 - 1) * (2^63 - 1)) / 10, which is less than
            // BasicDecimal128::kMaxValue, so there cannot be any overflow.
            r = res / R(DecimalUtil::kPowersOfTen[deltaScale]);
          } else {
            // We are multiplying decimal(38, 38) by decimal(38, 38). The result
            // should be a
            // decimal(38, 37), so delta scale = 38 + 38 - 37 = 39. Since we are
            // not in the 256 bit intermediate value case and we are scaling
            // down by 39, then we are guaranteed that the result is 0 (even if
            // we try to round). The largest possible intermediate result is 38
            // "9"s. If we scale down by 39, the leftmost 9 is now two digits to
            // the right of the rightmost "visible" one. The reason why we have
            // to handle this case separately is because a scale multiplier with
            // a deltaScale 39 does not fit into 128 bit.
            r = R(0);
          }
        }
      }
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return 0;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    return Addition::adjustPrecisionScale(
        aPrecision + bPrecision + 1, aScale + bScale);
  }

 private:
  // derive from Arrow
  inline static int256_t ReduceScaleBy(int256_t in, int32_t reduceBy) {
    if (reduceBy == 0) {
      // nothing to do.
      return in;
    }

    int256_t divisor = DecimalUtil::kPowersOfTen[reduceBy];
    DCHECK_GT(divisor, 0);
    DCHECK_EQ(divisor % 2, 0); // multiple of 10.
    auto result = in / divisor;
    auto remainder = in % divisor;
    // round up (same as BasicDecimal128::ReduceScaleBy)
    if (abs(remainder) >= (divisor >> 1)) {
      result += (in > 0 ? 1 : -1);
    }
    return result;
  }
};

class Divide {
 public:
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      const A& a,
      const B& b,
      uint8_t aRescale,
      uint8_t /*bRescale*/,
      uint8_t /* aPrecision */,
      uint8_t /* aScale */,
      uint8_t /* bPrecision */,
      uint8_t /* bScale */,
      uint8_t /* rPrecision */,
      uint8_t /* rScale */,
      bool* overflow) {
    DecimalUtilOp::divideWithRoundUp<R, A, B>(
        r, a, b, false, aRescale, 0, overflow);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale) {
    return rScale - fromScale + toScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    auto scale = std::max(6, aScale + bPrecision + 1);
    auto precision = aPrecision - aScale + bScale + scale;
    return Addition::adjustPrecisionScale(precision, scale);
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalMultiplySignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .integerVariable(
                  "r_precision", "min(38, a_precision + b_precision + 1)")
              .integerVariable(
                  "r_scale", "a_scale") // not same with the result type
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalAddSubtractSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .integerVariable(
              "r_precision",
              "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
          .integerVariable("r_scale", "max(a_scale, b_scale)")
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalDivideSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .integerVariable(
              "r_precision",
              "min(38, a_precision - a_scale + b_scale + max(6, a_scale + b_precision + 1))")
          .integerVariable(
              "r_scale",
              "min(37, max(6, a_scale + b_precision + 1))") // if precision is
                                                            // more than 38,
                                                            // scale has new
                                                            // value, this
                                                            // check constrait
                                                            // is not same
                                                            // with result
                                                            // type
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  auto [rPrecision, rScale] = Operation::computeResultPrecisionScale(
      aPrecision, aScale, bPrecision, bScale);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale, bScale, rScale);
  uint8_t bRescale = Operation::computeRescaleFactor(bScale, aScale, rScale);
  if (aType->kind() == TypeKind::SHORT_DECIMAL) {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      if (rPrecision > DecimalType<TypeKind::SHORT_DECIMAL>::kMaxPrecision) {
        // Arguments are short decimals and result is a long decimal.
        return std::make_shared<DecimalBaseFunction<
            UnscaledLongDecimal /*result*/,
            UnscaledShortDecimal,
            UnscaledShortDecimal,
            Operation>>(
            aRescale,
            bRescale,
            aPrecision,
            aScale,
            bPrecision,
            bScale,
            rPrecision,
            rScale,
            LONG_DECIMAL(rPrecision, rScale));
      } else {
        // Arguments are short decimals and result is a short decimal.
        return std::make_shared<DecimalBaseFunction<
            UnscaledShortDecimal /*result*/,
            UnscaledShortDecimal,
            UnscaledShortDecimal,
            Operation>>(
            aRescale,
            bRescale,
            aPrecision,
            aScale,
            bPrecision,
            bScale,
            rPrecision,
            rScale,
            SHORT_DECIMAL(rPrecision, rScale));
      }
    } else {
      // LHS is short decimal and rhs is a long decimal, result is long
      // decimal.
      return std::make_shared<DecimalBaseFunction<
          UnscaledLongDecimal /*result*/,
          UnscaledShortDecimal,
          UnscaledLongDecimal,
          Operation>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale,
          LONG_DECIMAL(rPrecision, rScale));
    }
  } else {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      // LHS is long decimal and rhs is short decimal, result is a long
      // decimal.
      return std::make_shared<DecimalBaseFunction<
          UnscaledLongDecimal /*result*/,
          UnscaledLongDecimal,
          UnscaledShortDecimal,
          Operation>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale,
          LONG_DECIMAL(rPrecision, rScale));
    } else {
      // Arguments and result are all long decimals.
      return std::make_shared<DecimalBaseFunction<
          UnscaledLongDecimal /*result*/,
          UnscaledLongDecimal,
          UnscaledLongDecimal,
          Operation>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale,
          LONG_DECIMAL(rPrecision, rScale));
    }
  }
  VELOX_UNSUPPORTED();
}
}; // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_add,
    decimalAddSubtractSignature(),
    createDecimalFunction<Addition>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_sub,
    decimalAddSubtractSignature(),
    createDecimalFunction<Subtraction>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_mul,
    decimalMultiplySignature(),
    createDecimalFunction<Multiply>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_div,
    decimalDivideSignature(),
    createDecimalFunction<Divide>);
}; // namespace facebook::velox::functions
