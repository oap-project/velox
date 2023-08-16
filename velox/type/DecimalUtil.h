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

#pragma once

#include <string>
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Nulls.h"
#include "velox/type/Conversions.h"
#include "velox/type/Type.h"

namespace facebook::velox {

/// A static class that holds helper functions for DECIMAL type.
class DecimalUtil {
 public:
  static constexpr int128_t kPowersOfTen[LongDecimalType::kMaxPrecision + 1] = {
      1,
      10,
      100,
      1'000,
      10'000,
      100'000,
      1'000'000,
      10'000'000,
      100'000'000,
      1'000'000'000,
      10'000'000'000,
      100'000'000'000,
      1'000'000'000'000,
      10'000'000'000'000,
      100'000'000'000'000,
      1'000'000'000'000'000,
      10'000'000'000'000'000,
      100'000'000'000'000'000,
      1'000'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10,
      1'000'000'000'000'000'000 * (int128_t)100,
      1'000'000'000'000'000'000 * (int128_t)1'000,
      1'000'000'000'000'000'000 * (int128_t)10'000,
      1'000'000'000'000'000'000 * (int128_t)100'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000 *
          (int128_t)10,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000 *
          (int128_t)100};

  static constexpr int128_t kLongDecimalMin =
      -kPowersOfTen[LongDecimalType::kMaxPrecision] + 1;
  static constexpr int128_t kLongDecimalMax =
      kPowersOfTen[LongDecimalType::kMaxPrecision] - 1;
  static constexpr int128_t kShortDecimalMin =
      -kPowersOfTen[ShortDecimalType::kMaxPrecision] + 1;
  static constexpr int128_t kShortDecimalMax =
      kPowersOfTen[ShortDecimalType::kMaxPrecision] - 1;

  FOLLY_ALWAYS_INLINE static void valueInRange(int128_t value) {
    VELOX_CHECK(
        (value >= kLongDecimalMin && value <= kLongDecimalMax),
        "Value '{}' is not in the range of Decimal Type",
        value);
  }

  /// Helper function to convert a decimal value to string.
  static std::string toString(const int128_t value, const TypePtr& type);

  template <typename T>
  inline static void fillDecimals(
      T* decimals,
      const uint64_t* nullsPtr,
      const T* values,
      const int64_t* scales,
      int32_t numValues,
      int32_t targetScale) {
    for (int32_t i = 0; i < numValues; i++) {
      if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
        int32_t currentScale = scales[i];
        T value = values[i];
        if constexpr (std::is_same_v<T, std::int64_t>) { // Short Decimal
          if (targetScale > currentScale &&
              targetScale - currentScale <= ShortDecimalType::kMaxPrecision) {
            value *= static_cast<T>(kPowersOfTen[targetScale - currentScale]);
          } else if (
              targetScale < currentScale &&
              currentScale - targetScale <= ShortDecimalType::kMaxPrecision) {
            value /= static_cast<T>(kPowersOfTen[currentScale - targetScale]);
          } else if (targetScale != currentScale) {
            VELOX_FAIL("Decimal scale out of range");
          }
        } else { // Long Decimal
          if (targetScale > currentScale) {
            while (targetScale > currentScale) {
              int32_t scaleAdjust = std::min<int32_t>(
                  ShortDecimalType::kMaxPrecision, targetScale - currentScale);
              value *= kPowersOfTen[scaleAdjust];
              currentScale += scaleAdjust;
            }
          } else if (targetScale < currentScale) {
            while (currentScale > targetScale) {
              int32_t scaleAdjust = std::min<int32_t>(
                  ShortDecimalType::kMaxPrecision, currentScale - targetScale);
              value /= kPowersOfTen[scaleAdjust];
              currentScale -= scaleAdjust;
            }
          }
        }
        decimals[i] = value;
      }
    }
  }

  template <typename TInput, typename TOutput>
  inline static std::optional<TOutput> rescaleWithRoundUp(
      const TInput inputValue,
      const int fromPrecision,
      const int fromScale,
      const int toPrecision,
      const int toScale) {
    int128_t rescaledValue = inputValue;
    auto scaleDifference = toScale - fromScale;
    bool isOverflow = false;
    if (scaleDifference >= 0) {
      isOverflow = __builtin_mul_overflow(
          rescaledValue,
          DecimalUtil::kPowersOfTen[scaleDifference],
          &rescaledValue);
    } else {
      scaleDifference = -scaleDifference;
      const auto scalingFactor = DecimalUtil::kPowersOfTen[scaleDifference];
      rescaledValue /= scalingFactor;
      int128_t remainder = inputValue % scalingFactor;
      if (inputValue >= 0 && remainder >= scalingFactor / 2) {
        ++rescaledValue;
      } else if (remainder <= -scalingFactor / 2) {
        --rescaledValue;
      }
    }
    // Check overflow.
    if (rescaledValue < -DecimalUtil::kPowersOfTen[toPrecision] ||
        rescaledValue > DecimalUtil::kPowersOfTen[toPrecision] || isOverflow) {
      return std::nullopt;
    }
    return static_cast<TOutput>(rescaledValue);
  }

  template <typename TInput, typename TOutput>
  inline static std::optional<TOutput> rescaleInt(
      const TInput inputValue,
      const int toPrecision,
      const int toScale) {
    int128_t rescaledValue = static_cast<int128_t>(inputValue);
    bool isOverflow = __builtin_mul_overflow(
        rescaledValue, DecimalUtil::kPowersOfTen[toScale], &rescaledValue);
    // Check overflow.
    if (rescaledValue < -DecimalUtil::kPowersOfTen[toPrecision] ||
        rescaledValue > DecimalUtil::kPowersOfTen[toPrecision] || isOverflow) {
      VELOX_USER_FAIL(
          "Cannot cast {} '{}' to DECIMAL({},{})",
          SimpleTypeTrait<TInput>::name,
          inputValue,
          toPrecision,
          toScale);
    }
    return static_cast<TOutput>(rescaledValue);
  }

  // Derives from Arrow function DecimalFromString.
  template <typename TOutput>
  inline static std::optional<TOutput> rescaleVarchar(
      const StringView& s,
      const int toPrecision,
      const int toScale) {
    DecimalComponents dec;
    VELOX_USER_CHECK(
        parseDecimalComponents(s.data(), s.size(), &dec),
        "Value {} is not a number",
        s);

    // Count number of significant digits (without leading zeros).
    size_t firstNonZero = dec.wholeDigits.find_first_not_of('0');
    size_t significantDigits = dec.fractionalDigits.size();
    if (firstNonZero != std::string::npos) {
      significantDigits += dec.wholeDigits.size() - firstNonZero;
    }
    int32_t parsedPrecision = static_cast<int32_t>(significantDigits);

    int32_t parsedScale = 0;
    if (dec.hasExponent) {
      auto adjustedExponent = dec.exponent;
      parsedScale =
          -adjustedExponent + static_cast<int32_t>(dec.fractionalDigits.size());
    } else {
      parsedScale = static_cast<int32_t>(dec.fractionalDigits.size());
    }

    int128_t out = 0;
    shiftAndAdd(dec.wholeDigits, out);
    shiftAndAdd(dec.fractionalDigits, out);
    if (dec.sign == '-') {
      out = -out;
    }

    if (parsedScale < 0) {
      /// Force the scale to zero, to avoid negative scales (due to
      /// compatibility issues with external systems such as databases).
      VELOX_USER_CHECK_LE(
          -parsedScale,
          LongDecimalType::kMaxScale,
          "Value {} cannot be represented as DECIMAL({}, {})",
          s,
          toPrecision,
          toScale);
      out = checkedMultiply<int128_t>(
          out, DecimalUtil::kPowersOfTen[-parsedScale], "HugeInt");
      parsedPrecision -= parsedScale;
      parsedScale = 0;
    }

    VELOX_USER_CHECK_LE(parsedPrecision, LongDecimalType::kMaxPrecision);
    return DecimalUtil::rescaleWithRoundUp<int128_t, TOutput>(
        out, parsedPrecision, parsedScale, toPrecision, toScale);
  }

  /// Rescale a double value to decimal value.
  ///
  /// Use `folly::convertTo` to convert a double value to int128_t or int64_t,
  /// it will throw an exception like 'loss of precision' when the `toValue`
  /// overflows the limits of int128_t or int64_t. The exception will be caught
  /// here to judge whether there is a overflow.
  ///
  /// Normally, return the rescaled value. Otherwise, if the `toValue` overflows
  /// the TOutput's limits or the `toValue` exceeds the precision's limits, it
  /// will throw an exception.
  template <typename TOutput>
  inline static std::optional<TOutput> rescaleDouble(
      const double inputValue,
      const int toPrecision,
      const int toScale) {
    auto toValue =
        inputValue * static_cast<double>(DecimalUtil::kPowersOfTen[toScale]);

    bool isOverflow = !std::isfinite(toValue);
    TOutput rescaledValue;
    try {
      rescaledValue =
          util::Converter<CppToType<TOutput>::typeKind>::cast(toValue);
    } catch (const std::exception& e) {
      isOverflow = true;
    }

    if (rescaledValue < -DecimalUtil::kPowersOfTen[toPrecision] ||
        rescaledValue > DecimalUtil::kPowersOfTen[toPrecision] || isOverflow) {
      VELOX_USER_FAIL(
          "Cannot cast {} '{:f}' to DECIMAL({},{})",
          SimpleTypeTrait<double>::name,
          inputValue,
          toPrecision,
          toScale);
    }
    return static_cast<TOutput>(rescaledValue);
  }

  template <typename R, typename A, typename B>
  inline static R divideWithRoundUp(
      R& r,
      const A& a,
      const B& b,
      bool noRoundUp,
      uint8_t aRescale,
      uint8_t /*bRescale*/) {
    VELOX_CHECK_NE(b, 0, "Division by zero");
    int resultSign = 1;
    A unsignedDividendRescaled(a);
    if (a < 0) {
      resultSign = -1;
      unsignedDividendRescaled *= -1;
    }
    B unsignedDivisor(b);
    if (b < 0) {
      resultSign *= -1;
      unsignedDivisor *= -1;
    }
    unsignedDividendRescaled = checkedMultiply<R>(
        unsignedDividendRescaled,
        R(DecimalUtil::kPowersOfTen[aRescale]),
        "Decimal");
    R quotient = unsignedDividendRescaled / unsignedDivisor;
    R remainder = unsignedDividendRescaled % unsignedDivisor;
    if (!noRoundUp && static_cast<const B>(remainder) * 2 >= unsignedDivisor) {
      ++quotient;
    }
    r = quotient * resultSign;
    return remainder;
  }

  /*
   * sum up and return overflow/underflow.
   */
  inline static int64_t addUnsignedValues(
      int128_t& sum,
      const int128_t& lhs,
      const int128_t& rhs,
      bool isResultNegative) {
    __uint128_t unsignedSum = (__uint128_t)lhs + (__uint128_t)rhs;
    // Ignore overflow value.
    sum = (int128_t)unsignedSum & ~kOverflowMultiplier;
    sum = isResultNegative ? -sum : sum;
    return (unsignedSum >> 127);
  }

  inline static int64_t
  addWithOverflow(int128_t& result, const int128_t& lhs, const int128_t& rhs) {
    bool isLhsNegative = lhs < 0;
    bool isRhsNegative = rhs < 0;
    int64_t overflow = 0;
    if (isLhsNegative == isRhsNegative) {
      // Both inputs of same time.
      if (isLhsNegative) {
        // Both negative, ignore signs and add.
        overflow = addUnsignedValues(result, -lhs, -rhs, true);
        overflow = -overflow;
      } else {
        overflow = addUnsignedValues(result, lhs, rhs, false);
      }
    } else {
      // If one of them is negative, use addition.
      result = lhs + rhs;
    }
    return overflow;
  }

  /*
   * Computes average. If there is an overflow value uses the following
   * expression to compute the average.
   *                       ---                                         ---
   *                      |    overflow_multiplier          sum          |
   * average = overflow * |     -----------------  +  ---------------    |
   *                      |         count              count * overflow  |
   *                       ---                                         ---
   */
  inline static void computeAverage(
      int128_t& avg,
      const int128_t& sum,
      const int64_t count,
      const int64_t overflow) {
    if (overflow == 0) {
      divideWithRoundUp<int128_t, int128_t, int64_t>(
          avg, sum, count, false, 0, 0);
    } else {
      __uint128_t sumA{0};
      auto remainderA =
          DecimalUtil::divideWithRoundUp<__uint128_t, __uint128_t, int64_t>(
              sumA, kOverflowMultiplier, count, true, 0, 0);
      double totalRemainder = (double)remainderA / count;
      __uint128_t sumB{0};
      auto remainderB =
          DecimalUtil::divideWithRoundUp<__uint128_t, __int128_t, int64_t>(
              sumB, sum, count * overflow, true, 0, 0);
      totalRemainder += (double)remainderB / (count * overflow);
      DecimalUtil::addWithOverflow(avg, sumA, sumB);
      avg = avg * overflow + (int)(totalRemainder * overflow);
    }
  }

  inline static std::optional<int128_t>
  computeValidSum(int128_t sum, int64_t overflow) {
    // Value is valid if the conditions below are true.
    int128_t validSum = sum;
    if ((overflow == 1 && sum < 0) || (overflow == -1 && sum > 0)) {
      validSum = static_cast<int128_t>(
          DecimalUtil::kOverflowMultiplier * overflow + sum);
    } else {
      if (overflow != 0) {
        return std::nullopt;
      }
    }
    return validSum;
  }

  /// Origins from java side BigInteger#bitLength.
  ///
  /// Returns the number of bits in the minimal two's-complement
  /// representation of this BigInteger, <em>excluding</em> a sign bit.
  /// For positive BigIntegers, this is equivalent to the number of bits in
  /// the ordinary binary representation.  For zero this method returns
  /// {@code 0}.  (Computes {@code (ceil(log2(this < 0 ? -this : this+1)))}.)
  ///
  /// @return number of bits in the minimal two's-complement
  ///         representation of this BigInteger, <em>excluding</em> a sign bit.
  static int32_t getByteArrayLength(int128_t value);

  /// This method return the same result with the BigInterger#toByteArray()
  /// method in Java side.
  ///
  /// Returns a byte array containing the two's-complement representation of
  /// this BigInteger. The byte array will be in big-endian byte-order: the most
  /// significant byte is in the zeroth element. The array will contain the
  /// minimum number of bytes required to represent this BigInteger, including
  /// at least one sign bit, which is (ceil((this.bitLength() + 1)/8)).
  ///
  /// @return The length of out.
  static int32_t toByteArray(int128_t value, char* out);

  static constexpr __uint128_t kOverflowMultiplier = ((__uint128_t)1 << 127);

 private:
  struct DecimalComponents {
    std::string_view wholeDigits;
    std::string_view fractionalDigits;
    int32_t exponent = 0;
    char sign = 0;
    bool hasExponent = false;
  };

  inline static size_t parseDigitsRun(
      const char* s,
      size_t start,
      size_t size,
      std::string_view* out) {
    size_t pos;
    for (pos = start; pos < size; ++pos) {
      if (!(s[pos] >= '0' && s[pos] <= '9')) {
        break;
      }
    }
    *out = std::string_view(s + start, pos - start);
    return pos;
  }

  static bool
  parseDecimalComponents(const char* s, size_t size, DecimalComponents* out) {
    size_t pos = 0;

    if (size == 0) {
      return false;
    }
    // Sign of the number.
    if (s[pos] == '-' || s[pos] == '+') {
      out->sign = *(s + pos);
      ++pos;
    }
    // First run of digits.
    pos = parseDigitsRun(s, pos, size, &out->wholeDigits);
    if (pos == size) {
      return !out->wholeDigits.empty();
    }
    // Optional dot (if given in fractional form).
    if (s[pos] == '.') {
      // Second run of digits.
      ++pos;
      pos = parseDigitsRun(s, pos, size, &out->fractionalDigits);
    }
    if (out->wholeDigits.empty() && out->fractionalDigits.empty()) {
      // Need at least some digits (whole or fractional).
      return false;
    }
    if (pos == size) {
      return true;
    }
    // Optional exponent.
    if (s[pos] == 'e' || s[pos] == 'E') {
      ++pos;
      if (pos != size && s[pos] == '+') {
        ++pos;
      }
      out->hasExponent = true;
      folly::StringPiece p = {s + pos, size - pos};
      out->exponent =
          folly::to<int32_t>(folly::StringPiece(s + pos, size - pos));
      return true;
    }
    return pos == size;
  }

  /// Multiple out by the appropriate power of 10 necessary to add source parsed
  /// as int128_t and then adds the parsed value of source.
  inline static void shiftAndAdd(std::string_view input, int128_t& out) {
    auto length = input.size();
    if (length == 0) {
      return;
    }
    out = checkedMultiply<int128_t>(out, DecimalUtil::kPowersOfTen[length]);
    auto inputValue =
        folly::to<int128_t>(folly::StringPiece(input.data(), length));
    out = checkedPlus<int128_t>(inputValue, out);
  }
}; // DecimalUtil
} // namespace facebook::velox
