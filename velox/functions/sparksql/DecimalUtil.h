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

#include <boost/multiprecision/cpp_int.hpp>

#include "velox/type/DecimalUtil.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::sparksql {
using uint128_t = __uint128_t;
using int256_t = boost::multiprecision::int256_t;

class DecimalUtil {
 public:
  template <
      class T,
      typename = std::enable_if_t<
          std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>>>
  inline static T convert(int256_t in, bool& overflow) {
    typedef typename std::
        conditional<std::is_same_v<T, int64_t>, uint64_t, __uint128_t>::type UT;
    T result = 0;
    constexpr int256_t uintMask =
        static_cast<int256_t>(std::numeric_limits<UT>::max());

    int256_t inAbs = abs(in);
    bool isNegative = in < 0;

    UT unsignResult = (inAbs & uintMask).convert_to<UT>();
    inAbs >>= sizeof(T) * 8;

    if (inAbs > 0) {
      // We've shifted in by bit of T, so nothing should be left.
      overflow = true;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      if (unsignResult > velox::DecimalUtil::kShortDecimalMax) {
        overflow = true;
      } else {
        result = static_cast<T>(unsignResult);
      }
    } else {
      // int128_t
      if (unsignResult > velox::DecimalUtil::kLongDecimalMax) {
        overflow = true;
      } else {
        result = static_cast<T>(unsignResult);
      }
    }
    return isNegative ? -result : result;
  }

  template <class T, typename = std::enable_if_t<std::is_same_v<T, int64_t>>>
  FOLLY_ALWAYS_INLINE static uint64_t absValue(int64_t a) {
    return a < 0 ? static_cast<uint64_t>(-a) : static_cast<uint64_t>(a);
  }

  template <class T, typename = std::enable_if_t<std::is_same_v<T, int128_t>>>
  FOLLY_ALWAYS_INLINE static uint128_t absValue(int128_t a) {
    return a < 0 ? static_cast<uint128_t>(-a) : static_cast<uint128_t>(a);
  }

  template <class T, typename = std::enable_if_t<std::is_same_v<T, int64_t>>>
  FOLLY_ALWAYS_INLINE static int64_t
  multiply(int64_t a, int64_t b, bool& overflow) {
    int64_t value;
    overflow = __builtin_mul_overflow(a, b, &value);
    if (!overflow && value >= velox::DecimalUtil::kShortDecimalMin &&
        value <= velox::DecimalUtil::kShortDecimalMax) {
      return value;
    }
    overflow = true;
    return -1;
  }

  template <class T, typename = std::enable_if_t<std::is_same_v<T, int128_t>>>
  FOLLY_ALWAYS_INLINE static int128_t
  multiply(int128_t a, int128_t b, bool& overflow) {
    int128_t value;
    overflow = __builtin_mul_overflow(a, b, &value);
    if (!overflow && value >= velox::DecimalUtil::kLongDecimalMin &&
        value <= velox::DecimalUtil::kLongDecimalMax) {
      return value;
    }
    overflow = true;
    return -1;
  }

  template <typename A, typename B>
  inline static int32_t
  minLeadingZeros(const A& a, const B& b, uint8_t aScale, uint8_t bScale) {
    int32_t aLeadingZeros = bits::countLeadingZeros(absValue<A>(a));
    int32_t bLeadingZeros = bits::countLeadingZeros(absValue<B>(b));
    if (aScale < bScale) {
      aLeadingZeros =
          minLeadingZerosAfterScaling(aLeadingZeros, bScale - aScale);
    } else if (aScale > bScale) {
      bLeadingZeros =
          minLeadingZerosAfterScaling(bLeadingZeros, aScale - bScale);
    }
    return std::min(aLeadingZeros, bLeadingZeros);
  }

  // Derives from Arrow BasicDecimal128 Divide
  template <typename R, typename A, typename B>
  inline static R divideWithRoundUp(
      R& r,
      const A& a,
      const B& b,
      uint8_t aRescale,
      uint8_t /*bRescale*/,
      bool& overflow) {
    if (b == 0) {
      overflow = true;
      return R(-1);
    }
    int resultSign = 1;
    R unsignedDividendRescaled(a);
    int aSign = 1;
    int bSign = 1;
    if (a < 0) {
      resultSign = -1;
      unsignedDividendRescaled *= -1;
      aSign = -1;
    }
    R unsignedDivisor(b);
    if (b < 0) {
      resultSign *= -1;
      unsignedDivisor *= -1;
      bSign = -1;
    }
    auto bitsRequiredAfterScaling = maxBitsRequiredAfterScaling<A>(a, aRescale);
    if (bitsRequiredAfterScaling <= 127) {
      overflow = __builtin_mul_overflow(
          unsignedDividendRescaled,
          R(velox::DecimalUtil::kPowersOfTen[aRescale]),
          &unsignedDividendRescaled);
      if (overflow) {
        return R(-1);
      }
      R quotient = unsignedDividendRescaled / unsignedDivisor;
      R remainder = unsignedDividendRescaled % unsignedDivisor;
      if (remainder * 2 >= unsignedDivisor) {
        ++quotient;
      }
      r = quotient * resultSign;
      return remainder;
    } else {
      if (aRescale > 38 && bitsRequiredAfterScaling > 255) {
        overflow = true;
        return R(-1);
      }
      int256_t aLarge = a;
      int256_t aLargeScaledUp =
          aLarge * velox::DecimalUtil::kPowersOfTen[aRescale];
      int256_t bLarge = b;
      int256_t resultLarge = aLargeScaledUp / bLarge;
      int256_t remainderLarge = aLargeScaledUp % bLarge;
      // Since we are scaling up and then, scaling down, round-up the result
      // (+1 for +ve, -1 for -ve), if the remainder is >= 2 * divisor.
      if (abs(2 * remainderLarge) >= abs(bLarge)) {
        // x +ve and y +ve, result is +ve =>   (1 ^ 1)  + 1 =  0 + 1 = +1
        // x +ve and y -ve, result is -ve =>  (-1 ^ 1)  + 1 = -2 + 1 = -1
        // x +ve and y -ve, result is -ve =>   (1 ^ -1) + 1 = -2 + 1 = -1
        // x -ve and y -ve, result is +ve =>  (-1 ^ -1) + 1 =  0 + 1 = +1
        resultLarge += (aSign ^ bSign) + 1;
      }

      auto result = convert<R>(resultLarge, overflow);
      if (overflow) {
        return R(-1);
      }
      r = result;
      auto remainder = convert<R>(remainderLarge, overflow);
      return remainder;
    }
  }

 private:
  inline static int32_t maxBitsRequiredIncreaseAfterScaling(int32_t scaleBy) {
    // We rely on the following formula:
    // bits_required(x * 10^y) <= bits_required(x) + floor(log2(10^y)) + 1
    // We precompute floor(log2(10^x)) + 1 for x = 0, 1, 2...75, 76

    static const int32_t floorLog2PlusOne[] = {
        0,   4,   7,   10,  14,  17,  20,  24,  27,  30,  34,  37,  40,
        44,  47,  50,  54,  57,  60,  64,  67,  70,  74,  77,  80,  84,
        87,  90,  94,  97,  100, 103, 107, 110, 113, 117, 120, 123, 127,
        130, 133, 137, 140, 143, 147, 150, 153, 157, 160, 163, 167, 170,
        173, 177, 180, 183, 187, 190, 193, 196, 200, 203, 206, 210, 213,
        216, 220, 223, 226, 230, 233, 236, 240, 243, 246, 250, 253};
    return floorLog2PlusOne[scaleBy];
  }

  template <typename A>
  inline static int32_t maxBitsRequiredAfterScaling(
      const A& num,
      uint8_t aRescale) {
    auto valueAbs = absValue<A>(num);
    int32_t numOccupied = sizeof(A) * 8 - bits::countLeadingZeros(valueAbs);
    return numOccupied + maxBitsRequiredIncreaseAfterScaling(aRescale);
  }

  /// If we have a number with 'numLeadingZeros' leading zeros, and we scale it
  /// up by 10^scale_by, this function returns the minimum number of leading
  /// zeros the result can have.
  inline static int32_t minLeadingZerosAfterScaling(
      int32_t numLeadingZeros,
      int32_t scaleBy) {
    int32_t result =
        numLeadingZeros - maxBitsRequiredIncreaseAfterScaling(scaleBy);
    return result;
  }
};
} // namespace facebook::velox::functions::sparksql
