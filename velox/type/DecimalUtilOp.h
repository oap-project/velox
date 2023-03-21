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
#include "velox/type/DecimalUtil.h"
#include "velox/type/Type.h"
#include "velox/type/UnscaledLongDecimal.h"
#include "velox/type/UnscaledShortDecimal.h"

#include <boost/multiprecision/cpp_int.hpp>

namespace facebook::velox {
using boost::multiprecision::int256_t;
using uint128_t = __uint128_t;

class DecimalUtilOp {
 public:
  inline static int32_t maxBitsRequiredIncreaseAfterScaling(int32_t scale_by) {
    // We rely on the following formula:
    // bits_required(x * 10^y) <= bits_required(x) + floor(log2(10^y)) + 1
    // We precompute floor(log2(10^x)) + 1 for x = 0, 1, 2...75, 76

    static const int32_t floor_log2_plus_one[] = {
        0,   4,   7,   10,  14,  17,  20,  24,  27,  30,  34,  37,  40,
        44,  47,  50,  54,  57,  60,  64,  67,  70,  74,  77,  80,  84,
        87,  90,  94,  97,  100, 103, 107, 110, 113, 117, 120, 123, 127,
        130, 133, 137, 140, 143, 147, 150, 153, 157, 160, 163, 167, 170,
        173, 177, 180, 183, 187, 190, 193, 196, 200, 203, 206, 210, 213,
        216, 220, 223, 226, 230, 233, 236, 240, 243, 246, 250, 253};
    return floor_log2_plus_one[scale_by];
  }

  template <typename A>
  inline static int32_t maxBitsRequiredAfterScaling(
      const A& num,
      uint8_t aRescale) {
    auto value = num.unscaledValue();
    auto valueAbs = std::abs(value);
    int32_t num_occupied = 0;
    if constexpr (std::is_same_v<A, UnscaledShortDecimal>) {
      num_occupied = 64 - bits::countLeadingZeros(valueAbs);
    } else {
      uint64_t hi = valueAbs >> 64;
      uint64_t lo = static_cast<uint64_t>(valueAbs);
      num_occupied = (hi == 0) ? 64 - bits::countLeadingZeros(lo)
                               : 64 - bits::countLeadingZeros(hi);
    }

    return num_occupied + maxBitsRequiredIncreaseAfterScaling(aRescale);
  }

  inline static int128_t ConvertToInt128(int256_t in) {
    int128_t result;
    int128_t INT128_MAX = int128_t(int128_t(-1L)) >> 1;
    constexpr int256_t UINT128_MASK = std::numeric_limits<uint128_t>::max();

    int256_t in_abs = abs(in);
    bool is_negative = in < 0;

    uint128_t unsignResult = (in_abs & UINT128_MASK).convert_to<uint128_t>();
    in_abs >>= 128;

    if (in_abs > 0) {
      // we've shifted in by 128-bit, so nothing should be left.
      VELOX_FAIL("in_abs overflow");
    } else if (unsignResult > INT128_MAX) {
      // the high-bit must not be set (signed 128-bit).
      VELOX_FAIL("in_abs > int128 max");
    } else {
      result = static_cast<int128_t>(unsignResult);
    }
    return is_negative ? -result : result;
  }

  inline static int64_t ConvertToInt64(int256_t in) {
    int64_t result;
    constexpr int256_t UINT64_MASK = std::numeric_limits<uint64_t>::max();

    int256_t in_abs = abs(in);
    bool is_negative = in < 0;

    uint128_t unsignResult = (in_abs & UINT64_MASK).convert_to<uint64_t>();
    in_abs >>= 64;

    if (in_abs > 0) {
      // we've shifted in by 128-bit, so nothing should be left.
      VELOX_FAIL("in_abs overflow");
    } else if (unsignResult > INT64_MAX) {
      // the high-bit must not be set (signed 128-bit).
      VELOX_FAIL("in_abs > int64 max");
    } else {
      result = static_cast<int64_t>(unsignResult);
    }
    return is_negative ? -result : result;
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
      unsignedDividendRescaled = checkedMultiply<R>(
          unsignedDividendRescaled, R(DecimalUtil::kPowersOfTen[aRescale]));
      R quotient = unsignedDividendRescaled / unsignedDivisor;
      R remainder = unsignedDividendRescaled % unsignedDivisor;
      if (!noRoundUp && remainder * 2 >= unsignedDivisor) {
        ++quotient;
      }
      r = quotient * resultSign;
      return remainder;
    } else if constexpr (
        std::is_same_v<R, UnscaledShortDecimal> ||
        std::is_same_v<R, UnscaledLongDecimal>) {
      // Derives from Arrow BasicDecimal128 Divide
      if (aRescale > 38 && bitsRequiredAfterScaling > 255) {
        VELOX_FAIL(
            "Decimal overflow because rescale {} > 38 and bitsRequiredAfterScaling {} > 255",
            aRescale,
            bitsRequiredAfterScaling);
      }
      int256_t aLarge = a.unscaledValue();
      int256_t x_large_scaled_up = aLarge * DecimalUtil::kPowersOfTen[aRescale];
      int256_t y_large = b.unscaledValue();
      int256_t result_large = x_large_scaled_up / y_large;
      int256_t remainder_large = x_large_scaled_up % y_large;
      // Since we are scaling up and then, scaling down, round-up the result (+1
      // for +ve, -1 for -ve), if the remainder is >= 2 * divisor.
      if (abs(2 * remainder_large) >= abs(y_large)) {
        // x +ve and y +ve, result is +ve =>   (1 ^ 1)  + 1 =  0 + 1 = +1
        // x +ve and y -ve, result is -ve =>  (-1 ^ 1)  + 1 = -2 + 1 = -1
        // x +ve and y -ve, result is -ve =>   (1 ^ -1) + 1 = -2 + 1 = -1
        // x -ve and y -ve, result is +ve =>  (-1 ^ -1) + 1 =  0 + 1 = +1
        result_large += (aSign ^ bSign) + 1;
      }
      if constexpr (std::is_same_v<R, UnscaledShortDecimal>) {
        int64_t result = ConvertToInt128(result_large);
        if (!R::valueInRange(result)) {
          VELOX_FAIL("overflow long decimal");
        }
        r = UnscaledShortDecimal(result);
        return UnscaledShortDecimal(ConvertToInt64(remainder_large));
      } else {
        int128_t result = ConvertToInt128(result_large);
        if (!R::valueInRange(result)) {
          VELOX_FAIL("overflow long decimal");
        }
        r = UnscaledLongDecimal(result);
        return UnscaledLongDecimal(ConvertToInt128(remainder_large));
      }
    } else {
      VELOX_FAIL("Should not reach here in DecimalUtilOp.h");
    }
  }
};
} // namespace facebook::velox
