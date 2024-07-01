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

#include "velox/functions/Udf.h"
#include "velox/type/Conversions.h"

namespace facebook::velox::functions::sparksql {
namespace detail {
static const StringView kNull = "NULL";
}

template <typename TExec>
struct ToPrettyStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  template <typename A>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/) {
    inputType_ = inputTypes[0];
  }

  template <typename TInput>
  void call(out_type<Varchar>& result, const TInput& input) {
    if constexpr (std::is_same_v<TInput, StringView>) {
      if (inputType_->isVarchar()) {
        result = input;
      } else if (inputType_->isVarbinary()) {
        result.reserve(1 + 3 * input.size());
        result.append("[");
        for (auto i = 0; i < input.size(); i++) {
          result.append(fmt::format("{:x}", input.data()[i]));
          if (i != input.size() - 1) {
            result.append(" ");
          }
        }
        result.append("]");
      }
      return;
    }
    if constexpr (std::is_same_v<TInput, int32_t>) {
      if (inputType_->isDate()) {
        auto output = DATE()->toString(input);
        result.append(output);
        return;
      }
    }
    const auto castResult =
        util::Converter<TypeKind::VARCHAR, void, util::SparkCastPolicy>::
            tryCast(input);
    if (castResult.hasError()) {
      result.setNoCopy(detail::kNull);
    } else {
      result.copy_from(castResult.value());
    }
  }

  template <typename TInput>
  void callNullable(out_type<Varchar>& result, const TInput* a) {
    if (a) {
      call(result, *a);
    } else {
      result.setNoCopy(detail::kNull);
    }
  }

 private:
  TypePtr inputType_;
  std::shared_ptr<SparkCastHooks> hooks_ = std::make_shared<SparkCastHooks>();
};

template <typename TExec>
struct ToPrettyStringTimeStampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      options_.timeZone = date::locate_zone(
          std::string_view((*timezone).data(), (*timezone).size()));
      timestampRowSize_ = getMaxStringLength(options_);
    }
  }

  void call(
      out_type<Varchar>& result,
      const Timestamp& input,
      const StringView& timezone) {
    if (!options_.timeZone) {
      options_.timeZone =
          date::locate_zone(std::string_view(timezone.data(), timezone.size()));
      timestampRowSize_ = getMaxStringLength(options_);
    }

    char out[timestampRowSize_];
    Timestamp inputValue(input);
    inputValue.toTimezone(*(options_.timeZone));
    const auto stringView =
        Timestamp::tsToStringView(inputValue, options_, out);
    result.append(stringView);
  }

  void callNullable(
      out_type<Varchar>& result,
      const arg_type<Timestamp>* timestamp,
      const arg_type<Varchar>* timezone) {
    if (timestamp) {
      VELOX_USER_CHECK_NOT_NULL(timezone);
      call(result, *timestamp, *timezone);
    } else {
      result.setNoCopy(detail::kNull);
    }
  }

 private:
  TimestampToStringOptions options_ = {
      .precision = TimestampToStringOptions::Precision::kMicroseconds,
      .leadingPositiveSign = true,
      .skipTrailingZeros = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };
  std::string::size_type timestampRowSize_;
};

// template <typename TExec>
// struct ToPrettyStringDecimalFunction {
//   VELOX_DEFINE_FUNCTION_TYPES(TExec);

//   template <typename A>
//   void initialize(
//       const std::vector<TypePtr>& inputTypes,
//       const core::QueryConfig& /*config*/,
//       A* /*a*/) {
//     const auto [precision, scale] = getDecimalPrecisionScale(*inputTypes[0]);
//     precision_ = precision;
//     scale_ = scale;
//   }

//   template <typename TInput>
//   void call(out_type<Varchar>& result, const TInput& input) {}

//   template <typename TInput>
//   void callNullable(out_type<Varchar>& result, const TInput* a) {
//     if (a) {
//       call(result, *a);
//     } else {
//       setNull(result);
//     }
//   }

//  private:
//   FOLLY_ALWAYS_INLINE void setNull(out_type<Varchar>& result) {
//     static const StringView kNull = "NULL";
//     result.setNoCopy(kNull);
//   }
//   uint8_t precision_ = -1;
//   uint8_t scale_ = -1;
// };
} // namespace facebook::velox::functions::sparksql
