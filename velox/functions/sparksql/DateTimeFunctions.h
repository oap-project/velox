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

#include "velox/functions/lib/TimeUtils.h"
#include "velox/functions/prestosql/DateTimeImpl.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct YearFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int32_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }
};

template <typename T>
struct DateAddFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const int32_t value) {
    result = addToDate(date, DateTimeUnit::kDay, value);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const int16_t value) {
    result = addToDate(date, DateTimeUnit::kDay, (int32_t)value);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const int8_t value) {
    result = addToDate(date, DateTimeUnit::kDay, (int32_t)value);
    return true;
  }
};
} // namespace facebook::velox::functions::sparksql
