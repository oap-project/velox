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

#include <stdint.h>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class DateTimeFunctionsTest : public SparkFunctionBaseTest {
 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  Date parseDate(const std::string& dateStr) {
    Date returnDate;
    parseTo(dateStr, returnDate);
    return returnDate;
  }
};

TEST_F(DateTimeFunctionsTest, year) {
  const auto year = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 9000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1969, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 12300000000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<Date> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Date(0)));
  EXPECT_EQ(1969, year(Date(-1)));
  EXPECT_EQ(2020, year(Date(18262)));
  EXPECT_EQ(1920, year(Date(-18262)));
}

TEST_F(DateTimeFunctionsTest, dateAdd) {
  const auto dateAddInt32 = [&](std::optional<Date> date,
                           std::optional<int32_t> value) {
    return evaluateOnce<Date>(
        "date_add(c0, c1)", date, value);
  };
  const auto dateAddInt16 = [&](std::optional<Date> date,
                           std::optional<int16_t> value) {
    return evaluateOnce<Date>(
        "date_add(c0, c1)", date, value);
  };
  const auto dateAddInt8 = [&](std::optional<Date> date,
                           std::optional<int8_t> value) {
    return evaluateOnce<Date>(
        "date_add(c0, c1)", date, value);
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateAddInt32(std::nullopt, 1));
  EXPECT_EQ(std::nullopt, dateAddInt16(std::nullopt, 1));
  EXPECT_EQ(std::nullopt, dateAddInt8(std::nullopt, 1));

  // Simple tests
  EXPECT_EQ(
      parseDate("2019-03-01"), dateAddInt32(parseDate("2019-02-28"), 1));
  EXPECT_EQ(
      parseDate("2019-03-01"), dateAddInt16(parseDate("2019-02-28"), 1));
  EXPECT_EQ(
      parseDate("2019-03-01"), dateAddInt8(parseDate("2019-02-28"), 1));

  // Account for the last day of a year-month
  EXPECT_EQ(
      parseDate("2020-02-29"), dateAddInt32(parseDate("2019-01-30"), 395));
  EXPECT_EQ(
      parseDate("2020-02-29"), dateAddInt16(parseDate("2019-01-30"), 395));

  // Check for negative intervals
  EXPECT_EQ(
      parseDate("2019-02-28"), dateAddInt32(parseDate("2020-02-29"), -366));
  EXPECT_EQ(
      parseDate("2019-02-28"), dateAddInt16(parseDate("2020-02-29"), -366));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
