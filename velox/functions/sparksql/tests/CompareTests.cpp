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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class CompareTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<bool> equaltonullsafe(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("equalnullsafe(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> equalto(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("equalto(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> lessthan(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("lessthan(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> lessthanorequal(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("lessthanorequal(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> greaterthan(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("greaterthan(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<bool> greaterthanorequal(
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<bool>("greaterthanorequal(c0, c1)", a, b);
  }
};

static constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();

TEST_F(CompareTest, equaltonullsafe) {
  EXPECT_EQ(equaltonullsafe<int64_t>(1, 1), true);
  EXPECT_EQ(equaltonullsafe<int32_t>(1, 2), false);
  EXPECT_EQ(equaltonullsafe<float>(std::nullopt, std::nullopt), true);
  EXPECT_EQ(equaltonullsafe<std::string>(std::nullopt, "abcs"), false);
  EXPECT_EQ(equaltonullsafe<std::string>(std::nullopt, std::nullopt), true);
  EXPECT_EQ(equaltonullsafe<double>(1, std::nullopt), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, std::nullopt), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, 1), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, kNaN), true);
}

TEST_F(CompareTest, equalto) {
  EXPECT_EQ(equalto<int64_t>(1, 1), true);
  EXPECT_EQ(equalto<int32_t>(1, 2), false);
  EXPECT_EQ(equalto<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(equalto<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(equalto<double>(kNaN, 1), false);
  EXPECT_EQ(lessthan<double>(0, kNaN), true);
  EXPECT_EQ(equalto<double>(kNaN, kNaN), true);
}

TEST_F(CompareTest, lessthan) {
  EXPECT_EQ(lessthan<int64_t>(1, 1), false);
  EXPECT_EQ(lessthan<int32_t>(1, 2), true);
  EXPECT_EQ(lessthan<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(lessthan<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthan<double>(kNaN, 1), false);
  EXPECT_EQ(lessthan<double>(0, kNaN), true);
  EXPECT_EQ(lessthan<double>(kNaN, kNaN), false);
}

TEST_F(CompareTest, lessthanorequal) {
  EXPECT_EQ(lessthanorequal<int64_t>(1, 1), true);
  EXPECT_EQ(lessthanorequal<int32_t>(1, 2), true);
  EXPECT_EQ(lessthanorequal<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(lessthanorequal<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(lessthanorequal<double>(kNaN, 1), false);
  EXPECT_EQ(lessthanorequal<double>(0, kNaN), true);
  EXPECT_EQ(lessthanorequal<double>(kNaN, kNaN), true);
}

TEST_F(CompareTest, greaterthan) {
  EXPECT_EQ(greaterthan<int64_t>(1, 1), false);
  EXPECT_EQ(greaterthan<int32_t>(1, 2), false);
  EXPECT_EQ(greaterthan<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(greaterthan<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthan<double>(kNaN, 1), true);
  EXPECT_EQ(greaterthan<double>(0, kNaN), false);
  EXPECT_EQ(greaterthan<double>(kNaN, kNaN), false);
}

TEST_F(CompareTest, greaterthanorequal) {
  EXPECT_EQ(greaterthanorequal<int64_t>(1, 1), true);
  EXPECT_EQ(greaterthanorequal<int32_t>(1, 2), false);
  EXPECT_EQ(greaterthanorequal<float>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthanorequal<std::string>(std::nullopt, "abcs"), std::nullopt);
  EXPECT_EQ(greaterthanorequal<std::string>(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthanorequal<double>(1, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthanorequal<double>(kNaN, std::nullopt), std::nullopt);
  EXPECT_EQ(greaterthanorequal<double>(kNaN, 1), true);
  EXPECT_EQ(greaterthanorequal<double>(0, kNaN), false);
  EXPECT_EQ(greaterthanorequal<double>(kNaN, kNaN), true);
}
} // namespace
}; // namespace facebook::velox::functions::sparksql::test
