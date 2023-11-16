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
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Type.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

class JsonTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> getJsonObject(
      std::optional<std::string> json,
      std::optional<std::string> jsonPath) {
    return evaluateOnce<std::string>("get_json_object(c0, c1)", json, jsonPath);
  }
};

TEST_F(JsonTest, getJsonObject) {
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$.hello"), "3.5");
  EXPECT_EQ(getJsonObject(R"({"hello": 3.5})", "$.hello"), "3.5");
  EXPECT_EQ(getJsonObject(R"({"hello": 292222730})", "$.hello"), "292222730");
  EXPECT_EQ(getJsonObject(R"({"hello": -292222730})", "$.hello"), "-292222730");
  EXPECT_EQ(getJsonObject(R"({"my": {"hello": 3.5}})", "$.my.hello"), "3.5");
  EXPECT_EQ(getJsonObject(R"({"my": {"hello": true}})", "$.my.hello"), "true");
  EXPECT_EQ(getJsonObject(R"({"hello": ""})", "$.hello"), "");
  // Field not found.
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$.hi"), std::nullopt);
  // Illegal json.
  EXPECT_EQ(getJsonObject(R"({"hello"-3.5})", "$.hello"), std::nullopt);
  // Illegal json path.
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$hello"), std::nullopt);
  EXPECT_EQ(getJsonObject(R"({"hello": "3.5"})", "$."), std::nullopt);

  EXPECT_EQ(
      getJsonObject(R"({"name": "Alice", "age": 5, "id": "001"})", "$.age"),
      "5");
  EXPECT_EQ(
      getJsonObject(R"({"name": "Alice", "age": 5, "id": "001"})", "$.id"),
      "001");
  // Object result.
  EXPECT_EQ(
      getJsonObject(
          R"({"my": {"param": {"name": "Alice", "age": "5", "id": "001"}}})",
          "$.my.param"),
      R"({"name": "Alice", "age": "5", "id": "001"})");
  EXPECT_EQ(
      getJsonObject(
          R"({"my": {"param": {"name": "Alice", "age": "5", "id": "001"}}})",
          "$['my']['param']"),
      R"({"name": "Alice", "age": "5", "id": "001"})");

  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"param": {"name": "Alice", "age": "5", "id": "001"}}}, {"other": "placehoder"}])",
          "$[0]['my']['param']['age']"),
      "5");
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"param": {"name": "Alice", "age": "5", "id": "001"}}}, {"other": "placehoder"}])",
          "$[0].my.param.age"),
      "5");

  // Gets array result.
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"param": {"name": "Alice"}}}, {"other": ["placeholder1", "placeholder2"]}])",
          "$[1].other"),
      R"(["placeholder1", "placeholder2"])");
  // Gets array element.
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"param": {"name": "Alice"}}}, {"other": ["placeholder1", "placeholder2"]}])",
          "$[1].other[0]"),
      "placeholder1");

  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"param": {"name": "Alice"}}}, {"other": ["placeholder1", "placeholder2"]}])",
          "$[1].other[1]"),
      "placeholder2");
  EXPECT_EQ(
      getJsonObject(
          R"([{"my": {"param": {"name": "Alice"quoted""}}}, {"other": ["placeholder1", "placeholder2"]}])",
          "$[0].my.param.name"),
      std::nullopt);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
