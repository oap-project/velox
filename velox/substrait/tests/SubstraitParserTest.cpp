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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/substrait/SubstraitParser.h"

using namespace facebook::velox;
using namespace facebook::velox::substrait;

namespace facebook::velox::substrait::test {

class SubstraitParserTest : public ::testing::Test {
 protected:
  SubstraitParser parser_;
};

TEST_F(SubstraitParserTest, getFunctionType) {
  std::vector<std::string> types;
  parser_.getSubFunctionTypes("sum:opt_i32", types);
  ASSERT_EQ("i32", types[0]);

  types.clear();
  parser_.getSubFunctionTypes("sum:i32", types);
  ASSERT_EQ("i32", types[0]);

  types.clear();
  parser_.getSubFunctionTypes("sum:opt_str_str", types);
  ASSERT_EQ(2, types.size());
  ASSERT_EQ("str", types[0]);
  ASSERT_EQ("str", types[1]);
}

} // namespace facebook::velox::substrait::test

