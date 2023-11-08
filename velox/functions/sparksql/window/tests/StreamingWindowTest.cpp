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
#include "velox/functions/lib/window/tests/WindowTestBase.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

static const std::vector<std::string> kWindowFunctions = {
    std::string("rank()"),
    std::string("dense_rank()"),
    std::string("percent_rank()"),
    std::string("cume_dist()"),
    std::string("row_number()"),
    std::string("ntile(1)"),
    std::string("sum(c2)"),
    std::string("min(c2)"),
    std::string("max(c2)"),
    std::string("count(c2)"),
    std::string("avg(c2)"),
    std::string("sum(1)")};

// The StreamingWindowTestBase class is used to instantiate parameterized window
// function tests. The parameters are based on the function being tested
// and a specific over clause. The window function is tested for the over
// clause and all combinations of frame clauses. Doing so amortizes the
// input vector and DuckDB table construction once across all the frame clauses
// for a (function, over clause) combination.
struct StreamingWindowTestParam {
  const std::string function;
  const std::string overClause;
  const std::string frameClause;
};

class StreamingWindowTestBase : public WindowTestBase {
 protected:
  explicit StreamingWindowTestBase(const StreamingWindowTestParam& testParam)
      : function_(testParam.function),
        overClause_(testParam.overClause),
        frameClause_(testParam.frameClause) {}

  void testStreamingWindowFunction(const std::vector<RowVectorPtr>& vectors) {
    WindowTestBase::testStreamingWindowFunction(
        vectors, function_, {overClause_}, {frameClause_});
  }

  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    velox::functions::window::sparksql::registerWindowFunctions("");
  }

  const std::string function_;
  const std::string overClause_;
  const std::string frameClause_;
};

std::vector<StreamingWindowTestParam> getStreamingWindowTestParams() {
  std::vector<StreamingWindowTestParam> params;
  for (auto function : kWindowFunctions) {
    for (auto overClause : kOverClauses) {
      for (auto frameClause : kFrameClauses) {
        params.push_back({function, overClause, frameClause});
      }
    }
  }
  return params;
}

class StreamingWindowTest
    : public StreamingWindowTestBase,
      public testing::WithParamInterface<StreamingWindowTestParam> {
 public:
  StreamingWindowTest() : StreamingWindowTestBase(GetParam()) {}
};

// Tests all functions with a dataset with uniform distribution of partitions.
TEST_P(StreamingWindowTest, basic) {
  testStreamingWindowFunction({makeSimpleVector(40)});
}

// Tests all functions with a dataset with all rows in a single partition,
// but in 2 input vectors.
TEST_P(StreamingWindowTest, singlePartition) {
  testStreamingWindowFunction(
      {makeSinglePartitionVector(40), makeSinglePartitionVector(50)});
}

// Tests all functions with a dataset in which all partitions have a single row.
TEST_P(StreamingWindowTest, singleRowPartitions) {
  testStreamingWindowFunction({makeSingleRowPartitionsVector(40)});
}

// Tests all functions with a dataset with randomly generated data.
TEST_P(StreamingWindowTest, randomInput) {
  testStreamingWindowFunction({makeRandomInputVector(30)});
}

// Run above tests for all combinations of window function and over clauses.
VELOX_INSTANTIATE_TEST_SUITE_P(
    StreamingWindowTestInstantiation,
    StreamingWindowTest,
    testing::ValuesIn(getStreamingWindowTestParams()));

}; // namespace
}; // namespace facebook::velox::window::test
