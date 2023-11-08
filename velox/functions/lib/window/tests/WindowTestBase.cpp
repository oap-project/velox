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
#include "velox/functions/lib/window/tests/WindowTestBase.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include <iostream>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

WindowTestBase::QueryInfo WindowTestBase::buildWindowQuery(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause) {
  std::string functionSql =
      fmt::format("{} over ({} {})", function, overClause, frameClause);
  auto op = PlanBuilder()
                .setParseOptions(options_)
                .values(input)
                .window({functionSql})
                .planNode();

  auto rowType = asRowType(input[0]->type());
  std::string columnsString = folly::join(", ", rowType->names());
  std::string querySql =
      fmt::format("SELECT {}, {} FROM tmp", columnsString, functionSql);

  return {op, functionSql, querySql};
}

namespace {
void splitOrderBy(
    const std::string& overClause,
    vector_size_t startIdx,
    vector_size_t length,
    std::vector<std::string>& orderByClauses) {
  auto orderByPars = overClause.substr(startIdx, length);
  std::istringstream tokenStream(orderByPars);
  std::string token;
  while (std::getline(tokenStream, token, ',')) {
    orderByClauses.push_back(token);
  }
}
}; // namespace

WindowTestBase::QueryInfo WindowTestBase::makeStreamingWindow(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause) {
  std::string functionSql =
      fmt::format("{} over ({} {})", function, overClause, frameClause);
  core::PlanNodePtr op = nullptr;
  std::vector<std::string> orderByClauses;

  std::string partitionByStr = "partition by";
  auto partitionByStartIdx = overClause.find(partitionByStr);

  std::string orderByStr = "order by";
  auto orderByStartIdx = overClause.find(orderByStr);

  // Extract the partition by keys.
  if (partitionByStartIdx != std::string::npos) {
    auto startIdx = partitionByStartIdx + partitionByStr.length();
    auto length = 0;
    if (orderByStartIdx != std::string::npos) {
      length = orderByStartIdx - startIdx;
    } else {
      length = overClause.length() - startIdx;
    }
    splitOrderBy(overClause, startIdx, length, orderByClauses);
  }

  // Add NULLS FIRST after partition by keys. Because the
  // defaultPartitionSortOrder in Window.cpp is NULLS FIRST.
  for (auto i = 0; i < orderByClauses.size(); i++) {
    if (orderByClauses[i].find("NULLS") == std::string::npos &&
        orderByClauses[i].find("nulls") == std::string::npos) {
      orderByClauses[i] = orderByClauses[i] + " NULLS FIRST";
    }
  }

  // Extract the order by keys.
  if (orderByStartIdx != std::string::npos) {
    auto startIdx = orderByStartIdx + orderByStr.length();
    auto length = overClause.length() - startIdx;
    splitOrderBy(overClause, startIdx, length, orderByClauses);
  }

  // Sort the input data before streaming window.
  op = PlanBuilder()
           .setParseOptions(options_)
           .values(input)
           .orderBy(orderByClauses, false)
           .streamingWindow({functionSql})
           .planNode();

  auto rowType = asRowType(input[0]->type());
  std::string columnsString = folly::join(", ", rowType->names());
  std::string querySql =
      fmt::format("SELECT {}, {} FROM tmp", columnsString, functionSql);

  return {op, functionSql, querySql};
}

RowVectorPtr WindowTestBase::makeSimpleVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row % 7; }, nullEvery(15)),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
  });
}

RowVectorPtr WindowTestBase::makeSinglePartitionVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row; }, nullEvery(7)),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
  });
}

RowVectorPtr WindowTestBase::makeSingleRowPartitionsVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
  });
}

VectorPtr WindowTestBase::makeRandomInputVector(
    const TypePtr& type,
    vector_size_t size,
    float nullRatio) {
  VectorFuzzer::Options options;
  options.vectorSize = size;
  options.nullRatio = nullRatio;
  options.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  VectorFuzzer fuzzer(options, pool_.get(), 0);
  return fuzzer.fuzzFlat(type);
}

RowVectorPtr WindowTestBase::makeRandomInputVector(vector_size_t size) {
  boost::random::mt19937 gen;
  // Frame index values require integer values > 0.
  auto genRandomFrameValue = [&](vector_size_t /*row*/) {
    return boost::random::uniform_int_distribution<int>(1)(gen);
  };
  return makeRowVector(
      {makeRandomInputVector(BIGINT(), size, 0.2),
       makeRandomInputVector(VARCHAR(), size, 0.3),
       makeFlatVector<int64_t>(size, genRandomFrameValue),
       makeFlatVector<int64_t>(size, genRandomFrameValue)});
}

void WindowTestBase::testWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::vector<std::string>& overClauses,
    const std::vector<std::string>& frameClauses,
    bool createTable) {
  if (createTable) {
    createDuckDbTable(input);
  }

  for (const auto& overClause : overClauses) {
    for (auto& frameClause : frameClauses) {
      auto queryInfo =
          buildWindowQuery(input, function, overClause, frameClause);
      SCOPED_TRACE(queryInfo.functionSql);
      assertQuery(queryInfo.planNode, queryInfo.querySql);
    }
  }
}

void WindowTestBase::testKRangeFrames(const std::string& function) {
  // The current support for k Range frames is limited to ascending sort
  // orders without null values. Frames clauses generating empty frames
  // are also not supported.

  // For deterministic results its expected that rows have a fixed ordering
  // in the partition so that the range frames are predictable. So the
  // input table.
  vector_size_t size = 100;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 10; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 4 + 1; }),
  });

  const std::string overClause = "partition by c0 order by c1";
  const std::vector<std::string> kRangeFrames = {
      "range between 5 preceding and current row",
      "range between current row and 5 following",
      "range between 5 preceding and 5 following",
      "range between unbounded preceding and 5 following",
      "range between 5 preceding and unbounded following",

      "range between c3 preceding and current row",
      "range between current row and c3 following",
      "range between c2 preceding and c3 following",
      "range between unbounded preceding and c3 following",
      "range between c3 preceding and unbounded following",
  };

  testWindowFunction({vectors}, function, {overClause}, kRangeFrames);
}

void WindowTestBase::testStreamingWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::vector<std::string>& overClauses,
    const std::vector<std::string>& frameClauses,
    bool createTable) {
  if (createTable) {
    createDuckDbTable(input);
  }

  for (const auto& overClause : overClauses) {
    for (auto& frameClause : frameClauses) {
      auto queryInfo =
          makeStreamingWindow(input, function, overClause, frameClause);
      SCOPED_TRACE(queryInfo.functionSql);
      assertQuery(queryInfo.planNode, queryInfo.querySql);
    }
  }
}

void WindowTestBase::assertWindowFunctionError(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& errorMessage) {
  assertWindowFunctionError(input, function, overClause, "", errorMessage);
}

void WindowTestBase::assertWindowFunctionError(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause,
    const std::string& errorMessage) {
  auto queryInfo = buildWindowQuery(input, function, overClause, frameClause);
  SCOPED_TRACE(queryInfo.functionSql);

  VELOX_ASSERT_THROW(
      assertQuery(queryInfo.planNode, queryInfo.querySql), errorMessage);
}

}; // namespace facebook::velox::window::test
