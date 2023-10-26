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
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class ArrayAggTest : public AggregationTestBase {
 protected:
  static void SetUpTestCase() {
    functions::aggregate::sparksql::registerAggregateFunctions("");
  }
};

TEST_F(ArrayAggTest, sortedGroupBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 1, 2, 1}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
      makeFlatVector<int32_t>({11, 44, 22, 55, 33, 66, 77}),
  });

  createDuckDbTable({data});

  // Sorted aggregations over same inputs.
  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {"c0"},
                      {
                          "array_agg(c1 ORDER BY c2 DESC)",
                          "array_agg(c1 ORDER BY c3)",
                      })
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT c0, array_agg(c1 ORDER BY c2 DESC), array_agg(c1 ORDER BY c3) "
          " FROM tmp GROUP BY 1");

  // Sorted aggregations over different inputs.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation(
                 {"c0"},
                 {
                     "array_agg(c1 ORDER BY c2 DESC)",
                     "array_agg(c2 ORDER BY c3)",
                 })
             .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT c0, array_agg(c1 ORDER BY c2 DESC), array_agg(c2 ORDER BY c3) "
          " FROM tmp GROUP BY 1");

  // Sorted aggregation with multiple sorting keys.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation(
                 {"c0"},
                 {
                     "array_agg(c1 ORDER BY c2 DESC, c3)",
                 })
             .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT c0, array_agg(c1 ORDER BY c2 DESC, c3) "
          " FROM tmp GROUP BY 1");
}

TEST_F(ArrayAggTest, global) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector({makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row * 2; })})};

  createDuckDbTable(vectors);
  testAggregations(
      vectors, {}, {"array_agg(c0)"}, "SELECT array_agg(c0) FROM tmp");
}

TEST_F(ArrayAggTest, globalWithNullData) {
  auto data = makeRowVector({makeNullableFlatVector<int64_t>(
      {1, std::nullopt, 3, 4, std::nullopt, 6, 7})});
  auto expectedResult =
      makeRowVector({makeArrayVector<int64_t>({{1, 3, 4, 6, 7}})});

  testAggregations({data}, {}, {"array_agg(c0)"}, {expectedResult});

  data = makeRowVector({makeNullableFlatVector<int64_t>(
      {std::nullopt, 33, std::nullopt, std::nullopt, 66, std::nullopt, 77})});
  expectedResult = makeRowVector({makeArrayVector<int64_t>({{33, 66, 77}})});
  testAggregations({data}, {}, {"array_agg(c0)"}, {expectedResult});
}

TEST_F(ArrayAggTest, globalNoData) {
  auto data = makeRowVector(ROW({"c0"}, {INTEGER()}), 0);
  auto expectedResult = makeRowVector({makeArrayVector<int32_t>({{}})});
  testAggregations({data}, {}, {"array_agg(c0)"}, {expectedResult});
}

TEST_F(ArrayAggTest, sortedGlobal) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
      makeFlatVector<int32_t>({11, 33, 22, 44, 66, 55, 77}),
  });

  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {},
                      {
                          "array_agg(c0 ORDER BY c1 DESC)",
                          "array_agg(c0 ORDER BY c2)",
                      })
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT array_agg(c0 ORDER BY c1 DESC), array_agg(c0 ORDER BY c2) FROM tmp");

  // Sorted aggregations over different inputs.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation(
                 {},
                 {
                     "array_agg(c0 ORDER BY c1 DESC)",
                     "array_agg(c1 ORDER BY c2)",
                 })
             .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT array_agg(c0 ORDER BY c1 DESC), array_agg(c1 ORDER BY c2) FROM tmp");

  // Sorted aggregation with multiple sorting keys.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation(
                 {},
                 {
                     "array_agg(c0 ORDER BY c1 DESC, c2)",
                 })
             .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT array_agg(c0 ORDER BY c1 DESC, c2) FROM tmp");
}
} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
