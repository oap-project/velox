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

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/CheckedArithmeticImpl.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql::aggregates {

template <typename UnscaledType>
struct DecimalSum {
  UnscaledType sum = UnscaledType(0);
  int32_t isEmpty{1};
};

template <
    typename TInput,
    typename TAccumulator,
    typename TResult = TAccumulator>
class DecimalSumAggregate : public exec::Aggregate {
 public:
  explicit DecimalSumAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(DecimalSum<TAccumulator>);
  }

  int32_t accumulatorAlignmentSize() const override {
    return sizeof(DecimalSum<TAccumulator>);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (exec::Aggregate::value<TAccumulator>(groups[i]))
          DecimalSum<TAccumulator>();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::FLAT);
    auto vector = (*result)->as<FlatVector<TResult>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    TResult* rawValues = vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto* decimalSum = accumulator(group);
        rawValues[i] = decimalSum->sum;
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::ROW);
    auto rowVector = (*result)->as<RowVector>();
    auto sumVector = rowVector->childAt(0)->asFlatVector<TAccumulator>();
    auto isEmptyVector = rowVector->childAt(1)->asFlatVector<int32_t>();

    rowVector->resize(numGroups);
    sumVector->resize(numGroups);
    isEmptyVector->resize(numGroups);

    TAccumulator* rawSums = sumVector->mutableRawValues();
    int32_t* rawIsEmpty = isEmptyVector->mutableRawValues();
    uint64_t* rawNulls = getRawNulls(rowVector);

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto* decimalSum = accumulator(group);
        rawSums[i] = decimalSum->sum;
        rawIsEmpty[i] = decimalSum->isEmpty;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInput>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], TAccumulator(value));
        });
      } else {
        rows.applyToSelected(
            [&](vector_size_t i) { updateNullValue(groups[i]); });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedRaw_.isNullAt(i)) {
          updateNullValue(groups[i]);
          return;
        }
        updateNonNullValue(
            groups[i], TAccumulator(decodedRaw_.valueAt<TInput>(i)));
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInput>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], (TAccumulator)data[i]);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(
            groups[i], TAccumulator(decodedRaw_.valueAt<TInput>(i)));
      });
    }
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInput>(0);
        const auto numRows = rows.countSelected();
        auto totalSum = TAccumulator(value) * numRows;
        updateNonNullValue(group, totalSum);
      } else {
        updateNullValue(group);
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedRaw_.isNullAt(i)) {
          updateNonNullValue(
              group, TAccumulator(decodedRaw_.valueAt<TInput>(i)));
        } else {
          updateNullValue(group);
        }
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInput>();
      TAccumulator totalSum(0);
      rows.applyToSelected([&](vector_size_t i) {
        totalSum = functions::checkedPlus<TAccumulator>(
            totalSum, TAccumulator(data[i]));
      });
      updateNonNullValue<false>(group, totalSum);
    } else {
      TAccumulator totalSum(0);
      rows.applyToSelected([&](vector_size_t i) {
        totalSum = functions::checkedPlus<TAccumulator>(
            totalSum, TAccumulator(decodedRaw_.valueAt<TInput>(i)));
      });
      updateNonNullValue(group, TAccumulator(totalSum));
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    VELOX_CHECK_EQ(
        decodedPartial_.base()->encoding(), VectorEncoding::Simple::ROW);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto baseSumVector =
        baseRowVector->childAt(0)->as<SimpleVector<TAccumulator>>();
    auto baseIsEmptyVector =
        baseRowVector->childAt(1)->as<SimpleVector<bool>>();
    DCHECK(baseIsEmptyVector);

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto sum = baseSumVector->valueAt(decodedIndex);
        auto isEmpty = baseIsEmptyVector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], TAccumulator(sum));
        });
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPartial_.isNullAt(i)) {
          return;
        }
        auto decodedIndex = decodedPartial_.index(i);
        updateNonNullValue(groups[i], baseSumVector->valueAt(decodedIndex));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        updateNonNullValue(groups[i], baseSumVector->valueAt(decodedIndex));
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    VELOX_CHECK_EQ(
        decodedPartial_.base()->encoding(), VectorEncoding::Simple::ROW);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPartial_.base());
    auto baseSumVector =
        baseRowVector->childAt(0)->as<SimpleVector<TAccumulator>>();
    auto baseIsEmptyVector =
        baseRowVector->childAt(1)->as<SimpleVector<bool>>();
    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        const auto numRows = rows.countSelected();
        auto totalSum = baseSumVector->valueAt(decodedIndex) * numRows;
        updateNonNullValue(group, totalSum);
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedPartial_.isNullAt(i)) {
          auto decodedIndex = decodedPartial_.index(i);
          updateNonNullValue(group, baseSumVector->valueAt(decodedIndex));
        }
      });
    } else {
      TAccumulator totalSum(0);
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        totalSum = functions::checkedPlus(
            totalSum, baseSumVector->valueAt(decodedIndex));
      });
      updateNonNullValue(group, totalSum);
    }
  }

 private:
  template <bool tableHasNulls = true>
  inline void updateNonNullValue(char* group, TAccumulator value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto decimalSum = accumulator(group);
    decimalSum->sum =
        functions::checkedPlus<TAccumulator>(decimalSum->sum, value);
    decimalSum->isEmpty = false;
  }

  inline void updateNullValue(char* group) {
    auto decimalSum = accumulator(group);
    decimalSum->isEmpty = decimalSum->isEmpty && true;
  }

  inline DecimalSum<TAccumulator>* accumulator(char* group) {
    return exec::Aggregate::value<DecimalSum<TAccumulator>>(group);
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

bool registerDecimalSumAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("DECIMAL(a_precision, a_scale)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build(),
  };

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::SHORT_DECIMAL:
            return std::make_unique<
                DecimalSumAggregate<UnscaledShortDecimal, UnscaledLongDecimal>>(
                resultType);
          case TypeKind::LONG_DECIMAL:
            return std::make_unique<
                DecimalSumAggregate<UnscaledLongDecimal, UnscaledLongDecimal>>(
                resultType);
          case TypeKind::ROW: {
            DCHECK(!exec::isRawInput(step));
            auto sumInputType = inputType->asRow().childAt(0);
            switch (sumInputType->kind()) {
              case TypeKind::SHORT_DECIMAL:
                return std::make_unique<DecimalSumAggregate<
                    UnscaledShortDecimal,
                    UnscaledLongDecimal>>(resultType);
              case TypeKind::LONG_DECIMAL:
                return std::make_unique<DecimalSumAggregate<
                    UnscaledLongDecimal,
                    UnscaledLongDecimal>>(resultType);
              default:
                VELOX_FAIL(
                    "Unknown sum type for {} aggregation {}",
                    name,
                    sumInputType->kindName());
            }
          }
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
}

} // namespace facebook::velox::functions::sparksql::aggregates