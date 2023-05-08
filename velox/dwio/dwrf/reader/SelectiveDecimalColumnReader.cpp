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

#include "velox/dwio/dwrf/reader/SelectiveDecimalColumnReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

uint64_t SelectiveDecimalColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  valueDecoder_->skip(numValues);
  scaleDecoder_->skip(numValues);
  return numValues;
}

void SelectiveDecimalColumnReader::seekToRowGroup(uint32_t index) {
  auto positionsProvider = formatData_->seekToRowGroup(index);
  valueDecoder_->seekToRowGroup(positionsProvider);
  scaleDecoder_->seekToRowGroup(positionsProvider);
  // Check that all the provided positions have been consumed.
  VELOX_CHECK(!positionsProvider.hasNext());
}

void SelectiveDecimalColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {

  prepareRead<int64_t>(offset, rows, incomingNulls);

  bool isDense = rows.back() == rows.size() - 1;
  velox::common::Filter* filter = scanSpec_->filter() ? scanSpec_->filter() : &alwaysTrue();

  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }

    if (isDense) {
      processFilter<true>(filter, ExtractToReader(this), rows);
    } else {
      processFilter<false>(filter, ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<true>(filter, DropValues(), rows);
    } else {
      processFilter<false>(filter, DropValues(), rows);
    }
  }
}

void SelectiveDecimalColumnReader::getValues(RowSet rows, VectorPtr* result) {
  auto nullsPtr = nullsInReadRange_
      ? (returnReaderNulls_ ? nullsInReadRange_->as<uint64_t>()
                            : rawResultNulls_)
      : nullptr;

  auto decimalValues = AlignedBuffer::allocate<UnscaledShortDecimal>(numValues_, &memoryPool_);
  auto rawDecimalValues = decimalValues->asMutable<UnscaledShortDecimal>();

  auto scales = scaleBuffer_->as<int64_t>();
  auto values = values_->as<int64_t>();

  // transfer to UnscaledShortDecimal
  int32_t scale = scale_;
  for (vector_size_t i = 0; i < numValues_; i++) {
    if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
      int32_t currentScale = scales[i];
      int64_t value = values[i];

      if (scale > currentScale && static_cast<uint64_t>(scale - currentScale) <= MAX_PRECISION_64) {
        value *= POWERS_OF_TEN[scale - currentScale];
      } else if (scale < currentScale && static_cast<uint64_t>(currentScale - scale) <= MAX_PRECISION_64) {
        value /= POWERS_OF_TEN[currentScale - scale];
      } else if (scale != currentScale) {
        VELOX_FAIL("Decimal scale out of range");
      }

      rawDecimalValues[i] = UnscaledShortDecimal(value);
    }
  }

  values_ = decimalValues;
  rawValues_ = values_->asMutable<char>();
  getFlatValues<UnscaledShortDecimal, UnscaledShortDecimal>(rows, result, type_, true);

  std::cout << "[zuochunwei] SelectiveDecimalColumnReader::getValues =====" << std::endl;
  std::cout << (*result)->toString() << std::endl;
}

const uint32_t SelectiveDecimalColumnReader::MAX_PRECISION_64;
const uint32_t SelectiveDecimalColumnReader::MAX_PRECISION_128;
const int64_t SelectiveDecimalColumnReader::POWERS_OF_TEN[MAX_PRECISION_64 + 1] = { 1,
                                                                                    10,
                                                                                    100,
                                                                                    1000,
                                                                                    10000,
                                                                                    100000,
                                                                                    1000000,
                                                                                    10000000,
                                                                                    100000000,
                                                                                    1000000000,
                                                                                    10000000000,
                                                                                    100000000000,
                                                                                    1000000000000,
                                                                                    10000000000000,
                                                                                    100000000000000,
                                                                                    1000000000000000,
                                                                                    10000000000000000,
                                                                                    100000000000000000,
                                                                                    1000000000000000000};
} // namespace facebook::velox::dwrf
