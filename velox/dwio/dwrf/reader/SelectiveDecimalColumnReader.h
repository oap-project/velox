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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

class SelectiveDecimalColumnReader
    : public dwio::common::SelectiveColumnReader {
 public:
  using ValueType = int128_t;

  SelectiveDecimalColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
      DwrfParams& params,
      common::ScanSpec& scanSpec) 
    : SelectiveColumnReader(nodeType, params, scanSpec, nodeType->type) {
      
    auto& stripe = params.stripeStreams();

    EncodingKey encodingKey{nodeType_->id, params.flatMapContext().sequence};
    auto values = encodingKey.forKind(proto::Stream_Kind_DATA);
    auto scale = encodingKey.forKind(proto::Stream_Kind_NANO_DATA); // equal to orc::proto::Stream_Kind_SECONDARY
    
    bool valuesVInts = stripe.getUseVInts(values);
    bool scaleVInts = stripe.getUseVInts(scale);

    format_ = stripe.format();
    if (format_ == velox::dwrf::DwrfFormat::kDwrf) {
       VELOX_FAIL("dwrf unsupport decimal");
    } else if (format_ == velox::dwrf::DwrfFormat::kOrc) {
      auto encoding = stripe.getEncoding(encodingKey);

      auto encodingKind = encoding.kind();
      VELOX_CHECK(encodingKind == proto::ColumnEncoding_Kind_DIRECT || encodingKind == proto::ColumnEncoding_Kind_DIRECT_V2);
      version_ = convertRleVersion(encodingKind);

      valueDecoder_ = createDirectDecoder<true>(stripe.getStream(values, true), valuesVInts, LONG_BYTE_SIZE);
      scaleDecoder_ = createRleDecoder<true>(stripe.getStream(scale, true), version_, params.pool(), scaleVInts, INT_BYTE_SIZE);
    } else {
      VELOX_FAIL("invalid stripe format");
    }
  }

  void seekToRowGroup(uint32_t index) override;

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls) override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  template <bool isDense>
  void SelectiveIntegerColumnReader::processValueHook(
      RowSet rows,
      ValueHook* hook) {
    VELOX_FAIL("TODO: orc decimal process ValueHook");
  }

  template <bool isDense, typename ExtractValues>
  void processFilter(
    velox::common::Filter* filter,
    ExtractValues extractValues,
    RowSet rows) {  
    switch (filter ? filter->kind() : velox::common::FilterKind::kAlwaysTrue) {
      case velox::common::FilterKind::kAlwaysTrue:
        readHelper<velox::common::AlwaysTrue, isDense>(filter, rows, extractValues);
        break;
      default:
        VELOX_FAIL("TODO: orc decimal process filter unsupport cases");
        break;
    }
  }

  template <bool dense>
  void readHelper(
    velox::common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {

    vector_size_t numRows = rows.back() + 1;

    numValues_ = 0;

    readOffset_ += numRows;
  }

private:
  dwrf::DwrfFormat format_;
  RleVersion version_;

  std::unique_ptr<dwio::common::IntDecoder<true>> valueDecoder_;
  std::unique_ptr<dwio::common::IntDecoder<true>> scaleDecoder_;

  int32_t scale_ = 0;
};

} // namespace facebook::velox::dwrf
