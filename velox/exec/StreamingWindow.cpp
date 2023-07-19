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
#include "velox/exec/StreamingWindow.h"
#include <iostream>
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

void StreamingWindow::addPreInput() {
  if (prevInput_ && numPartitions_ > 0) {
    data_->clear();
    auto startRows = partitionStartRows_[numPartitions_];
    preLastPartitionNums_ = partitionStartRows_[numPartitions_ + 1] - startRows;

    for (auto col = 0; col < prevInput_->childrenSize(); ++col) {
      decodedInputVectors_[col].decode(*prevInput_->childAt(col));
    }

    auto finalStartRow = startRows;
    if (startRows >= prePreLastPartitionNums_) {
      finalStartRow = startRows - prePreLastPartitionNums_;
    }

    // Add all the rows into the RowContainer.
    for (auto row = finalStartRow; row < prevInput_->size(); ++row) {
      char* newRow = data_->newRow();
      for (auto col = 0; col < prevInput_->childrenSize(); ++col) {
        data_->store(decodedInputVectors_[col], row, newRow, col);
      }
    }
  }
}

void StreamingWindow::addInput(RowVectorPtr input) {
  addPreInput();
  for (auto col = 0; col < input->childrenSize(); ++col) {
    decodedInputVectors_[col].decode(*input->childAt(col));
  }

  // Add all the rows into the RowContainer.
  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }
  }

  input_ = std::move(input);
  numRows_ = data_->numRows();
}

void StreamingWindow::createPeerAndFrameBuffers() {
  // TODO: This computation needs to be revised. It only takes into account
  // the input columns size. We need to also account for the output columns.
  numRowsPerOutput_ = outputBatchRows(data_->estimateRowSize());

  numRowsPerOutput_ = numRows_;

  peerStartBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());
  peerEndBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());

  auto numFuncs = windowFunctions_.size();

  frameStartBuffers_.clear();
  frameEndBuffers_.clear();
  validFrames_.clear();
  frameStartBuffers_.reserve(numFuncs);
  frameEndBuffers_.reserve(numFuncs);
  validFrames_.reserve(numFuncs);

  for (auto i = 0; i < numFuncs; i++) {
    BufferPtr frameStartBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    BufferPtr frameEndBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    frameStartBuffers_.push_back(frameStartBuffer);
    frameEndBuffers_.push_back(frameEndBuffer);
    validFrames_.push_back(SelectivityVector(numRowsPerOutput_));
  }
}

void StreamingWindow::noMoreInput() {
  Operator::noMoreInput();
}

bool StreamingWindow::isFinished() {
  return noMoreInput_ && input_ == nullptr && preLastPartitionNums_ == 0 &&
      outputs_.size() == 0;
}

RowVectorPtr StreamingWindow::getResult(bool isLastPartition) {
  auto numRowsPerBatch = outputBatchRows(data_->estimateRowSize());
  RowVectorPtr finalResult = std::dynamic_pointer_cast<RowVector>(
      outputs_[0]->slice(0, outputs_[0]->size()));
  // Get the finalResult from outputs_ based on the output size;
  auto batchSize = outputs_[0]->size();
  auto i = 1;
  if (batchSize > numRowsPerBatch) {
    auto length = numRowsPerBatch - batchSize;
    finalResult = std::dynamic_pointer_cast<RowVector>(
        outputs_[0]->slice(0, numRowsPerBatch));
    outputs_[0] = std::dynamic_pointer_cast<RowVector>(
        outputs_[0]->slice(numRowsPerBatch, length));
  } else {
    for (; i < outputs_.size(); i++) {
      if (batchSize + outputs_[i]->size() > numRowsPerBatch) {
        auto position = numRowsPerBatch - batchSize;
        auto preResult = std::dynamic_pointer_cast<RowVector>(
            outputs_[i]->slice(0, position));
        finalResult->append(preResult.get());
        auto length = outputs_[i]->size() - position;
        outputs_[i] = std::dynamic_pointer_cast<RowVector>(
            outputs_[i]->slice(position, length));
        break;
      } else {
        finalResult->append(outputs_[i].get());
      }
    }
  }

  if (finalResult->size() == numRowsPerBatch || isLastPartition) {
    outputs_.erase(outputs_.begin(), outputs_.begin() + i);
    return finalResult;
  } else {
    return nullptr;
  }
}

RowVectorPtr StreamingWindow::createOutput() {
  // Init sortedRows_
  sortedRows_.clear();
  sortedRows_.resize(numRows_);
  RowContainerIterator iter;
  data_->listRows(&iter, numRows_, sortedRows_.data());

  if (partitionStartRows_.size() > 2) {
    prePreLastPartitionNums_ = partitionStartRows_[numPartitions_ + 1] -
        partitionStartRows_[numPartitions_];
  }

  partitionStartRows_.clear();
  numPartitions_ = 0;
  computePartitionStartRows();

  if (!noMoreInput_) {
    preLastPartitionNums_ = partitionStartRows_[numPartitions_ + 1] -
        partitionStartRows_[numPartitions_];
  } else {
    preLastPartitionNums_ = 0;
  }

  auto numOutputRows = partitionStartRows_[numPartitions_];
  if (numPartitions_ == 0 && !noMoreInput_) {
    // only one group, need wait the next partition to handle.

    prevInput_ = input_;
    input_ = nullptr;

    return nullptr;
  }

  if (numPartitions_ == 0) {
    numOutputRows = partitionStartRows_[numPartitions_ + 1];
  }

  currentPartition_ = 0;
  numProcessedRows_ = 0;

  createPeerAndFrameBuffers();

  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numOutputRows, operatorCtx_->pool()));

  // Set all passthrough input columns.
  for (int i = 0; i < numInputColumns_; ++i) {
    data_->extractColumn(
        sortedRows_.data(), numOutputRows, i, result->childAt(i));
  }

  // Construct vectors for the window function output columns.
  std::vector<VectorPtr> windowOutputs;
  windowOutputs.reserve(windowFunctions_.size());
  for (int i = numInputColumns_; i < outputType_->size(); i++) {
    auto output = BaseVector::create(
        outputType_->childAt(i), numOutputRows, operatorCtx_->pool());
    windowOutputs.emplace_back(std::move(output));
  }

  // Compute the output values of window functions.
  callApplyLoop(numOutputRows, windowOutputs);

  for (int j = numInputColumns_; j < outputType_->size(); j++) {
    result->childAt(j) = windowOutputs[j - numInputColumns_];
  }

  prevInput_ = input_;
  input_ = nullptr;

  outputs_.push_back(result);
  return getResult(false);
}

RowVectorPtr StreamingWindow::getOutput() {
  if (!input_) {
    if (noMoreInput_ && preLastPartitionNums_ != 0) {
      // handle the last partition
      addPreInput();
      numRows_ = data_->numRows();
      return createOutput();
    }

    if (noMoreInput_ && outputs_.size() > 0) {
      return getResult(true);
    }

    return nullptr;
  }
  return createOutput();
}

} // namespace facebook::velox::exec
