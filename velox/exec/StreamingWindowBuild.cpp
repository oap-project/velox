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

#include "velox/exec/StreamingWindowBuild.h"

namespace facebook::velox::exec {

StreamingWindowBuild::StreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool)
    : WindowBuild(windowNode, pool) {
  partitionStartRows_.resize(0);
  sortedRows_.resize(0);
}

void StreamingWindowBuild::startNewPartition() {
  partitionStartRows_.push_back(sortedRows_.size());
  numRows_ = sortedRows_.size();
  sortedRows_.insert(
      sortedRows_.end(), partitionRows_.begin(), partitionRows_.end());
  partitionRows_.clear();
}

void StreamingWindowBuild::addInput(RowVectorPtr input) {
  for (auto col = 0; col < input->childrenSize(); ++col) {
    decodedInputVectors_[col].decode(*input->childAt(col));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    if (previousRow_ != nullptr &&
        compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_)) {
      startNewPartition();
    }

    partitionRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

void StreamingWindowBuild::noMoreInput() {
  startNewPartition();
  partitionStartRows_.push_back(sortedRows_.size());
  numRows_ = sortedRows_.size();
}

std::unique_ptr<WindowPartition> StreamingWindowBuild::nextPartition() {
  VELOX_CHECK(partitionStartRows_.size() > 0, "No window partitions available")

  currentPartition_++;
  VELOX_CHECK(
      currentPartition_ <= partitionStartRows_.size() - 2,
      "All window partitions consumed");

  // Erase previous partition.
  if (currentPartition_ > 0) {
    auto numPreviousPartitionRows = partitionStartRows_[currentPartition_] -
        partitionStartRows_[currentPartition_ - 1];
    data_->eraseRows(folly::Range<char**>(
        sortedRows_.data() + partitionStartRows_[currentPartition_ - 1],
        numPreviousPartitionRows));
  }

  auto windowPartition = std::make_unique<WindowPartition>(
      data_.get(), inputColumns_, sortKeyInfo_);
  // There is partition data available now.
  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);
  windowPartition->resetPartition(partition);

  return windowPartition;
}

bool StreamingWindowBuild::hasNextPartition() {
  return partitionStartRows_.size() > 0 &&
      currentPartition_ < int(partitionStartRows_.size() - 2);
}

} // namespace facebook::velox::exec
