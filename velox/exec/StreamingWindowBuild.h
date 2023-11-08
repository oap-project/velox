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

#include "velox/exec/WindowBuild.h"

namespace facebook::velox::exec {

// Need to ensure that the input data is already sorted. The previous partition
// will be erased when calling the next partition to reduce the memory
// footprint.
class StreamingWindowBuild : public WindowBuild {
 public:
  StreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      velox::memory::MemoryPool* pool);

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  std::unique_ptr<WindowPartition> nextPartition() override;

  bool hasNextPartition() override;

  bool needsInput() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return partitionStartRows_.size() == 0 ||
        currentPartition_ == partitionStartRows_.size() - 2;
  }

 private:
  // Create a new partition.
  void startNewPartition();

  // Vector of pointers to each input row in the data_ RowContainer.
  // The rows are sorted by partitionKeys + sortKeys. This total
  // ordering can be used to split partitions (with the correct
  // order by) for the processing.
  std::vector<char*> sortedRows_;

  // Holds rows within the current partition.
  std::vector<char*> partitionRows_;

  // This is a vector that gives the index of the start row
  // (in sortedRows_) of each partition in the RowContainer data_.
  // This auxiliary structure helps demarcate partitions.
  std::vector<vector_size_t> partitionStartRows_;

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  // Current partition being output. Used to construct WindowPartitions
  // during resetPartition.
  vector_size_t currentPartition_ = -1;
};

} // namespace facebook::velox::exec
