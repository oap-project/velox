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

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/Window.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

/// This is a very simple in-Memory implementation of a Window Operator
/// to compute window functions.
///
/// This operator uses a very naive algorithm that sorts all the input
/// data with a combination of the (partition_by keys + order_by keys)
/// to obtain a full ordering of the input. We can easily identify
/// partitions while traversing this sorted data in order.
/// It is also sorted in the order required for the WindowFunction
/// to process it.
///
/// We will revise this algorithm in the future using a HashTable based
/// approach pending some profiling results.
class StreamingWindow : public Window {
 public:
  StreamingWindow(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::WindowNode>& windowNode)
      : Window(operatorId, driverCtx, windowNode) {}

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void createPeerAndFrameBuffers() override;

 private:
  // Populate output_ vector using specified number of groups from the beginning
  // of the groups_ vector.
  RowVectorPtr createOutput();

  RowVectorPtr getResult(bool isLastPartition);

  void addPreInput();

  // Previous input vector. Used to compare grouping keys for groups which span
  // batches.
  RowVectorPtr prevInput_;

  // Number of rows in pre last partitions.
  vector_size_t preLastPartitionNums_ = 0;

  // Number of rows in pre last partitions.
  vector_size_t prePreLastPartitionNums_ = 0;

  std::vector<RowVectorPtr> outputs_;
};

} // namespace facebook::velox::exec
