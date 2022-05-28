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

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/substrait/SubstraitToVeloxExpr.h"

namespace facebook::velox::substrait {

/// This class is used to convert the Substrait plan into Velox plan.
class SubstraitVeloxPlanConverter {
 public:
  /// Used to convert Substrait JoinRel into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::JoinRel& sJoin);

  /// Used to convert Substrait AggregateRel into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::AggregateRel& sAgg);

  /// Used to convert Substrait ProjectRel into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::ProjectRel& sProject);

  /// Used to convert Substrait FilterRel into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::FilterRel& sFilter);

  /// Used to convert Substrait ReadRel into Velox PlanNode.
  /// Index: the index of the partition this item belongs to.
  /// Starts: the start positions in byte to read from the items.
  /// Lengths: the lengths in byte to read from the items.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::ReadRel& sRead,
      u_int32_t& index,
      std::vector<std::string>& paths,
      std::vector<u_int64_t>& starts,
      std::vector<u_int64_t>& lengths);

  /// Used to convert Substrait Rel into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::Rel& sRel);

  /// Used to convert Substrait RelRoot into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::RelRoot& sRoot);

  /// Used to convert Substrait Plan into Velox PlanNode.
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const ::substrait::Plan& sPlan);

  /// Used to construct the function map between the index
  /// and the Substrait function name.
  void constructFuncMap(const ::substrait::Plan& sPlan);

  /// Will return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() {
    return functionMap_;
  }

  /// Will return the index of Partition to be scanned.
  u_int32_t getPartitionIndex() {
    return partitionIndex_;
  }

  /// Will return the paths of the files to be scanned.
  const std::vector<std::string>& getPaths() {
    return paths_;
  }

  /// Will return the starts of the files to be scanned.
  const std::vector<u_int64_t>& getStarts() {
    return starts_;
  }

  /// Will return the lengths to be scanned for each file.
  const std::vector<u_int64_t>& getLengths() {
    return lengths_;
  }

  /// Used to insert certain plan node as input. The plan node
  /// id will start from the setted one.
  void insertInputNode(
      uint64_t inputIdx,
      const std::shared_ptr<const core::PlanNode>& inputNode,
      int planNodeId) {
    inputNodesMap_[inputIdx] = inputNode;
    planNodeId_ = planNodeId;
  }

  /// Used to check if ReadRel specifies an input of stream.
  /// If yes, the index of input stream will be returned.
  /// If not, -1 will be returned.
  int32_t streamIsInput(const ::substrait::ReadRel& sRel);

  /// Multiple conditions are connected to a binary tree structure with
  /// the relation key words, including AND, OR, and etc. Currently, only
  /// AND is supported. This function is used to extract all the Substrait
  /// conditions in the binary tree structure into a vector.
  void flattenConditions(
      const ::substrait::Expression& sFilter,
      std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions);

  /// Used to find the function specification in the constructed function map.
  std::string findFuncSpec(uint64_t id);

  /// Extract join keys from joinExpression.
  /// joinExpression is a boolean condition that describes whether each record
  /// from the left set “match” the record from the right set. The condition
  /// must only include the following operations: AND, ==, field references.
  /// Field references correspond to the direct output order of the data.
  void extractJoinKeys(
      const ::substrait::Expression& joinExpression,
      std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
      std::vector<const ::substrait::Expression::FieldReference*>& rightExprs);

 private:
  class FilterInfo {
   public:
    void forbidsNull() {
      nullAllowed_ = false;
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    // Return the initialization status.
    bool isInitialized() {
      return isInitialized_ ? true : false;
    }

    void setLeft(const std::optional<variant>& left, bool isExclusive) {
      leftBounds_.emplace_back(left);
      leftExclusives_.emplace_back(isExclusive);
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    void setRight(const std::optional<variant>& right, bool isExclusive) {
      rightBounds_.emplace_back(right);
      rightExclusives_.emplace_back(isExclusive);
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    void setValues(const std::vector<variant>& values) {
      for (const auto& value : values) {
        valuesVector_.emplace_back(value);
      }
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    bool isInitialized_ = false;
    // The Null allowing.
    bool nullAllowed_ = true;

    // If true, left bound will be exclusive.
    std::vector<bool> leftExclusives_;
    // If true, right bound will be exclusive.
    std::vector<bool> rightExclusives_;

    std::vector<std::optional<variant>> leftBounds_;
    std::vector<std::optional<variant>> rightBounds_;
    std::vector<variant> valuesVector_;
  };

  bool fieldOrComparedWithLiteral(
      const ::substrait::Expression_ScalarFunction& condition);

  /// The Partition index.
  u_int32_t partitionIndex_;

  /// The file paths to be scanned.
  std::vector<std::string> paths_;

  /// The file starts in the scan.
  std::vector<u_int64_t> starts_;

  /// The lengths to be scanned.
  std::vector<u_int64_t> lengths_;

  /// The unique identification for each PlanNode.
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// The map storing the pre-built plan nodes which can be accessed through
  /// index. This map is only used when the computation of a Substrait plan
  /// depends on other input nodes.
  std::unordered_map<uint64_t, std::shared_ptr<const core::PlanNode>>
      inputNodesMap_;

  /// The Substrait parser used to convert Substrait representations into
  /// recognizable representations.
  std::shared_ptr<SubstraitParser> subParser_{
      std::make_shared<SubstraitParser>()};

  /// The Expression converter used to convert Substrait representations into
  /// Velox expressions.
  std::shared_ptr<SubstraitVeloxExprConverter> exprConverter_;

  /// A function returning current function id and adding the plan node id by
  /// one once called.
  std::string nextPlanNodeId();

  /// Separate the functions to be two parts:
  /// subfieldFunctions can be handled by the subfieldFilters in HiveConnector,
  /// and remainingFunctions can be handled by the remainingFilter in
  /// HiveConnector.
  void separateFilters(
      const std::vector<::substrait::Expression_ScalarFunction>&
          scalarFunctions,
      std::vector<::substrait::Expression_ScalarFunction>& subfieldFunctions,
      std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions);

  bool rangeOnSameField(
      const ::substrait::Expression_ScalarFunction& condition);

  void setInValues(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap);

  void setFilterMap(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      const std::vector<TypePtr>& inputTypeList,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap,
      bool reverse = false);

  connector::hive::SubfieldFilters mapToFilters(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>> colInfoMap);

  /// Used to convert Substrait Filter into Velox SubfieldFilters which will
  /// be used in TableScan.
  connector::hive::SubfieldFilters toSubfieldFilters(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>&
          subfieldFunctions);

  std::shared_ptr<const core::ITypedExpr> connectWithAnd(
      std::vector<std::string> inputNameList,
      std::vector<TypePtr> inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>&
          remainingFunctions);

  template <typename T>
  void setColInfoMap(
      const std::string& filterName,
      uint32_t colIdx,
      std::optional<variant> literalVariant,
      bool reverse,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap);

  template <typename T, typename RangeType, typename MultiRangeType>
  void constructSubfieldFilters(
      uint32_t colIdx,
      const std::string& inputName,
      const std::shared_ptr<FilterInfo>& filterInfo,
      connector::hive::SubfieldFilters& filters);

  void constructSubfieldFiltersForInt(
      uint32_t colIdx,
      const std::string& inputName,
      const std::shared_ptr<FilterInfo>& filterInfo,
      connector::hive::SubfieldFilters& filters);

  void constructSubfieldFiltersForString(
      uint32_t colIdx,
      const std::string& inputName,
      const std::shared_ptr<FilterInfo>& filterInfo,
      connector::hive::SubfieldFilters& filters);

  /// Used to check if some of the input columns of Aggregation
  /// should be combined into a single column. Currently, this case occurs in
  /// final Average. The phase of Aggregation will also be set.
  bool needsRowConstruct(
      const ::substrait::AggregateRel& sAgg,
      core::AggregationNode::Step& aggStep);

  /// Used to convert AggregateRel into Velox plan node.
  /// This method will add a Project node before Aggregation to combine columns.
  std::shared_ptr<const core::PlanNode> toVeloxAggWithRowConstruct(
      const ::substrait::AggregateRel& sAgg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /// Used to convert AggregateRel into Velox plan node.
  /// The output of child node will be used as the input of Aggregation.
  std::shared_ptr<const core::PlanNode> toVeloxAgg(
      const ::substrait::AggregateRel& sAgg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);
};

} // namespace facebook::velox::substrait
