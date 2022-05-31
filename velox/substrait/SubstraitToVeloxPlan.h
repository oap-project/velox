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
#include "velox/substrait/TypeUtils.h"

namespace facebook::velox::substrait {

/// This class is used to convert the Substrait plan into Velox plan.
class SubstraitVeloxPlanConverter {
 public:
  SubstraitVeloxPlanConverter(
      memory::MemoryPool* pool,
      bool validationMode = false)
      : pool_(pool), validationMode_(validationMode) {}

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
  /// and the Substrait function name. Initialize the expression
  /// converter based on the constructed function map.
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
  /// Memory pool.
  memory::MemoryPool* pool_;

  /// Filter info for a column used in filter push down.
  class FilterInfo {
   public:
    // Disable null allow.
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

    // Add a lower bound to the range. Multiple lower bounds are
    // regarded to be in 'or' relation.
    void setLower(const std::optional<variant>& left, bool isExclusive) {
      lowerBounds_.emplace_back(left);
      lowerExclusives_.emplace_back(isExclusive);
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    // Add a upper bound to the range. Multiple upper bounds are
    // regarded to be in 'or' relation.
    void setUpper(const std::optional<variant>& right, bool isExclusive) {
      upperBounds_.emplace_back(right);
      upperExclusives_.emplace_back(isExclusive);
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    // Set a list of values to be used in the push down of 'in' expression.
    void setValues(const std::vector<variant>& values) {
      for (const auto& value : values) {
        valuesVector_.emplace_back(value);
      }
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    // Set a value for the not(equal) condition.
    void setNotValue(const std::optional<variant>& notValue) {
      notValue_ = notValue;
      if (!isInitialized_) {
        isInitialized_ = true;
      }
    }

    // Whether this filter map is initialized.
    bool isInitialized_ = false;

    // The null allow.
    bool nullAllowed_ = true;

    // If true, left bound will be exclusive.
    std::vector<bool> lowerExclusives_;

    // If true, right bound will be exclusive.
    std::vector<bool> upperExclusives_;

    // A value should not be equal to.
    std::optional<variant> notValue_ = std::nullopt;

    // The lower bounds in 'or' relation.
    std::vector<std::optional<variant>> lowerBounds_;

    // The upper bounds in 'or' relation.
    std::vector<std::optional<variant>> upperBounds_;

    // The list of values used in 'in' expression.
    std::vector<variant> valuesVector_;
  };

  /// A function returning current function id and adding the plan node id by
  /// one once called.
  std::string nextPlanNodeId();

  /// Check the args of the scalar function. Should be field or
  /// field with literal.
  bool fieldOrWithLiteral(
      const ::substrait::Expression_ScalarFunction& function);

  /// Separate the functions to be two parts:
  /// subfield functions to be handled by the subfieldFilters in HiveConnector,
  /// and remaining functions to be handled by the remainingFilter in
  /// HiveConnector.
  void separateFilters(
      const std::vector<::substrait::Expression_ScalarFunction>&
          scalarFunctions,
      std::vector<::substrait::Expression_ScalarFunction>& subfieldFunctions,
      std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions);

  /// Check whether the chidren functions of this scalar function have the same
  /// column index. Curretly used to check whether the two chilren functions of
  /// 'or' expression are effective on the same column.
  bool chidrenFunctionsOnSameField(
      const ::substrait::Expression_ScalarFunction& function);

  /// Extract the list from in function, and set it to the filter info.
  void setInValues(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap);

  /// Extract the scalar function, and set the filter info for different types
  /// of columns. If reverse is true, the opposite filter info will be set.
  void setFilterMap(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      const std::vector<TypePtr>& inputTypeList,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap,
      bool reverse = false);

  /// Set the filter info for a column base on the information
  /// extracted from filter condition.
  template <typename T>
  void setColInfoMap(
      const std::string& filterName,
      uint32_t colIdx,
      std::optional<variant> literalVariant,
      bool reverse,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap);

  /// Create a multirange to specify the filter 'x != notValue' with:
  /// x > notValue or x < notValue.
  template <TypeKind KIND, typename FilterType>
  void createNotEqualFilter(
      variant notVariant,
      bool nullAllowed,
      std::vector<std::unique_ptr<FilterType>>& colFilters);

  /// Create a values range to handle in filter.
  /// variants: the list of values extracted from the in expression.
  /// inputName: the column input name.
  template <TypeKind KIND>
  void setInFilter(
      const std::vector<variant>& variants,
      bool nullAllowed,
      const std::string& inputName,
      connector::hive::SubfieldFilters& filters);

  /// Set the constructed filters into SubfieldFilters.
  /// The FilterType is used to distinguish BigintRange and
  /// Filter (the base class). This is needed because BigintMultiRange
  /// can only accept the unique ptr of BigintRange as parameter.
  template <TypeKind KIND, typename FilterType>
  void setSubfieldFilter(
      std::vector<std::unique_ptr<FilterType>> colFilters,
      const std::string& inputName,
      bool nullAllowed,
      connector::hive::SubfieldFilters& filters);

  /// Create the subfield filter based on the constructed filter info.
  /// inputName: the input name of a column.
  template <TypeKind KIND, typename FilterType>
  void constructSubfieldFilters(
      uint32_t colIdx,
      const std::string& inputName,
      const std::shared_ptr<FilterInfo>& filterInfo,
      connector::hive::SubfieldFilters& filters);

  /// Construct subfield filters according to the pre-set map of filter info.
  connector::hive::SubfieldFilters mapToFilters(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>> colInfoMap);

  /// Convert subfield functions into subfieldFilters to
  /// be used in Hive Connector.
  connector::hive::SubfieldFilters toSubfieldFilters(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>&
          subfieldFunctions);

  /// Connect all remaining functions with 'and' relation
  /// for the use of remaingFilter in Hive Connector.
  std::shared_ptr<const core::ITypedExpr> connectWithAnd(
      std::vector<std::string> inputNameList,
      std::vector<TypePtr> inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>&
          remainingFunctions);

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

  /// A flag used to specify validation.
  bool validationMode_ = false;
};

} // namespace facebook::velox::substrait
