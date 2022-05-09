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

#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/TypeUtils.h"

namespace facebook::velox::substrait {

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::AggregateRel& sAgg) {
  std::shared_ptr<const core::PlanNode> childNode;
  if (sAgg.has_input()) {
    childNode = toVeloxPlan(sAgg.input());
  } else {
    VELOX_FAIL("Child Rel is expected in AggregateRel.");
  }
  core::AggregationNode::Step aggStep;
  // Get aggregation phase and check if there are input columns need to be
  // combined into row.
  if (needsRowConstruct(sAgg, aggStep)) {
    return toVeloxAggWithRowConstruct(sAgg, childNode, aggStep);
  }
  return toVeloxAgg(sAgg, childNode, aggStep);
}

std::shared_ptr<const core::PlanNode>
SubstraitVeloxPlanConverter::toVeloxAggWithRowConstruct(
    const ::substrait::AggregateRel& sAgg,
    const std::shared_ptr<const core::PlanNode>& childNode,
    const core::AggregationNode::Step& aggStep) {
  // Will add a Project node before Aggregate node to combine columns into row.
  std::vector<std::shared_ptr<const core::ITypedExpr>> constructExprs;
  const auto& groupings = sAgg.groupings();
  const auto& constructInputType = childNode->outputType();

  // Handle groupings.
  uint32_t groupingOutIdx = 0;
  for (const auto& grouping : groupings) {
    const auto& groupingExprs = grouping.grouping_expressions();
    for (const auto& groupingExpr : groupingExprs) {
      // Velox's groupings are limited to be Field.
      auto fieldExpr = exprConverter_->toVeloxExpr(
          groupingExpr.selection(), constructInputType);
      constructExprs.push_back(fieldExpr);
      groupingOutIdx += 1;
    }
  }

  // Handle aggregations.
  std::vector<std::string> aggFuncNames;
  aggFuncNames.reserve(sAgg.measures().size());
  std::vector<TypePtr> aggOutTypes;
  aggOutTypes.reserve(sAgg.measures().size());

  for (const auto& smea : sAgg.measures()) {
    const auto& aggFunction = smea.measure();
    std::string funcName = subParser_->findVeloxFunction(
        functionMap_, aggFunction.function_reference());
    aggFuncNames.emplace_back(funcName);
    aggOutTypes.emplace_back(
        toVeloxType(subParser_->parseType(aggFunction.output_type())->type));
    if (funcName == "avg") {
      // Will use row constructor to combine the sum and count columns into row.
      if (aggFunction.args().size() != 2) {
        VELOX_FAIL("Final average should have two args.");
      }
      std::vector<std::shared_ptr<const core::ITypedExpr>> aggParams;
      aggParams.reserve(aggFunction.args().size());
      for (const auto& arg : aggFunction.args()) {
        aggParams.emplace_back(
            exprConverter_->toVeloxExpr(arg, constructInputType));
      }
      auto constructExpr = std::make_shared<const core::CallTypedExpr>(
          ROW({"sum", "count"}, {DOUBLE(), BIGINT()}),
          std::move(aggParams),
          "row_constructor");
      constructExprs.emplace_back(constructExpr);
    } else {
      if (aggFunction.args().size() != 1) {
        VELOX_FAIL("Expect only one arg.");
      }
      for (const auto& arg : aggFunction.args()) {
        constructExprs.emplace_back(
            exprConverter_->toVeloxExpr(arg, constructInputType));
      }
    }
  }

  // Get the output names of row construct.
  std::vector<std::string> constructOutNames;
  constructOutNames.reserve(constructExprs.size());
  for (uint32_t colIdx = 0; colIdx < constructExprs.size(); colIdx++) {
    constructOutNames.emplace_back(
        subParser_->makeNodeName(planNodeId_, colIdx));
  }

  uint32_t totalOutColNum = constructExprs.size();
  // Create the row construct node.
  auto constructNode = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(constructOutNames),
      std::move(constructExprs),
      childNode);

  // Create the Aggregation node.
  bool ignoreNullKeys = false;
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(
      totalOutColNum - groupingOutIdx);
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      preGroupingExprs = {};

  // Get the output names of Aggregate node.
  std::vector<std::string> aggOutNames;
  aggOutNames.reserve(totalOutColNum - groupingOutIdx);
  for (uint32_t idx = groupingOutIdx; idx < totalOutColNum; idx++) {
    aggOutNames.emplace_back(subParser_->makeNodeName(planNodeId_, idx));
  }

  // Get the Aggregate expressions.
  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggExprs;
  aggExprs.reserve(totalOutColNum - groupingOutIdx);
  const auto& constructOutType = constructNode->outputType();
  for (uint32_t colIdx = groupingOutIdx; colIdx < totalOutColNum; colIdx++) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> aggArgs;
    aggArgs.reserve(1);
    // Use the colIdx to access the columns after grouping columns.
    aggArgs.emplace_back(std::make_shared<const core::FieldAccessTypedExpr>(
        constructOutType->childAt(colIdx), constructOutType->names()[colIdx]));
    // Use the another index to access the types and names of aggregation
    // columns.
    aggExprs.emplace_back(std::make_shared<const core::CallTypedExpr>(
        aggOutTypes[colIdx - groupingOutIdx],
        std::move(aggArgs),
        aggFuncNames[colIdx - groupingOutIdx]));
  }

  // Get the grouping expressions.
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> groupingExprs;
  groupingExprs.reserve(groupingOutIdx);
  for (uint32_t colIdx = 0; colIdx < groupingOutIdx; colIdx++) {
    // Velox's groupings are limited to be Field.
    groupingExprs.emplace_back(
        std::make_shared<const core::FieldAccessTypedExpr>(
            constructOutType->childAt(colIdx),
            constructOutType->names()[colIdx]));
  }

  // Create the Aggregation node.
  auto aggNode = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      aggStep,
      groupingExprs,
      preGroupingExprs,
      aggOutNames,
      aggExprs,
      aggregateMasks,
      ignoreNullKeys,
      constructNode);
  return aggNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxAgg(
    const ::substrait::AggregateRel& sAgg,
    const std::shared_ptr<const core::PlanNode>& childNode,
    const core::AggregationNode::Step& aggStep) {
  const auto& inputType = childNode->outputType();
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      veloxGroupingExprs;

  // Get the grouping expressions.
  uint32_t groupingOutIdx = 0;
  for (const auto& grouping : sAgg.groupings()) {
    for (const auto& groupingExpr : grouping.grouping_expressions()) {
      // Velox's groupings are limited to be Field.
      veloxGroupingExprs.emplace_back(
          exprConverter_->toVeloxExpr(groupingExpr.selection(), inputType));
      groupingOutIdx += 1;
    }
  }

  // Parse measures and get the aggregate expressions.
  uint32_t aggOutIdx = groupingOutIdx;
  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggExprs;
  aggExprs.reserve(sAgg.measures().size());
  for (const auto& smea : sAgg.measures()) {
    const auto& aggFunction = smea.measure();
    std::string funcName = subParser_->findVeloxFunction(
        functionMap_, aggFunction.function_reference());
    std::vector<std::shared_ptr<const core::ITypedExpr>> aggParams;
    aggParams.reserve(aggFunction.args().size());
    for (const auto& arg : aggFunction.args()) {
      aggParams.emplace_back(exprConverter_->toVeloxExpr(arg, inputType));
    }
    auto aggVeloxType =
        toVeloxType(subParser_->parseType(aggFunction.output_type())->type);
    if (funcName == "avg") {
      // Will used sum and count to calculate the partial avg.
      auto sumExpr = std::make_shared<const core::CallTypedExpr>(
          aggVeloxType, aggParams, "sum");
      auto countExpr = std::make_shared<const core::CallTypedExpr>(
          BIGINT(), aggParams, "count");
      aggExprs.emplace_back(sumExpr);
      aggExprs.emplace_back(countExpr);
      aggOutIdx += 2;
    } else {
      auto aggExpr = std::make_shared<const core::CallTypedExpr>(
          aggVeloxType, std::move(aggParams), funcName);
      aggExprs.emplace_back(aggExpr);
      aggOutIdx += 1;
    }
  }

  bool ignoreNullKeys = false;
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(
      aggOutIdx - groupingOutIdx);
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      preGroupingExprs = {};

  // Get the output names of Aggregation.
  std::vector<std::string> aggOutNames;
  aggOutNames.reserve(aggOutIdx - groupingOutIdx);
  for (int idx = groupingOutIdx; idx < aggOutIdx; idx++) {
    aggOutNames.emplace_back(subParser_->makeNodeName(planNodeId_, idx));
  }

  // Create Aggregate node.
  auto aggNode = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      aggStep,
      veloxGroupingExprs,
      preGroupingExprs,
      aggOutNames,
      aggExprs,
      aggregateMasks,
      ignoreNullKeys,
      childNode);
  return aggNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ProjectRel& sProject) {
  std::shared_ptr<const core::PlanNode> childNode;
  if (sProject.has_input()) {
    childNode = toVeloxPlan(sProject.input());
  } else {
    VELOX_FAIL("Child Rel is expected in ProjectRel.");
  }

  // Construct Velox Expressions.
  const auto& projectExprs = sProject.expressions();
  std::vector<std::string> projectNames;
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  projectNames.reserve(projectExprs.size());
  expressions.reserve(projectExprs.size());

  const auto& inputType = childNode->outputType();
  int colIdx = 0;
  for (const auto& expr : projectExprs) {
    expressions.emplace_back(exprConverter_->toVeloxExpr(expr, inputType));
    projectNames.emplace_back(subParser_->makeNodeName(planNodeId_, colIdx));
    colIdx += 1;
  }

  auto projectNode = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(projectNames),
      std::move(expressions),
      childNode);
  return projectNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::FilterRel& sFilter) {
  std::shared_ptr<const core::PlanNode> childNode;
  if (sFilter.has_input()) {
    childNode = toVeloxPlan(sFilter.input());
  } else {
    VELOX_FAIL("Child Rel is expected in FilterRel.");
  }

  const auto& inputType = childNode->outputType();
  const auto& sExpr = sFilter.condition();

  return std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      exprConverter_->toVeloxExpr(sExpr, inputType),
      childNode);
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& sRead,
    u_int32_t& index,
    std::vector<std::string>& paths,
    std::vector<u_int64_t>& starts,
    std::vector<u_int64_t>& lengths) {
  // Check if the ReadRel specifies an input of stream. If yes, the pre-built
  // input node will be used as the data source.
  auto streamIdx = streamIsInput(sRead);
  if (streamIdx >= 0) {
    if (inputNodesMap_.find(streamIdx) == inputNodesMap_.end()) {
      VELOX_FAIL(
          "Could not find source index {} in input nodes map.", streamIdx);
    }
    return inputNodesMap_[streamIdx];
  }

  // Otherwise, will create TableScan node for ReadRel.
  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> veloxTypeList;
  if (sRead.has_base_schema()) {
    const auto& baseSchema = sRead.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      colNameList.emplace_back(name);
    }
    auto substraitTypeList = subParser_->parseNamedStruct(baseSchema);
    veloxTypeList.reserve(substraitTypeList.size());
    for (const auto& subType : substraitTypeList) {
      veloxTypeList.emplace_back(toVeloxType(subType->type));
    }
  }

  // Parse local files
  if (sRead.has_local_files()) {
    const auto& fileList = sRead.local_files().items();
    paths.reserve(fileList.size());
    starts.reserve(fileList.size());
    lengths.reserve(fileList.size());
    for (const auto& file : fileList) {
      // Expect all Partitions share the same index.
      index = file.partition_index();
      paths.emplace_back(file.uri_file());
      starts.emplace_back(file.start());
      lengths.emplace_back(file.length());
    }
  }

  // Velox requires Filter Pushdown must being enabled.
  bool filterPushdownEnabled = true;
  std::shared_ptr<connector::hive::HiveTableHandle> tableHandle;
  if (!sRead.has_filter()) {
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        "hive_table",
        filterPushdownEnabled,
        connector::hive::SubfieldFilters{},
        nullptr);
  } else {
    connector::hive::SubfieldFilters filters =
        toVeloxFilter(colNameList, veloxTypeList, sRead.filter());
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        "hive_table", filterPushdownEnabled, std::move(filters), nullptr);
  }

  // Get assignments and out names.
  std::vector<std::string> outNames;
  outNames.reserve(colNameList.size());
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = subParser_->makeNodeName(planNodeId_, idx);
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx],
        connector::hive::HiveColumnHandle::ColumnType::kRegular,
        veloxTypeList[idx]);
    outNames.emplace_back(outName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  auto tableScanNode = std::make_shared<core::TableScanNode>(
      nextPlanNodeId(), outputType, tableHandle, assignments);
  return tableScanNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Rel& sRel) {
  if (sRel.has_aggregate()) {
    return toVeloxPlan(sRel.aggregate());
  }
  if (sRel.has_project()) {
    return toVeloxPlan(sRel.project());
  }
  if (sRel.has_filter()) {
    return toVeloxPlan(sRel.filter());
  }
  if (sRel.has_read()) {
    return toVeloxPlan(sRel.read(), partitionIndex_, paths_, starts_, lengths_);
  }
  VELOX_NYI("Substrait conversion not supported for Rel.");
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::RelRoot& sRoot) {
  // TODO: Use the names as the output names for the whole computing.
  const auto& sNames = sRoot.names();
  if (sRoot.has_input()) {
    const auto& sRel = sRoot.input();
    return toVeloxPlan(sRel);
  }
  VELOX_FAIL("Input is expected in RelRoot.");
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& sPlan) {
  // Construct the function map based on the Substrait representation.
  constructFuncMap(sPlan);

  // Create the expression converter.
  exprConverter_ = std::make_shared<SubstraitVeloxExprConverter>(functionMap_);

  // In fact, only one RelRoot or Rel is expected here.
  for (const auto& sRel : sPlan.relations()) {
    if (sRel.has_root()) {
      return toVeloxPlan(sRel.root());
    }
    if (sRel.has_rel()) {
      return toVeloxPlan(sRel.rel());
    }
  }
  VELOX_FAIL("RelRoot or Rel is expected in Plan.");
}

void SubstraitVeloxPlanConverter::constructFuncMap(
    const ::substrait::Plan& sPlan) {
  // Construct the function map based on the Substrait representation.
  for (const auto& sExtension : sPlan.extensions()) {
    if (!sExtension.has_extension_function()) {
      continue;
    }
    const auto& sFmap = sExtension.extension_function();
    auto id = sFmap.function_anchor();
    auto name = sFmap.name();
    functionMap_[id] = name;
  }
}

std::string SubstraitVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

// This class contains the needed infos for Filter Pushdown.
// TODO: Support different types here.
class FilterInfo {
 public:
  // Used to set the left bound.
  void setLeft(double left, bool isExclusive) {
    left_ = left;
    leftExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Used to set the right bound.
  void setRight(double right, bool isExclusive) {
    right_ = right;
    rightExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Will fordis Null value if called once.
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

  // The left bound.
  std::optional<double> left_ = std::nullopt;
  // The right bound.
  std::optional<double> right_ = std::nullopt;
  // The Null allowing.
  bool nullAllowed_ = true;
  // If true, left bound will be exclusive.
  bool leftExclusive_ = false;
  // If true, right bound will be exclusive.
  bool rightExclusive_ = false;

 private:
  bool isInitialized_ = false;
};

connector::hive::SubfieldFilters SubstraitVeloxPlanConverter::toVeloxFilter(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    const ::substrait::Expression& sFilter) {
  connector::hive::SubfieldFilters filters;
  // A map between the column index and the FilterInfo for that column.
  std::unordered_map<int, std::shared_ptr<FilterInfo>> colInfoMap;
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    colInfoMap[idx] = std::make_shared<FilterInfo>();
  }

  std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
  flattenConditions(sFilter, scalarFunctions);
  // Construct the FilterInfo for the related column.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = subParser_->findSubstraitFuncSpec(
        functionMap_, scalarFunction.function_reference());
    auto filterName = subParser_->getSubFunctionName(filterNameSpec);
    int32_t colIdx;
    // TODO: Add different types' support here.
    double val;
    for (auto& param : scalarFunction.args()) {
      auto typeCase = param.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection: {
          auto sel = param.selection();
          // TODO: Only direct reference is considered here.
          auto dRef = sel.direct_reference();
          colIdx = subParser_->parseReferenceSegment(dRef);
          break;
        }
        case ::substrait::Expression::RexTypeCase::kLiteral: {
          auto sLit = param.literal();
          // TODO: Only double is considered here.
          val = sLit.fp64();
          break;
        }
        default:
          VELOX_NYI(
              "Substrait conversion not supported for arg type '{}'", typeCase);
      }
    }
    if (filterName == "is_not_null") {
      colInfoMap[colIdx]->forbidsNull();
    } else if (filterName == "gte") {
      colInfoMap[colIdx]->setLeft(val, false);
    } else if (filterName == "gt") {
      colInfoMap[colIdx]->setLeft(val, true);
    } else if (filterName == "lte") {
      colInfoMap[colIdx]->setRight(val, false);
    } else if (filterName == "lt") {
      colInfoMap[colIdx]->setRight(val, true);
    } else {
      VELOX_NYI(
          "Substrait conversion not supported for filter name '{}'",
          filterName);
    }
  }

  // Construct the Filters.
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    auto filterInfo = colInfoMap[idx];
    // Set the left bound to be negative infinity.
    double leftBound = -1.0 / 0.0;
    // Set the right bound to be positive infinity.
    double rightBound = 1.0 / 0.0;
    bool leftUnbounded = true;
    bool rightUnbounded = true;
    bool leftExclusive = false;
    bool rightExclusive = false;
    if (filterInfo->isInitialized()) {
      if (filterInfo->left_) {
        leftUnbounded = false;
        leftBound = filterInfo->left_.value();
        leftExclusive = filterInfo->leftExclusive_;
      }
      if (filterInfo->right_) {
        rightUnbounded = false;
        rightBound = filterInfo->right_.value();
        rightExclusive = filterInfo->rightExclusive_;
      }
      bool nullAllowed = filterInfo->nullAllowed_;
      filters[common::Subfield(inputNameList[idx])] =
          std::make_unique<common::DoubleRange>(
              leftBound,
              leftUnbounded,
              leftExclusive,
              rightBound,
              rightUnbounded,
              rightExclusive,
              nullAllowed);
    }
  }
  return filters;
}

void SubstraitVeloxPlanConverter::flattenConditions(
    const ::substrait::Expression& sFilter,
    std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions) {
  auto typeCase = sFilter.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sFunc = sFilter.scalar_function();
      auto filterNameSpec = subParser_->findSubstraitFuncSpec(
          functionMap_, sFunc.function_reference());
      // TODO: Only and relation is supported here.
      if (subParser_->getSubFunctionName(filterNameSpec) == "and") {
        for (const auto& sCondition : sFunc.args()) {
          flattenConditions(sCondition, scalarFunctions);
        }
      } else {
        scalarFunctions.emplace_back(sFunc);
      }
      break;
    }
    default:
      VELOX_NYI("GetFlatConditions not supported for type '{}'", typeCase);
  }
}

std::string SubstraitVeloxPlanConverter::findFuncSpec(uint64_t id) {
  return subParser_->findSubstraitFuncSpec(functionMap_, id);
}

bool SubstraitVeloxPlanConverter::needsRowConstruct(
    const ::substrait::AggregateRel& sAgg,
    core::AggregationNode::Step& aggStep) {
  if (sAgg.measures().size() == 0) {
    // When only groupings exist, set the phase to be Single.
    aggStep = core::AggregationNode::Step::kSingle;
    return false;
  }
  for (const auto& smea : sAgg.measures()) {
    auto aggFunction = smea.measure();
    std::string funcName = subParser_->findVeloxFunction(
        functionMap_, aggFunction.function_reference());
    // Set the aggregation phase.
    switch (aggFunction.phase()) {
      case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
        aggStep = core::AggregationNode::Step::kPartial;
        break;
      case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
        aggStep = core::AggregationNode::Step::kIntermediate;
        break;
      case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
        aggStep = core::AggregationNode::Step::kFinal;
        // Only Final Average needs row construct currently.
        if (funcName == "avg") {
          return true;
        }
        break;
      default:
        throw std::runtime_error("Aggregate phase is not supported.");
    }
  }
  return false;
}

int32_t SubstraitVeloxPlanConverter::streamIsInput(
    const ::substrait::ReadRel& sRead) {
  if (sRead.has_local_files()) {
    const auto& fileList = sRead.local_files().items();
    if (fileList.size() == 0) {
      VELOX_FAIL("At least one file path is expected.");
    }

    // The stream input will be specified with the format of
    // "iterator:${index}".
    std::string filePath = fileList[0].uri_file();
    std::string prefix = "iterator:";
    std::size_t pos = filePath.find(prefix);
    if (pos == std::string::npos) {
      return -1;
    }

    // Get the index.
    std::string idxStr = filePath.substr(pos + prefix.size(), filePath.size());
    try {
      return stoi(idxStr);
    } catch (const std::exception& err) {
      VELOX_FAIL(err.what());
    }
  }
  VELOX_FAIL("Local file is expected.");
}

} // namespace facebook::velox::substrait
