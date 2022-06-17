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

namespace facebook::velox::substrait {

namespace {
template <typename T>
// Get the lowest value for numeric type.
T getLowest() {
  return std::numeric_limits<T>::lowest();
};

// Get the lowest value for string.
template <>
std::string getLowest<std::string>() {
  return "";
};

// Get the max value for numeric type.
template <typename T>
T getMax() {
  return std::numeric_limits<T>::max();
};

// The max value will be used in BytesRange. Return empty string here instead.
template <>
std::string getMax<std::string>() {
  return "";
};
} // namespace

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::JoinRel& sJoin) {
  if (!sJoin.has_left()) {
    VELOX_FAIL("Left Rel is expected in JoinRel.");
  }
  if (!sJoin.has_right()) {
    VELOX_FAIL("Right Rel is expected in JoinRel.");
  }

  auto leftNode = toVeloxPlan(sJoin.left());
  auto rightNode = toVeloxPlan(sJoin.right());

  auto outputSize =
      leftNode->outputType()->size() + rightNode->outputType()->size();
  std::vector<std::string> outputNames;
  std::vector<std::shared_ptr<const Type>> outputTypes;
  outputNames.reserve(outputSize);
  outputTypes.reserve(outputSize);
  for (const auto& node : {leftNode, rightNode}) {
    const auto& names = node->outputType()->names();
    outputNames.insert(outputNames.end(), names.begin(), names.end());
    const auto& types = node->outputType()->children();
    outputTypes.insert(outputTypes.end(), types.begin(), types.end());
  }
  auto outputRowType = std::make_shared<const RowType>(
      std::move(outputNames), std::move(outputTypes));

  // extract join keys from join expression
  std::vector<const ::substrait::Expression::FieldReference*> leftExprs,
      rightExprs;
  extractJoinKeys(sJoin.expression(), leftExprs, rightExprs);
  VELOX_CHECK_EQ(leftExprs.size(), rightExprs.size());
  size_t numKeys = leftExprs.size();

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> leftKeys,
      rightKeys;
  leftKeys.reserve(numKeys);
  rightKeys.reserve(numKeys);
  for (size_t i = 0; i < numKeys; ++i) {
    leftKeys.emplace_back(
        exprConverter_->toVeloxExpr(*leftExprs[i], outputRowType));
    rightKeys.emplace_back(
        exprConverter_->toVeloxExpr(*rightExprs[i], outputRowType));
  }

  std::shared_ptr<const core::ITypedExpr> filter;
  if (sJoin.has_post_join_filter()) {
    filter =
        exprConverter_->toVeloxExpr(sJoin.post_join_filter(), outputRowType);
  }

  // Map join type
  core::JoinType joinType;
  switch (sJoin.type()) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      joinType = core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      joinType = core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      joinType = core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      joinType = core::JoinType::kRight;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI:
      joinType = core::JoinType::kSemi;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_ANTI:
      joinType = core::JoinType::kAnti;
      break;
    default:
      VELOX_NYI("Unsupported Join type: {}", sJoin.type());
  }

  // Create join node
  return std::make_shared<core::HashJoinNode>(
      nextPlanNodeId(),
      joinType,
      leftKeys,
      rightKeys,
      filter,
      leftNode,
      rightNode,
      outputRowType);
}

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
    std::shared_ptr<SplitInfo>& splitInfo) {
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
    splitInfo->paths.reserve(fileList.size());
    splitInfo->starts.reserve(fileList.size());
    splitInfo->lengths.reserve(fileList.size());
    for (const auto& file : fileList) {
      // Expect all Partitions share the same index.
      splitInfo->partitionIndex = file.partition_index();
      splitInfo->paths.emplace_back(file.uri_file());
      splitInfo->starts.emplace_back(file.start());
      splitInfo->lengths.emplace_back(file.length());
      auto format = file.format();
      if (format == 2 || format == 3) {
        splitInfo->format = dwio::common::FileFormat::ORC;
      } else if (format == 1) {
        splitInfo->format = dwio::common::FileFormat::PARQUET;
      } else {
        splitInfo->format = dwio::common::FileFormat::UNKNOWN;
      }
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
    // Flatten the conditions connected with 'and'.
    std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
    flattenConditions(sRead.filter(), scalarFunctions);

    // Separate the filters to be two parts. The first part can be pushed down.
    std::vector<::substrait::Expression_ScalarFunction> subfieldFunctions;
    std::vector<::substrait::Expression_ScalarFunction> remainingFunctions;
    separateFilters(scalarFunctions, subfieldFunctions, remainingFunctions);

    // Create the filters to be pushed down.
    connector::hive::SubfieldFilters subfieldFilters =
        toSubfieldFilters(colNameList, veloxTypeList, subfieldFunctions);

    // Connect the remaining filters with 'and'.
    std::shared_ptr<const core::ITypedExpr> remainingFilter =
        connectWithAnd(colNameList, veloxTypeList, remainingFunctions);

    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        "hive_table",
        filterPushdownEnabled,
        std::move(subfieldFilters),
        remainingFilter);
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
  if (sRel.has_join()) {
    return toVeloxPlan(sRel.join());
  }
  if (sRel.has_read()) {
    auto splitInfo = std::make_shared<SplitInfo>();

    auto planNode = toVeloxPlan(sRel.read(), splitInfo);
    splitInfoMap_[planNode->id()] = splitInfo;
    return planNode;
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
  // Construct the function map based on the Substrait representation,
  // and initialize the expression converter with it.
  constructFuncMap(sPlan);

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
  exprConverter_ =
      std::make_shared<SubstraitVeloxExprConverter>(pool_, functionMap_);
}

std::string SubstraitVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
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
  if (validationMode_) {
    return -1;
  }
  VELOX_FAIL("Local file is expected.");
}

void SubstraitVeloxPlanConverter::extractJoinKeys(
    const ::substrait::Expression& joinExpression,
    std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
    std::vector<const ::substrait::Expression::FieldReference*>& rightExprs) {
  std::vector<const ::substrait::Expression*> expressions;
  expressions.push_back(&joinExpression);
  while (!expressions.empty()) {
    auto visited = expressions.back();
    expressions.pop_back();
    if (visited->rex_type_case() ==
        ::substrait::Expression::RexTypeCase::kScalarFunction) {
      const auto& funcName =
          subParser_->getSubFunctionName(subParser_->findVeloxFunction(
              functionMap_, visited->scalar_function().function_reference()));
      const auto& args = visited->scalar_function().args();
      if (funcName == "and") {
        expressions.push_back(&args[0]);
        expressions.push_back(&args[1]);
      } else if (funcName == "eq") {
        VELOX_CHECK(std::all_of(
            args.cbegin(), args.cend(), [](const ::substrait::Expression& arg) {
              return arg.has_selection();
            }));
        leftExprs.push_back(&args[0].selection());
        rightExprs.push_back(&args[1].selection());
      } else {
        VELOX_NYI("Join condition {} not supported.", funcName);
      }
    } else {
      VELOX_FAIL(
          "Unable to parse from join expression: {}",
          joinExpression.DebugString());
    }
  }
}

connector::hive::SubfieldFilters SubstraitVeloxPlanConverter::toSubfieldFilters(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    const std::vector<::substrait::Expression_ScalarFunction>&
        scalarFunctions) {
  // A map between the column index and the FilterInfo.
  std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>> colInfoMap;
  for (uint32_t idx = 0; idx < inputTypeList.size(); idx++) {
    colInfoMap[idx] = std::make_shared<FilterInfo>();
  }

  // Construct the FilterInfo for the related column.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = subParser_->findSubstraitFuncSpec(
        functionMap_, scalarFunction.function_reference());
    auto filterName = subParser_->getSubFunctionName(filterNameSpec);
    if (filterName == "not") {
      VELOX_CHECK(scalarFunction.args().size() == 1);
      VELOX_CHECK(
          scalarFunction.args()[0].has_scalar_function(),
          "Scalar function expected.");
      // Set its chid to filter info with reverse enabled.
      setFilterMap(
          scalarFunction.args()[0].scalar_function(),
          inputTypeList,
          colInfoMap,
          true);
      continue;
    }

    if (filterName == "or") {
      VELOX_CHECK(scalarFunction.args().size() == 2);
      VELOX_CHECK(std::all_of(
          scalarFunction.args().cbegin(),
          scalarFunction.args().cend(),
          [](const ::substrait::Expression& arg) {
            return arg.has_scalar_function();
          }));
      // Set the chidren functions to filter info. They should be
      // effective to the same field.
      for (const auto& arg : scalarFunction.args()) {
        setFilterMap(arg.scalar_function(), inputTypeList, colInfoMap);
      }
      continue;
    }

    setFilterMap(scalarFunction, inputTypeList, colInfoMap);
  }

  // Create subfield filters based on the constructed filter info map.
  return mapToFilters(inputNameList, inputTypeList, colInfoMap);
}

bool SubstraitVeloxPlanConverter::fieldOrWithLiteral(
    const ::substrait::Expression_ScalarFunction& function) {
  if (function.args().size() == 1) {
    if (function.args()[0].has_selection()) {
      // Only field exists.
      return true;
    } else {
      return false;
    }
  }

  if (function.args().size() != 2) {
    return false;
  }
  bool fieldExists = false;
  bool literalExists = false;
  for (const auto& param : function.args()) {
    auto typeCase = param.rex_type_case();
    switch (typeCase) {
      case ::substrait::Expression::RexTypeCase::kSelection:
        fieldExists = true;
        break;
      case ::substrait::Expression::RexTypeCase::kLiteral:
        literalExists = true;
        break;
      default:
        break;
    }
  }
  // Whether the field and literal both exist.
  return fieldExists && literalExists;
}

bool SubstraitVeloxPlanConverter::chidrenFunctionsOnSameField(
    const ::substrait::Expression_ScalarFunction& function) {
  // Get the column indices of the chidren functions.
  std::vector<int32_t> colIndices;
  for (const auto& arg : function.args()) {
    if (!arg.has_scalar_function()) {
      return false;
    }
    auto scalarFunction = arg.scalar_function();
    for (const auto& param : scalarFunction.args()) {
      if (param.has_selection()) {
        auto field = param.selection();
        VELOX_CHECK(field.has_direct_reference());
        int32_t colIdx =
            subParser_->parseReferenceSegment(field.direct_reference());
        colIndices.emplace_back(colIdx);
      }
    }
  }

  if (std::all_of(colIndices.begin(), colIndices.end(), [&](uint32_t idx) {
        return idx == colIndices[0];
      })) {
    // All indices are the same.
    return true;
  }
  return false;
}

void SubstraitVeloxPlanConverter::separateFilters(
    const std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
    std::vector<::substrait::Expression_ScalarFunction>& subfieldFunctions,
    std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions) {
  // Condtions can be pushed down.
  std::unordered_set<std::string> supportedFunctions = {
      "is_not_null", "gte", "gt", "lte", "lt", "equal", "in"};
  // Used to record the columns indices for not(equal) conditions.
  std::unordered_set<uint32_t> notEqualCols;

  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = subParser_->findSubstraitFuncSpec(
        functionMap_, scalarFunction.function_reference());
    auto filterName = subParser_->getSubFunctionName(filterNameSpec);
    if (filterName != "not" && filterName != "or") {
      // Check if the condition is supported to be pushed down.
      // The arg should be field or field with literal.
      if (supportedFunctions.find(filterName) != supportedFunctions.end() &&
          fieldOrWithLiteral(scalarFunction)) {
        subfieldFunctions.emplace_back(scalarFunction);
      } else {
        remainingFunctions.emplace_back(scalarFunction);
      }
      continue;
    }

    // When the function is not/or, check whether its chidren can be pushed
    // down.
    // OR Conditon whose chidren functions are on different columns is not
    // supported to be pushed down.
    if (filterName == "or" && !chidrenFunctionsOnSameField(scalarFunction)) {
      remainingFunctions.emplace_back(scalarFunction);
      continue;
    }

    // Check whether the chidren functions are supported to be
    // pushed down. If yes, this scalar function will be added
    // into the subfield functions.
    bool supported = true;
    for (const auto& arg : scalarFunction.args()) {
      if (!arg.has_scalar_function()) {
        // Not with a Boolean Literal is not supported curretly.
        // It can be pushed down with an AlwaysTrue or AlwaysFalse Range.
        supported = false;
        break;
      }

      auto nameSpec = subParser_->findSubstraitFuncSpec(
          functionMap_, arg.scalar_function().function_reference());
      auto functionName = subParser_->getSubFunctionName(nameSpec);

      // The arg should be field or field with literal.
      if (supportedFunctions.find(functionName) == supportedFunctions.end() ||
          !fieldOrWithLiteral(arg.scalar_function())) {
        supported = false;
        break;
      }

      // Mutiple not(equal) conditons cannot be pushed down because
      // the multiple range is in OR relation while AND relation is
      // actually needed.
      if (filterName == "not" && functionName == "equal") {
        for (const auto& eqArg : arg.scalar_function().args()) {
          if (!eqArg.has_selection()) {
            continue;
          }
          uint32_t colIdx = subParser_->parseReferenceSegment(
              eqArg.selection().direct_reference());
          // If one not(equal) condition for this column already exists,
          // this function cannot be pushed down then.
          if (notEqualCols.find(colIdx) == notEqualCols.end()) {
            notEqualCols.insert(colIdx);
          } else {
            supported = false;
            break;
          }
        }
        if (!supported) {
          break;
        }
      }
    }
    if (supported) {
      subfieldFunctions.emplace_back(scalarFunction);
    } else {
      remainingFunctions.emplace_back(scalarFunction);
    }
  }
}

void SubstraitVeloxPlanConverter::setInValues(
    const ::substrait::Expression_ScalarFunction& scalarFunction,
    std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap) {
  VELOX_CHECK(
      scalarFunction.args().size() == 2, "Two args expected in In expression.");
  VELOX_CHECK(scalarFunction.args()[0].has_selection(), "Field expected.");

  // Get the column index.
  uint32_t colIdx = subParser_->parseReferenceSegment(
      scalarFunction.args()[0].selection().direct_reference());
  VELOX_CHECK(scalarFunction.args()[1].has_literal(), "Literal expected.");
  VELOX_CHECK(scalarFunction.args()[1].literal().has_list(), "List expected.");

  // Get the value list.
  std::vector<variant> variants;
  auto valueList = scalarFunction.args()[1].literal().list();
  variants.reserve(valueList.values().size());
  for (const auto& literal : valueList.values()) {
    variants.emplace_back(
        exprConverter_->toTypedVariant(literal)->veloxVariant);
  }

  // Set the value list to filter info.
  colInfoMap[colIdx]->setValues(variants);
}

template <typename T>
void SubstraitVeloxPlanConverter::setColInfoMap(
    const std::string& filterName,
    uint32_t colIdx,
    std::optional<variant> literalVariant,
    bool reverse,
    std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap) {
  if (filterName == "is_not_null") {
    if (reverse) {
      VELOX_NYI("Reverse not supported for filter name '{}'", filterName);
    }
    colInfoMap[colIdx]->forbidsNull();
    return;
  }

  if (filterName == "gte") {
    if (reverse) {
      colInfoMap[colIdx]->setUpper(literalVariant, true);
    } else {
      colInfoMap[colIdx]->setLower(literalVariant, false);
    }
    return;
  }

  if (filterName == "gt") {
    if (reverse) {
      colInfoMap[colIdx]->setUpper(literalVariant, false);
    } else {
      colInfoMap[colIdx]->setLower(literalVariant, true);
    }
    return;
  }

  if (filterName == "lte") {
    if (reverse) {
      colInfoMap[colIdx]->setLower(literalVariant, true);
    } else {
      colInfoMap[colIdx]->setUpper(literalVariant, false);
    }
    return;
  }

  if (filterName == "lt") {
    if (reverse) {
      colInfoMap[colIdx]->setLower(literalVariant, false);
    } else {
      colInfoMap[colIdx]->setUpper(literalVariant, true);
    }
    return;
  }

  if (filterName == "equal") {
    if (reverse) {
      colInfoMap[colIdx]->setNotValue(literalVariant);
    } else {
      colInfoMap[colIdx]->setLower(literalVariant, false);
      colInfoMap[colIdx]->setUpper(literalVariant, false);
    }
    return;
  }
  VELOX_NYI("SetColInfoMap not supported for filter name '{}'", filterName);
}

void SubstraitVeloxPlanConverter::setFilterMap(
    const ::substrait::Expression_ScalarFunction& scalarFunction,
    const std::vector<TypePtr>& inputTypeList,
    std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>>& colInfoMap,
    bool reverse) {
  auto nameSpec = subParser_->findSubstraitFuncSpec(
      functionMap_, scalarFunction.function_reference());
  auto functionName = subParser_->getSubFunctionName(nameSpec);

  // Extract the in args and set to filter info.
  if (functionName == "in") {
    setInValues(scalarFunction, colInfoMap);
    return;
  }

  // Extract the column index and column bound from the scalar function.
  std::optional<uint32_t> colIdx;
  std::optional<::substrait::Expression_Literal> substraitLit;
  for (const auto& param : scalarFunction.args()) {
    auto typeCase = param.rex_type_case();
    switch (typeCase) {
      case ::substrait::Expression::RexTypeCase::kSelection:
        colIdx = subParser_->parseReferenceSegment(
            param.selection().direct_reference());
        break;
      case ::substrait::Expression::RexTypeCase::kLiteral:
        substraitLit = param.literal();
        break;
      default:
        VELOX_NYI(
            "Substrait conversion not supported for arg type '{}'", typeCase);
    }
  }
  if (!colIdx.has_value()) {
    VELOX_NYI("Column index is expected in subfield filters creation.");
  }

  // Set the extracted bound to the specific column.
  uint32_t colIdxVal = colIdx.value();
  auto inputType = inputTypeList[colIdxVal];
  std::optional<variant> val;
  switch (inputType->kind()) {
    case TypeKind::INTEGER:
      if (substraitLit) {
        val = variant(substraitLit.value().i32());
      }
      setColInfoMap<int>(functionName, colIdxVal, val, reverse, colInfoMap);
      break;
    case TypeKind::BIGINT:
      if (substraitLit) {
        val = variant(substraitLit.value().i64());
      }
      setColInfoMap<int64_t>(functionName, colIdxVal, val, reverse, colInfoMap);
      break;
    case TypeKind::DOUBLE:
      if (substraitLit) {
        val = variant(substraitLit.value().fp64());
      }
      setColInfoMap<double>(functionName, colIdxVal, val, reverse, colInfoMap);
      break;
    case TypeKind::VARCHAR:
      if (substraitLit) {
        val = variant(substraitLit.value().string());
      }
      setColInfoMap<std::string>(
          functionName, colIdxVal, val, reverse, colInfoMap);
      break;
    default:
      VELOX_NYI(
          "Subfield filters creation not supported for input type '{}'",
          inputType);
  }
}

template <TypeKind KIND, typename FilterType>
void SubstraitVeloxPlanConverter::createNotEqualFilter(
    variant notVariant,
    bool nullAllowed,
    std::vector<std::unique_ptr<FilterType>>& colFilters) {
  using NativeType = typename RangeTraits<KIND>::NativeType;
  using RangeType = typename RangeTraits<KIND>::RangeType;

  // Value > lower
  std::unique_ptr<FilterType> lowerFilter = std::make_unique<RangeType>(
      notVariant.value<NativeType>(), /*lower*/
      false, /*lowerUnbounded*/
      true, /*lowerExclusive*/
      getMax<NativeType>(), /*upper*/
      true, /*upperUnbounded*/
      false, /*upperExclusive*/
      nullAllowed); /*nullAllowed*/
  colFilters.emplace_back(std::move(lowerFilter));

  // Value < upper
  std::unique_ptr<FilterType> upperFilter = std::make_unique<RangeType>(
      getLowest<NativeType>(), /*lower*/
      true, /*lowerUnbounded*/
      false, /*lowerExclusive*/
      notVariant.value<NativeType>(), /*upper*/
      false, /*upperUnbounded*/
      true, /*upperExclusive*/
      nullAllowed); /*nullAllowed*/
  colFilters.emplace_back(std::move(upperFilter));
}

template <TypeKind KIND>
void SubstraitVeloxPlanConverter::setInFilter(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {}

template <>
void SubstraitVeloxPlanConverter::setInFilter<TypeKind::DOUBLE>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  std::vector<double> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    double value = variant.value<double>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] =
      common::createDoubleValues(values, nullAllowed);
}

template <>
void SubstraitVeloxPlanConverter::setInFilter<TypeKind::BIGINT>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  std::vector<int64_t> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    int64_t value = variant.value<int64_t>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] =
      common::createBigintValues(values, nullAllowed);
}

template <>
void SubstraitVeloxPlanConverter::setInFilter<TypeKind::INTEGER>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  // Use bigint values for int type.
  std::vector<int64_t> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    // Use the matched type to get value from variant.
    int64_t value = variant.value<int32_t>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] =
      common::createBigintValues(values, nullAllowed);
}

template <>
void SubstraitVeloxPlanConverter::setInFilter<TypeKind::VARCHAR>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  std::vector<std::string> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    std::string value = variant.value<std::string>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] =
      std::make_unique<common::BytesValues>(values, nullAllowed);
}

template <TypeKind KIND, typename FilterType>
void SubstraitVeloxPlanConverter::setSubfieldFilter(
    std::vector<std::unique_ptr<FilterType>> colFilters,
    const std::string& inputName,
    bool nullAllowed,
    connector::hive::SubfieldFilters& filters) {
  using MultiRangeType = typename RangeTraits<KIND>::MultiRangeType;

  if (colFilters.size() == 1) {
    filters[common::Subfield(inputName)] = std::move(colFilters[0]);
  } else if (colFilters.size() > 1) {
    filters[common::Subfield(inputName)] =
        std::make_unique<MultiRangeType>(std::move(colFilters), nullAllowed);
  }
}

template <TypeKind KIND, typename FilterType>
void SubstraitVeloxPlanConverter::constructSubfieldFilters(
    uint32_t colIdx,
    const std::string& inputName,
    const std::shared_ptr<FilterInfo>& filterInfo,
    connector::hive::SubfieldFilters& filters) {
  using NativeType = typename RangeTraits<KIND>::NativeType;
  using RangeType = typename RangeTraits<KIND>::RangeType;
  using MultiRangeType = typename RangeTraits<KIND>::MultiRangeType;

  // Handle 'in' filter.
  if (filterInfo->valuesVector_.size() > 0) {
    setInFilter<KIND>(
        filterInfo->valuesVector_,
        filterInfo->nullAllowed_,
        inputName,
        filters);
    // Currently, In cannot coexist with other filter conditions
    // due to multirange is in 'OR' relation but 'AND' can be needed.
    VELOX_CHECK(
        filterInfo->lowerBounds_.size() == 0,
        "Other conditons cannot be supported.");
    VELOX_CHECK(
        filterInfo->upperBounds_.size() == 0,
        "Other conditons cannot be supported.");
    return;
  }

  // Construct the Filters.
  std::vector<std::unique_ptr<FilterType>> colFilters;
  NativeType lowerBound = getLowest<NativeType>();
  NativeType upperBound = getMax<NativeType>();
  bool lowerUnbounded = true;
  bool upperUnbounded = true;
  bool lowerExclusive = false;
  bool upperExclusive = false;
  if (filterInfo->isInitialized()) {
    // Handle not(equal) filter.
    if (filterInfo->notValue_) {
      variant notVariant = filterInfo->notValue_.value();
      createNotEqualFilter<KIND, FilterType>(
          notVariant, filterInfo->nullAllowed_, colFilters);
    }

    // Handle other filter ranges.
    uint32_t rangeSize = std::max(
        filterInfo->lowerBounds_.size(), filterInfo->upperBounds_.size());
    bool nullAllowed = filterInfo->nullAllowed_;
    for (uint32_t idx = 0; idx < rangeSize; idx++) {
      if (idx < filterInfo->lowerBounds_.size() &&
          filterInfo->lowerBounds_[idx]) {
        lowerUnbounded = false;
        variant lowerVariant = filterInfo->lowerBounds_[idx].value();
        lowerBound = lowerVariant.value<NativeType>();
        lowerExclusive = filterInfo->lowerExclusives_[idx];
      }
      if (idx < filterInfo->upperBounds_.size() &&
          filterInfo->upperBounds_[idx]) {
        upperUnbounded = false;
        variant upperVariant = filterInfo->upperBounds_[idx].value();
        upperBound = upperVariant.value<NativeType>();
        upperExclusive = filterInfo->upperExclusives_[idx];
      }
      std::unique_ptr<FilterType> filter = std::make_unique<RangeType>(
          lowerBound,
          lowerUnbounded,
          lowerExclusive,
          upperBound,
          upperUnbounded,
          upperExclusive,
          nullAllowed);
      colFilters.emplace_back(std::move(filter));
    }
  }
  // Set the SubfieldFilter.
  setSubfieldFilter<KIND, FilterType>(
      std::move(colFilters), inputName, filterInfo->nullAllowed_, filters);
}

connector::hive::SubfieldFilters SubstraitVeloxPlanConverter::mapToFilters(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    std::unordered_map<uint32_t, std::shared_ptr<FilterInfo>> colInfoMap) {
  // Construct the subfield filters based on the filter info map.
  connector::hive::SubfieldFilters filters;
  for (uint32_t colIdx = 0; colIdx < inputNameList.size(); colIdx++) {
    auto inputType = inputTypeList[colIdx];
    switch (inputType->kind()) {
      case TypeKind::INTEGER:
        constructSubfieldFilters<TypeKind::INTEGER, common::BigintRange>(
            colIdx, inputNameList[colIdx], colInfoMap[colIdx], filters);
        break;
      case TypeKind::BIGINT:
        constructSubfieldFilters<TypeKind::BIGINT, common::BigintRange>(
            colIdx, inputNameList[colIdx], colInfoMap[colIdx], filters);
        break;
      case TypeKind::DOUBLE:
        constructSubfieldFilters<TypeKind::DOUBLE, common::Filter>(
            colIdx, inputNameList[colIdx], colInfoMap[colIdx], filters);
        break;
      case TypeKind::VARCHAR:
        constructSubfieldFilters<TypeKind::VARCHAR, common::Filter>(
            colIdx, inputNameList[colIdx], colInfoMap[colIdx], filters);
        break;
      default:
        VELOX_NYI(
            "Subfield filters creation not supported for input type '{}'",
            inputType);
    }
  }
  return filters;
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxPlanConverter::connectWithAnd(
    std::vector<std::string> inputNameList,
    std::vector<TypePtr> inputTypeList,
    const std::vector<::substrait::Expression_ScalarFunction>&
        remainingFunctions) {
  if (remainingFunctions.size() == 0) {
    return nullptr;
  }
  auto inputType = ROW(std::move(inputNameList), std::move(inputTypeList));
  std::shared_ptr<const core::ITypedExpr> remainingFilter =
      exprConverter_->toVeloxExpr(remainingFunctions[0], inputType);
  if (remainingFunctions.size() == 1) {
    return remainingFilter;
  }
  // Will connect multiple functions with AND.
  uint32_t idx = 1;
  while (idx < remainingFunctions.size()) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> params;
    params.reserve(2);
    params.emplace_back(remainingFilter);
    params.emplace_back(
        exprConverter_->toVeloxExpr(remainingFunctions[idx], inputType));
    remainingFilter = std::make_shared<const core::CallTypedExpr>(
        BOOLEAN(), std::move(params), "and");
    idx += 1;
  }
  return remainingFilter;
}

} // namespace facebook::velox::substrait
