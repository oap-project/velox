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

#include "velox/parse/TypeResolver.h"
#include "velox/core/ITypedExpr.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/parse/Expressions.h"
#include "velox/type/Type.h"

namespace facebook::velox::parse {
namespace {

std::string toString(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  std::ostringstream signature;
  signature << functionName << "(";
  for (auto i = 0; i < argTypes.size(); i++) {
    if (i > 0) {
      signature << ", ";
    }
    signature << argTypes[i]->toString();
  }
  signature << ")";
  return signature.str();
}

std::string toString(
    const std::vector<const exec::FunctionSignature*>& signatures) {
  std::stringstream out;
  for (auto i = 0; i < signatures.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << signatures[i]->toString();
  }
  return out.str();
}

std::pair<uint8_t, uint8_t> computeRoundDecimalResultPrecisionScale(
    uint8_t aPrecision,
    uint8_t aScale,
    int32_t scale) {
  auto integralLeastNumDigits = aPrecision - aScale + 1;
  if (scale < 0) {
    auto newPrecision = std::max(integralLeastNumDigits, -scale + 1);
    return {std::min(newPrecision, 38), 0};
  } else {
    return {
        std::min(
            static_cast<int32_t>(
                integralLeastNumDigits +
                std::min(static_cast<int32_t>(aScale), scale)),
            38),
        std::min(static_cast<int32_t>(aScale), scale)};
  }
}

int32_t getScaleFromConstantType(
    const core::ConstantTypedExpr* typeExpr,
    memory::MemoryPool* pool) {
  int32_t scale;
  if (typeExpr->type()->kind() == TypeKind::BIGINT) {
    return typeExpr->toConstantVector(pool)
        ->asUnchecked<ConstantVector<int64_t>>()
        ->value();
  } else if (typeExpr->type()->kind() == TypeKind::INTEGER) {
    return typeExpr->toConstantVector(pool)
        ->asUnchecked<ConstantVector<int32_t>>()
        ->value();
  } else {
    VELOX_UNSUPPORTED(
        "Second argument of decimal_round should be cast bigint or integer value as integer");
  }
}

int32_t getRoundDecimalScale(core::TypedExprPtr input) {
  std::shared_ptr<memory::MemoryPool> pool = memory::addDefaultLeafMemoryPool();
  if (auto cast = dynamic_cast<const core::CastTypedExpr*>(input.get())) {
    VELOX_USER_CHECK(
        input->type()->kind() == TypeKind::INTEGER,
        "Second argument of decimal_round should be cast value as integer");
    auto scaleInput = cast->inputs()[0];
    auto constant =
        dynamic_cast<const core::ConstantTypedExpr*>(scaleInput.get());
    VELOX_USER_CHECK_NOT_NULL(constant);
    return getScaleFromConstantType(constant, pool.get());
  } else if (
      auto constant =
          dynamic_cast<const core::ConstantTypedExpr*>(input.get())) {
    VELOX_USER_CHECK(
        constant->type()->kind() == TypeKind::INTEGER,
        "Second argument of decimal_round should be integer");
    return constant->toConstantVector(pool.get())
        ->asUnchecked<ConstantVector<int32_t>>()
        ->value();
  } else {
    VELOX_USER_FAIL(
        "Second argument of decimal_round should be cast value as integer or integer constant");
  }
}

TypePtr resolveType(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs,
    const std::shared_ptr<const core::CallExpr>& expr,
    bool nullOnFailure) {
  // TODO Replace with struct_pack
  if (expr->getFunctionName() == "row_constructor") {
    auto numInput = inputs.size();
    std::vector<TypePtr> types(numInput);
    std::vector<std::string> names(numInput);
    for (auto i = 0; i < numInput; i++) {
      types[i] = inputs[i]->type();
      names[i] = fmt::format("c{}", i + 1);
    }
    return ROW(std::move(names), std::move(types));
  }

  if (expr->getFunctionName() == "decimal_round") {
    auto numInput = inputs.size();
    int32_t scale = 0;
    if (numInput > 1) {
      scale = getRoundDecimalScale(inputs[1]);
    }
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*inputs[0]->type());
    auto [rPrecision, rScale] =
        computeRoundDecimalResultPrecisionScale(aPrecision, aScale, scale);
    return DECIMAL(rPrecision, rScale);
  }

  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (auto& input : inputs) {
    inputTypes.emplace_back(input->type());
  }

  if (auto resolvedType = exec::resolveTypeForSpecialForm(
          expr->getFunctionName(), inputTypes)) {
    return resolvedType;
  }

  return resolveScalarFunctionType(
      expr->getFunctionName(), inputTypes, nullOnFailure);
}

} // namespace

void registerTypeResolver() {
  core::Expressions::setTypeResolverHook(&resolveType);
}

TypePtr resolveScalarFunctionType(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    bool nullOnFailure) {
  auto returnType = resolveFunction(name, argTypes);
  if (returnType) {
    return returnType;
  }

  if (nullOnFailure) {
    return nullptr;
  }

  auto allSignatures = getFunctionSignatures();
  auto it = allSignatures.find(name);
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL("Scalar function doesn't exist: {}.", name);
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Scalar function signature is not supported: {}. Supported signatures: {}.",
        toString(name, argTypes),
        toString(functionSignatures));
  }
}
} // namespace facebook::velox::parse
