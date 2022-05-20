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

#include "velox/substrait/SubstraitToVeloxExpr.h"
#include "velox/substrait/TypeUtils.h"

namespace facebook::velox::substrait {

template <TypeKind KIND>
VectorPtr SubstraitVeloxExprConverter::setVectorFromVariantsByKind(
    const std::vector<velox::variant>& value,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(
      BaseVector::create(CppToType<T>::create(), value.size(), pool));

  for (vector_size_t i = 0; i < value.size(); i++) {
    if (value[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, value[i].value<T>());
    }
  }
  return flatVector;
}

template <>
VectorPtr
SubstraitVeloxExprConverter::setVectorFromVariantsByKind<TypeKind::VARBINARY>(
    const std::vector<velox::variant>& value,
    memory::MemoryPool* pool) {
  throw std::invalid_argument("Return of VARBINARY data is not supported");
}

template <>
VectorPtr
SubstraitVeloxExprConverter::setVectorFromVariantsByKind<TypeKind::VARCHAR>(
    const std::vector<velox::variant>& value,
    memory::MemoryPool* pool) {
  auto flatVector = std::dynamic_pointer_cast<FlatVector<StringView>>(
      BaseVector::create(VARCHAR(), value.size(), pool));

  for (vector_size_t i = 0; i < value.size(); i++) {
    if (value[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, StringView(value[i].value<Varchar>()));
    }
  }
  return flatVector;
}

VectorPtr SubstraitVeloxExprConverter::setVectorFromVariants(
    const TypePtr& type,
    const std::vector<velox::variant>& value,
    velox::memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setVectorFromVariantsByKind, type->kind(), value, pool);
}

ArrayVectorPtr SubstraitVeloxExprConverter::toArrayVector(
    TypePtr type,
    VectorPtr vector,
    memory::MemoryPool* pool) {
  vector_size_t size = 1;
  BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool);
  BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool);
  BufferPtr nulls = AlignedBuffer::allocate<uint64_t>(size, pool);

  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();
  auto rawNulls = nulls->asMutable<uint64_t>();

  bits::fillBits(rawNulls, 0, size, pool);
  vector_size_t nullCount = 0;

  *rawSizes++ = vector->size();
  *rawOffsets++ = 0;

  return std::make_shared<ArrayVector>(
      pool, ARRAY(type), nulls, size, offsets, sizes, vector, nullCount);
}

std::shared_ptr<const core::FieldAccessTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::FieldReference& sField,
    const RowTypePtr& inputType) {
  auto typeCase = sField.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::FieldReference::ReferenceTypeCase::
        kDirectReference: {
      const auto& dRef = sField.direct_reference();
      int32_t colIdx = subParser_->parseReferenceSegment(dRef);

      const auto& inputTypes = inputType->children();
      const auto& inputNames = inputType->names();
      const int64_t inputSize = inputNames.size();

      if (colIdx <= inputSize) {
        // Convert type to row.
        return std::make_shared<core::FieldAccessTypedExpr>(
            inputTypes[colIdx],
            std::make_shared<core::InputTypedExpr>(inputTypes[colIdx]),
            inputNames[colIdx]);
      } else {
        VELOX_FAIL("Missing the column with id '{}' .", colIdx);
      }
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for Reference '{}'", typeCase);
  }
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toAliasExpr(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& params) {
  VELOX_CHECK(params.size() == 1, "Alias expects one parameter.");
  return params[0];
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toIsNotNullExpr(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& params,
    const TypePtr& outputType) {
  // Convert is_not_null to not(is_null).
  auto isNullExpr = std::make_shared<const core::CallTypedExpr>(
      outputType, std::move(params), "is_null");
  std::vector<std::shared_ptr<const core::ITypedExpr>> notParams;
  notParams.reserve(1);
  notParams.emplace_back(isNullExpr);
  return std::make_shared<const core::CallTypedExpr>(
      outputType, std::move(notParams), "not");
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::ScalarFunction& sFunc,
    const RowTypePtr& inputType) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> params;
  params.reserve(sFunc.args().size());
  for (const auto& sArg : sFunc.args()) {
    params.emplace_back(toVeloxExpr(sArg, inputType));
  }
  const auto& veloxFunction =
      subParser_->findVeloxFunction(functionMap_, sFunc.function_reference());
  const auto& veloxType =
      toVeloxType(subParser_->parseType(sFunc.output_type())->type);

  // Omit alias because because name change is not needed.
  if (veloxFunction == "alias") {
    return toAliasExpr(params);
  }
  if (veloxFunction == "is_not_null") {
    return toIsNotNullExpr(params, veloxType);
  }
  return std::make_shared<const core::CallTypedExpr>(
      veloxType, std::move(params), veloxFunction);
}

TypePtr SubstraitVeloxExprConverter::literalToType(
    const ::substrait::Expression::Literal& literal) {
  auto typeCase = literal.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return BOOLEAN();
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return INTEGER();
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return BIGINT();
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return DOUBLE();
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
      return VARCHAR();
    default:
      VELOX_NYI("LiteralToType not supported for type case '{}'", typeCase);
  }
}

variant SubstraitVeloxExprConverter::toVariant(
    const ::substrait::Expression::Literal& literal) {
  auto typeCase = literal.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return variant(literal.boolean());
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return variant(literal.i32());
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return variant(literal.i64());
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return variant(literal.fp64());
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
      return variant(literal.string());
    default:
      VELOX_NYI("ToVariant not supported for type case '{}'", typeCase);
  }
}

std::shared_ptr<const core::ConstantTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Literal& sLit) {
  auto typeCase = sLit.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
      return std::make_shared<core::ConstantTypedExpr>(toVariant(sLit));
    case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
      std::vector<variant> variants;
      variants.reserve(sLit.list().values().size());
      VELOX_CHECK(
          sLit.list().values().size() > 0,
          "List should have at least one item.");
      std::optional<TypePtr> literalType = std::nullopt;
      for (const auto& literal : sLit.list().values()) {
        if (!literalType.has_value()) {
          literalType = literalToType(literal);
        }
        variants.emplace_back(toVariant(literal));
      }
      VELOX_CHECK(literalType.has_value(), "Type expected.");
      auto type = literalType.value();
      VectorPtr vector = setVectorFromVariants(type, variants, pool_.get());
      ArrayVectorPtr arrayVector = toArrayVector(type, vector, pool_.get());
      auto constantVector = BaseVector::wrapInConstant(1, 0, arrayVector);
      auto constantExpr =
          std::make_shared<core::ConstantTypedExpr>(constantVector);
      return constantExpr;
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for type case '{}'", typeCase);
  }
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Cast& castExpr,
    const RowTypePtr& inputType) {
  auto substraitType = subParser_->parseType(castExpr.type());
  auto type = toVeloxType(substraitType->type);
  // TODO add flag in substrait after. now is set false.
  bool nullOnFailure = false;

  std::vector<core::TypedExprPtr> inputs{
      toVeloxExpr(castExpr.input(), inputType)};

  return std::make_shared<core::CastTypedExpr>(type, inputs, nullOnFailure);
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression& sExpr,
    const RowTypePtr& inputType) {
  std::shared_ptr<const core::ITypedExpr> veloxExpr;
  auto typeCase = sExpr.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kLiteral:
      return toVeloxExpr(sExpr.literal());
    case ::substrait::Expression::RexTypeCase::kScalarFunction:
      return toVeloxExpr(sExpr.scalar_function(), inputType);
    case ::substrait::Expression::RexTypeCase::kSelection:
      return toVeloxExpr(sExpr.selection(), inputType);
    case ::substrait::Expression::RexTypeCase::kCast:
      return toVeloxExpr(sExpr.cast(), inputType);
    default:
      VELOX_NYI(
          "Substrait conversion not supported for Expression '{}'", typeCase);
  }
}

} // namespace facebook::velox::substrait
