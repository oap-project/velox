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

#include <utility>

#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/lib/Re2Functions.h"

namespace facebook::velox::functions::sparksql {
namespace {

// str_to_map(expr [, pairDelim [, keyValueDelim] ] )
class StrToMap final : public exec::VectorFunction {
 public:
  StrToMap() = default;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);
    DecodedVector* strings = decodedArgs.at(0);
    char pairDelim = ',';
    char kvDelim = ':';
    VELOX_CHECK(
        !args.empty(),
        "StrToMap function should provide at least one argument");
    if (args.size() > 1) {
      pairDelim = args[1]->as<SimpleVector<StringView>>()->valueAt(0).data()[0];
      if (args.size() > 2) {
        kvDelim = args[2]->as<SimpleVector<StringView>>()->valueAt(0).data()[0];
      }
    }

    BaseVector::ensureWritable(
        rows, MAP(VARCHAR(), VARCHAR()), context.pool(), result);
    exec::VectorWriter<Map<Varchar, Varchar>> resultWriter;
    resultWriter.init(*result->as<MapVector>());

    std::unordered_map<StringView, vector_size_t> keyToIdx;
    rows.applyToSelected([&](vector_size_t row) {
      resultWriter.setOffset(row);
      auto& mapWriter = resultWriter.current();

      const StringView& current = strings->valueAt<StringView>(row);
      const char* pos = current.begin();
      const char* end = pos + current.size();
      const char* pair;
      const char* kv;
      do {
        pair = std::find(pos, end, pairDelim);
        kv = std::find(pos, pair, kvDelim);
        auto key = StringView(pos, kv - pos);
        auto iter = keyToIdx.find(key);
        if (iter == keyToIdx.end()) {
          keyToIdx.emplace(key, mapWriter.size());
          if (kv == pair) {
            mapWriter.add_null().append(key);
          } else {
            auto [keyWriter, valueWriter] = mapWriter.add_item();
            keyWriter.append(key);
            valueWriter.append(StringView(kv + 1, pair - kv - 1));
          }
        } else {
          auto valueWriter = std::get<1>(mapWriter[iter->second]);
          if (kv == pair) {
            valueWriter = std::nullopt;
          } else {
            valueWriter = StringView(kv + 1, pair - kv - 1);
          }
        }

        pos = pair + 1; // Skip past delim.
      } while (pair != end);

      resultWriter.commit();
    });

    resultWriter.finish();

    // Ensure that our result elements vector uses the same string buffer as
    // the input vector of strings.
    result->as<MapVector>()
        ->mapKeys()
        ->as<FlatVector<StringView>>()
        ->acquireSharedStringBuffers(strings->base());
    result->as<MapVector>()
        ->mapValues()
        ->as<FlatVector<StringView>>()
        ->acquireSharedStringBuffers(strings->base());
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> strToMapSignatures() {
  // varchar, varchar -> array(varchar)
  return {
      exec::FunctionSignatureBuilder()
          .returnType("map(varchar, varchar)")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("map(varchar, varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("map(varchar, varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build()};
}

std::shared_ptr<exec::VectorFunction> createStrToMap(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  return std::make_shared<StrToMap>();
}

/// The function returns specialized version of split based on the constant
/// inputs.
/// \param inputArgs the inputs types (VARCHAR, VARCHAR, int64) and constant
///     values (if provided).
std::shared_ptr<exec::VectorFunction> createSplit(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  return makeRe2SplitAll(name, inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // varchar, varchar -> array(varchar)
  return re2SplitAllSignatures();
}
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_str_to_map,
    strToMapSignatures(),
    createStrToMap);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_regexp_split,
    signatures(),
    createSplit);
} // namespace facebook::velox::functions::sparksql
