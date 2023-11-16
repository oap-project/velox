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

#include "velox/functions/prestosql/SIMDJsonFunctions.h"

using namespace simdjson;

namespace facebook::velox::functions::sparksql {

template <typename T>
struct SIMDGetJsonObjectFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  std::optional<std::string> formattedJsonPath_;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE std::string prepareJsonPath(
      const arg_type<Varchar>& jsonPath) {
    // Makes a conversion from spark's json path, e.g., "$.a.b".
    char formattedJsonPath[jsonPath.size() + 1];
    int j = 0;
    for (int i = 0; i < jsonPath.size(); i++) {
      if (jsonPath.data()[i] == '$' || jsonPath.data()[i] == ']' ||
          jsonPath.data()[i] == '\'') {
        continue;
      } else if (jsonPath.data()[i] == '[' || jsonPath.data()[i] == '.') {
        formattedJsonPath[j] = '/';
        j++;
      } else {
        formattedJsonPath[j] = jsonPath.data()[i];
        j++;
      }
    }
    formattedJsonPath[j] = '\0';
    return std::string(formattedJsonPath, j + 1);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*json*/,
      const arg_type<Varchar>* jsonPath) {
    if (jsonPath != nullptr) {
      formattedJsonPath_ = prepareJsonPath(*jsonPath);
    }
  }

  FOLLY_ALWAYS_INLINE simdjson::error_code extractStringResult(
      simdjson_result<ondemand::value> rawResult,
      std::string* res) {
    switch (rawResult.type()) {
      case ondemand::json_type::number: {
        std::stringstream ss;
        switch (rawResult.get_number_type()) {
          case ondemand::number_type::unsigned_integer: {
            uint64_t numRes;
            auto error = rawResult.get_uint64().get(numRes);
            if (!error) {
              ss << numRes;
              *res = ss.str();
            }
            return error;
          }
          case ondemand::number_type::signed_integer: {
            int64_t numRes;
            auto error = rawResult.get_int64().get(numRes);
            if (!error) {
              ss << numRes;
              *res = ss.str();
            }
            return error;
          }
          case ondemand::number_type::floating_point_number: {
            double numRes;
            auto error = rawResult.get_double().get(numRes);
            if (!error) {
              ss << numRes;
              *res = ss.str();
            }
            return error;
          }
        }
      }
      case ondemand::json_type::string: {
        std::string_view resView;
        auto error = rawResult.get_string().get(resView);
        *res = std::string(resView);
        return error;
      }
      case ondemand::json_type::boolean: {
        bool boolRes = false;
        rawResult.get_bool().get(boolRes);
        if (boolRes) {
          *res = "true";
        } else {
          *res = "false";
        }
        return SUCCESS;
      }
      case ondemand::json_type::object: {
        // For nested case, e.g., for "{"my": {"hello": 10}}", "$.my" will
        // return an object type.
        auto obj = rawResult.get_object();
        // For the case that result is a json object.
        std::stringstream ss;
        ss << obj;
        *res = ss.str();
        return SUCCESS;
      }
      case ondemand::json_type::array: {
        auto arrayObj = rawResult.get_array();
        // For the case that result is a json object.
        std::stringstream ss;
        ss << arrayObj;
        *res = ss.str();
        return SUCCESS;
      }
      default: {
        return UNSUPPORTED_ARCHITECTURE;
      }
    }
  }

  // This is a simple validation by checking whether the obtained result is
  // followed by expected char. It is useful in ondemand kind of parsing which
  // ignores the json format validation for characters following the current
  // parsing position. For many cases, even though this function returns true,
  // the raw json string can still be illegal possibly.
  bool isValidEnding(const char* currentPos) {
    char endingChar = *currentPos;
    if (endingChar == ',') {
      return true;
    } else if (endingChar == '}') {
      return true;
    } else if (endingChar == ']') {
      return true;
    } else if (
        endingChar == ' ' || endingChar == '\r' || endingChar == '\n' ||
        endingChar == '\t') {
      // space, '\r', '\n' or '\t' can precede valid ending char.
      return isValidEnding(currentPos++);
    } else {
      return false;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
      const arg_type<Varchar>& jsonPath) {
    ParserContext ctx(json.data(), json.size());
    try {
      ctx.parseDocument();
    } catch (simdjson_error& e) {
      return false;
    }

    simdjson_result<ondemand::value> rawResult;
    try {
      if (formattedJsonPath_.has_value()) {
        rawResult = ctx.jsonDoc.at_pointer(formattedJsonPath_.value().data());
      } else {
        rawResult = ctx.jsonDoc.at_pointer(prepareJsonPath(jsonPath).data());
      }
    } catch (simdjson_error& e) {
      return false;
    }
    // Field not found.
    if (rawResult.error() == NO_SUCH_FIELD) {
      return false;
    }
    std::string res;
    try {
      auto error = extractStringResult(rawResult, &res);
      if (error) {
        return false;
      }
    } catch (simdjson_error& e) {
      return false;
    }

    const char* currentPos;
    ctx.jsonDoc.current_location().get(currentPos);
    if (!isValidEnding(currentPos)) {
      return false;
    }

    result.resize(res.length());
    std::memcpy(result.data(), res.data(), res.length());
    return true;
  }
};

} // namespace facebook::velox::functions::sparksql
