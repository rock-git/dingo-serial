// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "serial/schema/base_schema.h"

namespace dingodb {

void SortSchema(std::vector<BaseSchemaPtr>& schemas) {
  uint32_t flag = 1;
  for (uint32_t i = 0; i < schemas.size() - flag; i++) {
    auto bs = schemas.at(i);
    if (bs != nullptr) {
      if ((!bs->IsKey()) && (bs->GetLength() == 0)) {
        int32_t target = schemas.size() - flag++;
        auto ts = schemas.at(target);
        while ((ts->GetLength() == 0) || ts->IsKey()) {
          target--;
          if (static_cast<uint32_t>(target) == i) {
            return;
          }
          flag++;
        }
        schemas.at(i) = ts;
        schemas.at(target) = bs;
      }
    }
  }
}

void FormatSchema(std::vector<BaseSchemaPtr>& schemas, bool le) {
  for (auto& schema : schemas) {
    if (schema != nullptr) {
      schema->SetIsLe(le);
    }
  }
}

bool VectorFindAndRemove(std::vector<int>& vec, int t) {
  for (auto it = vec.begin(); it != vec.end(); ++it) {
    if (*it == t) {
      vec.erase(it);
      return true;
    }
  }
  return false;
}

bool IsLE() {
  uint32_t i = 1;
  char* c = (char*)&i;
  return *c == 1;
}

bool StringToBool(const std::string& str) { return !(str == "0" || str == "false"); }
int32_t StringToInt32(const std::string& str) { return std::strtol(str.c_str(), nullptr, 10); }
int64_t StringToInt64(const std::string& str) { return std::strtoll(str.c_str(), nullptr, 10); }
float StringToFloat(const std::string& str) { return std::strtof(str.c_str(), nullptr); }
double StringToDouble(const std::string& str) { return std::strtod(str.c_str(), nullptr); }

}  // namespace dingodb
