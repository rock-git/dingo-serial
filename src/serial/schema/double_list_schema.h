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

#ifndef DINGO_SERIAL_DOUBLE_LIST_SCHEMA_H_
#define DINGO_SERIAL_DOUBLE_LIST_SCHEMA_H_

#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <vector>

#include "dingo_schema.h"

namespace dingodb {

template <>
class DingoSchema<std::vector<double>> : public BaseSchema {
 public:
  Type GetType() override { return kDoubleList; }
  int GetLength() override;

  BaseSchemaPtr Clone() override { return std::make_shared<DingoSchema<std::vector<double>>>(); }

  int SkipKey(Buf& buf) override;
  int SkipValue(Buf& buf) override;

  int EncodeKey(const std::any& data, Buf& buf) override;
  int EncodeValue(const std::any& data, Buf& buf) override;

  std::any DecodeKey(Buf& buf) override;
  std::any DecodeValue(Buf& buf) override;

 private:
  void EncodeDoubleList(const std::vector<double>& data, Buf& buf);
  void DecodeDoubleList(Buf& buf, std::vector<double>& data);
};

}  // namespace dingodb

#endif