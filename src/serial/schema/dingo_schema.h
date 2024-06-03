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

#ifndef DINGO_SERIAL_DINGO_SCHEMA_H_
#define DINGO_SERIAL_DINGO_SCHEMA_H_

#include <memory>

#include "base_schema.h"
#include "serial/buf.h"

namespace dingodb {

template <class T>
class DingoSchema : public BaseSchema {
 public:
  BaseSchemaPtr Clone() override { return nullptr; }

  int SkipKey(Buf& /*buf*/) override { return 0; }
  int SkipValue(Buf& /*buf*/) override { return 0; }

  int EncodeKey(const std::any& /*data*/, Buf& /*buf*/) override { return 0; }
  int EncodeValue(const std::any& /*data*/, Buf& /*buf*/) override { return 0; }

  std::any DecodeKey(Buf& buf /*NOLINT*/) override { return std::any(); }
  std::any DecodeValue(Buf& buf /*NOLINT*/) override { return std::any(); }
};

}  // namespace dingodb

#endif