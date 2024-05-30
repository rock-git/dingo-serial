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

#include <memory>
#include <optional>
#include <vector>

#include "serial/schema/dingo_schema.h"

namespace dingodb {

template <>

class DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>> : public BaseSchema {
 private:
  int index_;
  bool key_, allow_null_;
  bool le_ = true;

  static int GetDataLength();
  static int GetWithNullTagLength();
  static void LeInternalEncodeValue(Buf* buf, double data);
  static void BeInternalEncodeValue(Buf* buf, double data);

 public:
  Type GetType() override;
  bool AllowNull() override;
  int GetLength() override;
  bool IsKey() override;
  int GetIndex() override;
  void SetIndex(int index);
  void SetIsKey(bool key);
  void SetAllowNull(bool allow_null);
  void SetIsLe(bool le);
  static void EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<double>>> data);
  static void EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<double>>> data);
  static std::optional<std::shared_ptr<std::vector<double>>> DecodeKey(Buf* buf);
  static void SkipKey(Buf* buf);
  double InternalDecodeData(Buf* buf) const;
  void EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<double>>> data);
  std::optional<std::shared_ptr<std::vector<double>>> DecodeValue(Buf* buf);
  void SkipValue(Buf* buf);
};

}  // namespace dingodb

#endif