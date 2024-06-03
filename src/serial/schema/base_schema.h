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

#ifndef DINGO_SERIAL_BASE_SCHEMA_H_
#define DINGO_SERIAL_BASE_SCHEMA_H_

#include <any>
#include <cstdint>
#include <memory>
#include <string>

#include "serial/buf.h"

namespace dingodb {

class BaseSchema;
using BaseSchemaPtr = std::shared_ptr<BaseSchema>;

class BaseSchema {
 public:
  virtual ~BaseSchema() = default;

  enum Type {
    kBool,
    kInteger,
    kFloat,
    kLong,
    kDouble,
    kString,
    kBoolList,
    kIntegerList,
    kFloatList,
    kLongList,
    kDoubleList,
    kStringList
  };

  static const char* GetTypeString(Type type) {
    switch (type) {
      case kBool:
        return "kBool";
      case kInteger:
        return "kInteger";
      case kFloat:
        return "kFloat";
      case kLong:
        return "kLong";
      case kDouble:
        return "kDouble";
      case kString:
        return "kString";
      case kBoolList:
        return "kBoolList";
      case kIntegerList:
        return "kIntegerList";
      case kFloatList:
        return "kFloatList";
      case kLongList:
        return "kLongList";
      case kDoubleList:
        return "kDoubleList";
      case kStringList:
        return "kStringList";
      default:
        return "unknown";
    }
  }

  virtual Type GetType() = 0;
  virtual int GetLength() = 0;

  virtual BaseSchemaPtr Clone() = 0;

  bool IsLe() const { return le_; }
  bool AllowNull() const { return allow_null_; }
  bool IsKey() const { return is_key_; }
  int GetIndex() const { return index_; }

  void SetName(const std::string& name) { name_ = name; }
  const std::string& GetName() const { return name_; }

  void SetIndex(int index) { index_ = index; }
  void SetIsLe(bool le) { le_ = le; }
  void SetIsKey(bool is_key) { is_key_ = is_key; }
  void SetAllowNull(bool allow_null) { allow_null_ = allow_null; }

  virtual int SkipKey(Buf& buf) = 0;
  virtual int SkipValue(Buf& buf) = 0;

  virtual int EncodeKey(const std::any& data, Buf& buf) = 0;
  virtual int EncodeValue(const std::any& data, Buf& buf) = 0;

  virtual std::any DecodeKey(Buf& buf) = 0;
  virtual std::any DecodeValue(Buf& buf) = 0;

 protected:
  const uint8_t k_null = 0;
  const uint8_t k_not_null = 1;

 private:
  std::string name_;

  bool le_{true};
  bool is_key_{false};
  bool allow_null_{false};
  int index_;
};

}  // namespace dingodb

#endif
