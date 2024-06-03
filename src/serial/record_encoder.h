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

#ifndef DINGO_SERIAL_RECORD_ENCODER_H_
#define DINGO_SERIAL_RECORD_ENCODER_H_

#include <any>
#include <functional>
#include <memory>
#include <string>

#include "keyvalue.h"  // IWYU pragma: keep
#include "schema/boolean_list_schema.h"
#include "schema/boolean_schema.h"  // IWYU pragma: keep
#include "schema/double_list_schema.h"
#include "schema/double_schema.h"  // IWYU pragma: keep
#include "schema/float_list_schema.h"
#include "schema/float_schema.h"  // IWYU pragma: keep
#include "schema/integer_list_schema.h"
#include "schema/integer_schema.h"  // IWYU pragma: keep
#include "schema/long_list_schema.h"
#include "schema/long_schema.h"  // IWYU pragma: keep
#include "schema/string_list_schema.h"
#include "schema/string_schema.h"  // IWYU pragma: keep
#include "utils.h"                 // IWYU pragma: keep

namespace dingodb {

class RecordEncoder;
using RecordEncoderPtr = std::shared_ptr<RecordEncoder>;

class RecordEncoder {
 public:
  RecordEncoder(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id);
  RecordEncoder(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id, bool le);

  static RecordEncoderPtr New(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id) {
    return std::make_shared<RecordEncoder>(schema_version, schemas, common_id);
  }

  int Encode(char prefix, const std::vector<std::any>& record, std::string& key, std::string& value);

  int EncodeKey(char prefix, const std::vector<std::any>& record, std::string& output);
  int EncodeValue(const std::vector<std::any>& record, std::string& output);

  int EncodeMaxKeyPrefix(char prefix, std::string& output) const;
  int EncodeMinKeyPrefix(char prefix, std::string& output) const;

 private:
  void EncodePrefix(Buf& buf, char prefix) const;
  void EncodeReverseTag(Buf& buf) const;
  void EncodeSchemaVersion(Buf& buf) const;

  bool le_;
  uint8_t codec_version_ = 1;
  int schema_version_;
  long common_id_;

  std::vector<BaseSchemaPtr> schemas_;
};

}  // namespace dingodb

#endif
