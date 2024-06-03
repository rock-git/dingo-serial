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

#include "record_encoder.h"

#include <sys/types.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "serial/keyvalue.h"  // IWYU pragma: keep

namespace dingodb {

const int kBufInitCapacity = 1024;

RecordEncoder::RecordEncoder(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id)
    : RecordEncoder(schema_version, schemas, common_id, IsLE()) {}

RecordEncoder::RecordEncoder(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id, bool le)
    : le_(le), schema_version_(schema_version), common_id_(common_id), schemas_(schemas) {
  FormatSchema(schemas_, le);
}

void RecordEncoder::EncodePrefix(Buf& buf, char prefix) const {
  buf.Write(prefix);
  buf.WriteLong(common_id_);
}

void RecordEncoder::EncodeReverseTag(Buf& buf) const {
  // buf.ReverseWrite(codec_version_);
  // buf.ReverseWrite(0);
  // buf.ReverseWrite(0);
  // buf.ReverseWrite(0);
}

void RecordEncoder::EncodeSchemaVersion(Buf& buf) const { buf.WriteInt(schema_version_); }

int RecordEncoder::Encode(char prefix, const std::vector<std::any>& record, std::string& key, std::string& value) {
  int ret = EncodeKey(prefix, record, key);
  if (ret < 0) {
    return ret;
  }
  ret = EncodeValue(record, value);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RecordEncoder::EncodeKey(char prefix, const std::vector<std::any>& record, std::string& output) {
  Buf buf(kBufInitCapacity, this->le_);
  // |namespace|id| ... |tag|
  EncodePrefix(buf, prefix);
  EncodeReverseTag(buf);

  for (uint32_t i = 0; i < schemas_.size(); ++i) {
    const auto& schema = schemas_.at(i);

    if (schema->IsKey()) {
      const auto& column = record.at(i);
      schema->EncodeKey(column, buf);
    }
  }

  buf.GetString(output);
  return output.size();
}

int RecordEncoder::EncodeValue(const std::vector<std::any>& record, std::string& output) {
  Buf buf(kBufInitCapacity, this->le_);
  EncodeSchemaVersion(buf);

  for (const auto& schema : schemas_) {
    if (!schema->IsKey()) {
      const auto& column = record.at(schema->GetIndex());
      schema->EncodeValue(column, buf);
    }
  }

  buf.GetString(output);
  return output.size();
}

int RecordEncoder::EncodeMaxKeyPrefix(char prefix, std::string& output) const {
  if (common_id_ == INT64_MAX) {
    return -1;
  }

  Buf buf(kBufInitCapacity, this->le_);
  buf.Write(prefix);
  buf.WriteLong(common_id_ + 1);

  buf.GetString(output);
  return output.size();
}

int RecordEncoder::EncodeMinKeyPrefix(char prefix, std::string& output) const {
  Buf buf(kBufInitCapacity, this->le_);

  buf.Write(prefix);
  buf.WriteLong(common_id_);

  buf.GetString(output);
  return output.size();
}

}  // namespace dingodb
