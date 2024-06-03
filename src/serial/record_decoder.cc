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

#include "record_decoder.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "serial/utils.h"

namespace dingodb {

// TODO cast and decode function not good, optimize on 0.8.0 or later

using CastAndDecodeOrSkipFuncPointer = void (*)(BaseSchemaPtr schema, Buf& key_buf, Buf& value_buf,
                                                std::vector<std::any>& record, int record_index, bool skip);

template <typename T>
void CastAndDecodeOrSkip(BaseSchemaPtr schema, Buf& key_buf, Buf& value_buf, std::vector<std::any>& record,
                         int record_index, bool is_skip) {
  auto dingo_schema = std::dynamic_pointer_cast<DingoSchema<T>>(schema);
  if (is_skip) {
    if (schema->IsKey()) {
      dingo_schema->SkipKey(key_buf);
    } else if (!value_buf.IsEnd()) {
      dingo_schema->SkipValue(value_buf);
    }
  } else {
    if (schema->IsKey()) {
      record.at(record_index) = dingo_schema->DecodeKey(key_buf);
    } else {
      if (value_buf.IsEnd()) {
        record.at(record_index) = std::any();
      } else {
        record.at(record_index) = dingo_schema->DecodeValue(value_buf);
      }
    }
  }
}

CastAndDecodeOrSkipFuncPointer cast_and_decode_or_skip_func_ptrs[] = {
    CastAndDecodeOrSkip<bool>,
    CastAndDecodeOrSkip<int32_t>,
    CastAndDecodeOrSkip<float>,
    CastAndDecodeOrSkip<int64_t>,
    CastAndDecodeOrSkip<double>,
    CastAndDecodeOrSkip<std::string>,
    CastAndDecodeOrSkip<std::vector<bool>>,
    CastAndDecodeOrSkip<std::vector<int32_t>>,
    CastAndDecodeOrSkip<std::vector<float>>,
    CastAndDecodeOrSkip<std::vector<int64_t>>,
    CastAndDecodeOrSkip<std::vector<double>>,
    CastAndDecodeOrSkip<std::vector<std::string>>,
};

RecordDecoder::RecordDecoder(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id)
    : RecordDecoder(schema_version, schemas, common_id, IsLE()) {}

RecordDecoder::RecordDecoder(int schema_version, const std::vector<BaseSchemaPtr>& schemas, long common_id, bool le)
    : le_(le), schema_version_(schema_version), common_id_(common_id), schemas_(schemas) {
  FormatSchema(schemas_, le);
}

bool RecordDecoder::CheckPrefix(Buf& buf) const {
  // skip name space
  buf.Skip(1);
  return buf.ReadLong() == common_id_;
}

bool RecordDecoder::CheckReverseTag(Buf& buf) const {
  // if (buf.ReverseRead() <= codec_version_) {
  //   buf.ReverseSkip(3);
  //   return true;
  // }
  return false;
}

bool RecordDecoder::CheckSchemaVersion(Buf& buf) const { return buf.ReadInt() <= schema_version_; }

void DecodeOrSkip(BaseSchemaPtr schema, Buf& key_buf, Buf& value_buf, std::vector<std::any>& record, int record_index,
                  bool skip) {
  cast_and_decode_or_skip_func_ptrs[static_cast<int>(schema->GetType())](schema, key_buf, value_buf, record,
                                                                         record_index, skip);
}

int RecordDecoder::Decode(const std::string& key, const std::string& value, std::vector<std::any>& record /*output*/) {
  Buf key_buf(key, this->le_);
  Buf value_buf(value, this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTag(key_buf) || !CheckSchemaVersion(value_buf)) {
    return -1;
  }

  record.resize(schemas_.size());
  for (const auto& bs : schemas_) {
    if (bs) {
      DecodeOrSkip(bs, key_buf, value_buf, record, bs->GetIndex(), false);
    }
  }

  return 0;
}

int RecordDecoder::Decode(std::string&& key, std::string&& value, std::vector<std::any>& record) {
  Buf key_buf(std::move(key), this->le_);
  Buf value_buf(std::move(value), this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTag(key_buf) || !CheckSchemaVersion(value_buf)) {
    return -1;
  }

  record.resize(schemas_.size());
  for (const auto& bs : schemas_) {
    if (bs) {
      DecodeOrSkip(bs, key_buf, value_buf, record, bs->GetIndex(), false);
    }
  }

  return 0;
}

int RecordDecoder::DecodeKey(const std::string& key, std::vector<std::any>& record /*output*/) {
  Buf key_buf(key, this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTag(key_buf)) {
    return -1;
  }

  record.resize(schemas_.size());
  int index = 0;
  for (const auto& bs : schemas_) {
    if (bs && bs->IsKey()) {
      DecodeOrSkip(bs, key_buf, key_buf, record, index, false);
    }
    index++;
  }

  return 0;
}

int RecordDecoder::Decode(const KeyValue& key_value, std::vector<std::any>& record) {
  return Decode(key_value.GetKey(), key_value.GetValue(), record);
}

int RecordDecoder::Decode(const std::string& key, const std::string& value, const std::vector<int>& column_indexes,
                          std::vector<std::any>& record) {
  Buf key_buf(key, this->le_);
  Buf value_buf(value, this->le_);

  if (!CheckPrefix(key_buf) || !CheckReverseTag(key_buf) || !CheckSchemaVersion(value_buf)) {
    return -1;
  }

  uint32_t size = column_indexes.size();
  record.resize(size);

  // clo_index: offset
  std::vector<std::pair<uint32_t, uint32_t>> col_index_mapping;
  col_index_mapping.reserve(size);
  for (uint32_t i = 0; i < size; ++i) {
    col_index_mapping.push_back(std::make_pair(column_indexes[i], i));
  }

  // sort indexed_mapping_index
  std::sort(col_index_mapping.begin(), col_index_mapping.end());

  uint32_t decode_col_count = 0;
  for (uint32_t i = 0; i < schemas_.size(); ++i) {
    auto& schema = schemas_[i];
    if (schema == nullptr) {
      continue;
    }
    if (decode_col_count == size) {
      break;
    }
    auto& item = col_index_mapping[decode_col_count];

    int offset = -1;
    if (item.first == i) {
      offset = item.second;
      ++decode_col_count;
    }

    DecodeOrSkip(schema, key_buf, value_buf, record, offset, offset == -1);
  }

  return 0;
}

int RecordDecoder::Decode(const KeyValue& key_value, const std::vector<int>& column_indexes,
                          std::vector<std::any>& record) {
  return Decode(key_value.GetKey(), key_value.GetValue(), column_indexes, record);
}

}  // namespace dingodb
