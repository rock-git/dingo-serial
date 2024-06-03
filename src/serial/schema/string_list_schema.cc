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

#include "string_list_schema.h"

#include <any>
#include <memory>
#include <string>
#include <utility>

#include "serial/compiler.h"

namespace dingodb {

int DingoSchema<std::vector<std::string>>::EncodeStringListNotComparable(const std::vector<std::string>& data,
                                                                         Buf& buf) {
  int size = 4;
  buf.WriteInt(data.size());
  for (const std::string& str : data) {
    buf.WriteInt(str.length());
    buf.WriteString(str);
    size += str.size() + 4;
  }

  return size;
}

void DingoSchema<std::vector<std::string>>::DecodeStringListNotComparable(Buf& buf, std::vector<std::string>& data) {
  int size = buf.ReadInt();
  data.resize(size);
  for (int i = 0; i < size; ++i) {
    int str_len = buf.ReadInt();
    std::string str(str_len, '\n');
    for (int j = 0; j < str_len; ++j) {
      str[j] = buf.Read();
    }
    data[i] = std::move(str);
  }
}

int DingoSchema<std::vector<std::string>>::GetLength() {
  throw std::runtime_error("string list unsupport length");
  return -1;
}

int DingoSchema<std::vector<std::string>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupported encode key list type");
  return -1;
}

int DingoSchema<std::vector<std::string>>::SkipValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return 1;
  }

  int size = 5;
  int str_num = buf.ReadInt();
  for (int i = 0; i < str_num; ++i) {
    int str_len = buf.ReadInt();
    buf.Skip(str_len);
    size += str_len + 4;
  }

  return size;
}

int DingoSchema<std::vector<std::string>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupported encode key list type");
  return -1;
}

//
int DingoSchema<std::vector<std::string>>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const std::vector<std::string>&>(data);
    return EncodeStringListNotComparable(ref_data, buf) + 1;
  } else {
    buf.Write(k_null);

    return 1;
  }
}

std::any DingoSchema<std::vector<std::string>>::DecodeKey(Buf&) {
  throw std::runtime_error("Unsupported encode key list type");
}

std::any DingoSchema<std::vector<std::string>>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  std::vector<std::string> data;
  DecodeStringListNotComparable(buf, data);

  return std::any(std::move(data));
}

}  // namespace dingodb