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

#include "boolean_list_schema.h"

#include <any>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "serial/compiler.h"

namespace dingodb {

int DingoSchema<std::vector<bool>>::GetLength() {
  throw std::runtime_error("bool list unsupport length");
  return -1;
}

int DingoSchema<std::vector<bool>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

int DingoSchema<std::vector<bool>>::SkipValue(Buf& buf) {
  uint8_t is_null = buf.Read();
  if (is_null == k_null) {
    return 1;
  }

  int32_t size = buf.ReadInt();
  buf.Skip(size);

  return size + 1;
}

int DingoSchema<std::vector<bool>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

// {is_null: 1byte}|{n:4byte}|{value: 1byte}*n
int DingoSchema<std::vector<bool>>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (DINGO_LIKELY(data.has_value())) {
    const auto& ref_data = std::any_cast<const std::vector<bool>&>(data);

    buf.Write(k_not_null);
    buf.WriteInt(ref_data.size());
    for (const bool& value : ref_data) {
      buf.Write(value ? 1 : 0);
    }

    return ref_data.size() + 5;
  } else {
    buf.Write(k_null);
    return 1;
  }
}

std::any DingoSchema<std::vector<bool>>::DecodeKey(Buf&) { throw std::runtime_error("Unsupport decode key list type"); }

std::any DingoSchema<std::vector<bool>>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  int size = buf.ReadInt();

  std::vector<bool> data(size, false);
  for (int i = 0; i < size; ++i) {
    data[i] = buf.Read();
  }

  return std::any(std::move(data));
}

}  // namespace dingodb