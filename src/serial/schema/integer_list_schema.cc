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

#include "integer_list_schema.h"

#include <cstdint>
#include <utility>

#include "serial/compiler.h"

namespace dingodb {

constexpr int kDataLength = 4;
constexpr int kDataWithNullLength = 5;

void DingoSchema<std::vector<int32_t>>::EncodeIntList(const std::vector<int32_t>& data, Buf& buf) {
  buf.WriteInt(data.size());

  if (DINGO_LIKELY(IsLe())) {
    for (const int32_t& value : data) {
      uint32_t* v = (uint32_t*)&value;
      buf.Write(*v >> 24);
      buf.Write(*v >> 16);
      buf.Write(*v >> 8);
      buf.Write(*v);
    }
  } else {
    for (const int32_t& value : data) {
      uint32_t* v = (uint32_t*)&value;
      buf.Write(*v);
      buf.Write(*v >> 8);
      buf.Write(*v >> 16);
      buf.Write(*v >> 24);
    }
  }
}

void DingoSchema<std::vector<int32_t>>::DecodeIntList(Buf& buf, std::vector<int32_t>& data) {
  int size = buf.ReadInt();
  data.resize(size);

  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < size; ++i) {
      uint32_t value =
          ((buf.Read() & 0xFF) << 24) | ((buf.Read() & 0xFF) << 16) | ((buf.Read() & 0xFF) << 8) | (buf.Read() & 0xFF);
      data[i] = static_cast<int32_t>(value);
    }

  } else {
    for (int i = 0; i < size; ++i) {
      uint32_t value =
          (buf.Read() & 0xFF) | ((buf.Read() & 0xFF) << 8) | ((buf.Read() & 0xFF) << 16) | ((buf.Read() & 0xFF) << 24);
      data[i] = static_cast<int32_t>(value);
    }
  }
}

int DingoSchema<std::vector<int32_t>>::GetLength() {
  throw std::runtime_error("int list unsupport length");
  return -1;
}

int DingoSchema<std::vector<int32_t>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

int DingoSchema<std::vector<int32_t>>::SkipValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return 1;
  }
  int32_t size = buf.ReadInt() * 4;
  buf.Skip(size);

  return size + 5;
}

int DingoSchema<std::vector<int32_t>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

// {is_null: 1byte}|{n:4byte}|{value: 4byte}*n
int DingoSchema<std::vector<int32_t>>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const std::vector<int32_t>&>(data);
    EncodeIntList(ref_data, buf);

    return ref_data.size() * 4 + 5;
  } else {
    buf.Write(k_null);
    return 1;
  }
}

std::any DingoSchema<std::vector<int32_t>>::DecodeKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
}

std::any DingoSchema<std::vector<int32_t>>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  std::vector<int32_t> data;
  DecodeIntList(buf, data);

  return std::move(std::any(std::move(data)));
}

}  // namespace dingodb