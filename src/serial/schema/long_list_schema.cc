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

#include "long_list_schema.h"

#include <cstdint>
#include <utility>

#include "serial/compiler.h"

namespace dingodb {

void DingoSchema<std::vector<int64_t>>::EncodeLongList(const std::vector<int64_t>& data, Buf& buf) {
  buf.WriteInt(data.size());
  if (DINGO_LIKELY(IsLe())) {
    for (const int64_t& value : data) {
      uint64_t* l = (uint64_t*)&value;
      buf.Write(*l >> 56);
      buf.Write(*l >> 48);
      buf.Write(*l >> 40);
      buf.Write(*l >> 32);
      buf.Write(*l >> 24);
      buf.Write(*l >> 16);
      buf.Write(*l >> 8);
      buf.Write(*l);
    }
  } else {
    for (const int64_t& value : data) {
      uint64_t* l = (uint64_t*)&value;
      buf.Write(*l);
      buf.Write(*l >> 8);
      buf.Write(*l >> 16);
      buf.Write(*l >> 24);
      buf.Write(*l >> 32);
      buf.Write(*l >> 40);
      buf.Write(*l >> 48);
      buf.Write(*l >> 56);
    }
  }
}

void DingoSchema<std::vector<int64_t>>::DecodeLongList(Buf& buf, std::vector<int64_t>& data) const {
  int size = buf.ReadInt();
  data.resize(size);

  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < size; ++i) {
      uint64_t value = buf.Read() & 0xFF;
      for (int j = 0; j < 7; ++j) {
        value <<= 8;
        value |= buf.Read() & 0xFF;
      }

      data[i] = static_cast<int64_t>(value);
    }
  } else {
    for (int i = 0; i < size; ++i) {
      uint64_t value = buf.Read() & 0xFF;

      for (int j = 1; j < 8; ++j) {
        value |= (((uint64_t)buf.Read() & 0xFF) << (8 * j));
      }

      data[i] = static_cast<int64_t>(value);
    }
  }
}

int DingoSchema<std::vector<int64_t>>::GetLength() {
  throw std::runtime_error("long list unsupport length");
  return -1;
}

int DingoSchema<std::vector<int64_t>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

int DingoSchema<std::vector<int64_t>>::SkipValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return 1;
  }

  int32_t size = buf.ReadInt() * 8;
  buf.Skip(size);

  return size + 5;
}

int DingoSchema<std::vector<int64_t>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

// {is_null: 1byte}|{n:4byte}|{value: 8byte}*n
int DingoSchema<std::vector<int64_t>>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const std::vector<int64_t>&>(data);
    EncodeLongList(ref_data, buf);

    return ref_data.size() * 8 + 5;
  } else {
    buf.Write(k_null);

    return 1;
  }
}

std::any DingoSchema<std::vector<int64_t>>::DecodeKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
}

std::any DingoSchema<std::vector<int64_t>>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  std::vector<int64_t> data;
  DecodeLongList(buf, data);

  return std::move(std::any(std::move(data)));
}

}  // namespace dingodb