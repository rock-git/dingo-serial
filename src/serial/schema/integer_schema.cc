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

#include "integer_schema.h"

#include <any>
#include <cstdint>

#include "serial/compiler.h"

namespace dingodb {

constexpr int kDataLength = 5;

void DingoSchema<int32_t>::EncodeIntComparable(int32_t data, Buf& buf) {
  uint32_t* i = (uint32_t*)&data;
  if (DINGO_LIKELY(IsLe())) {
    buf.Write(*i >> 24 ^ 0x80);
    buf.Write(*i >> 16);
    buf.Write(*i >> 8);
    buf.Write(*i);
  } else {
    buf.Write(*i ^ 0x80);
    buf.Write(*i >> 8);
    buf.Write(*i >> 16);
    buf.Write(*i >> 24);
  }
}

int32_t DingoSchema<int32_t>::DecodeIntComparable(Buf& buf) {
  uint32_t data;
  if (DINGO_LIKELY(IsLe())) {
    data = (((buf.Read() & 0xFF) ^ 0x80) << 24) | ((buf.Read() & 0xFF) << 16) | ((buf.Read() & 0xFF) << 8) |
           (buf.Read() & 0xFF);
  } else {
    data = ((buf.Read() & 0xFF) ^ 0x80) | ((buf.Read() & 0xFF) << 8) | ((buf.Read() & 0xFF) << 16) |
           ((buf.Read() & 0xFF) << 24);
  }

  return static_cast<int32_t>(data);
}

void DingoSchema<int32_t>::EncodeIntNotComparable(int32_t data, Buf& buf) {
  uint32_t* i = (uint32_t*)&data;
  if (DINGO_LIKELY(IsLe())) {
    buf.Write(*i >> 24);
    buf.Write(*i >> 16);
    buf.Write(*i >> 8);
    buf.Write(*i);
  } else {
    buf.Write(*i);
    buf.Write(*i >> 8);
    buf.Write(*i >> 16);
    buf.Write(*i >> 24);
  }
}

int32_t DingoSchema<int32_t>::DecodeIntNotComparable(Buf& buf) {
  uint32_t data;
  if (DINGO_LIKELY(IsLe())) {
    data = ((buf.Read() & 0xFF) << 24) | ((buf.Read() & 0xFF) << 16) | ((buf.Read() & 0xFF) << 8) | (buf.Read() & 0xFF);
  } else {
    data = (buf.Read() & 0xFF) | ((buf.Read() & 0xFF) << 8) | ((buf.Read() & 0xFF) << 16) | ((buf.Read() & 0xFF) << 24);
  }

  return static_cast<int32_t>(data);
}

int DingoSchema<int32_t>::GetLength() { return kDataLength; }

int DingoSchema<int32_t>::SkipKey(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

int DingoSchema<int32_t>::SkipValue(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

// {is_null: 1byte}|{value: 4byte}
int DingoSchema<int32_t>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const int32_t&>(data);
    EncodeIntComparable(ref_data, buf);

  } else {
    buf.Write(k_null);
    buf.WriteInt(0);
  }

  return kDataLength;
}

// {is_null: 1byte}|{value: 4byte}
int DingoSchema<int32_t>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const int32_t&>(data);
    EncodeIntNotComparable(ref_data, buf);

  } else {
    buf.Write(k_null);
    buf.WriteInt(0);
  }

  return kDataLength;
}

std::any DingoSchema<int32_t>::DecodeKey(Buf& buf) {
  if (buf.Read() == k_null) {
    buf.Skip(kDataLength - 1);
    return std::any();
  }

  return std::any(DecodeIntComparable(buf));
}

std::any DingoSchema<int32_t>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    buf.Skip(kDataLength - 1);
    return std::any();
  }

  return std::any(DecodeIntNotComparable(buf));
}

}  // namespace dingodb