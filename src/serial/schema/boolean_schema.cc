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

#include "boolean_schema.h"

#include <any>
#include <cassert>
#include <cstdint>

#include "serial/compiler.h"

namespace dingodb {

constexpr int kDataLength = 2;

int DingoSchema<bool>::GetLength() { return kDataLength; }

int DingoSchema<bool>::SkipKey(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

int DingoSchema<bool>::SkipValue(Buf& buf) {
  buf.Skip(kDataLength);
  return kDataLength;
}

inline int DingoSchema<bool>::Encode(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const bool&>(data);
    buf.Write(k_not_null);
    buf.Write(ref_data ? 1 : 0);
  } else {
    buf.Write(k_null);
    buf.Write(0);
  }

  return kDataLength;
}

int DingoSchema<bool>::EncodeKey(const std::any& data, Buf& buf) { return Encode(data, buf); }

int DingoSchema<bool>::EncodeValue(const std::any& data, Buf& buf) { return Encode(data, buf); }

std::any DingoSchema<bool>::DecodeKey(Buf& buf) {
  if (buf.Read() == k_null) {
    buf.Skip(kDataLength - 1);
    return std::any();
  }

  return std::any(static_cast<bool>(buf.Read()));
}

std::any DingoSchema<bool>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    buf.Skip(kDataLength - 1);
    return std::any();
  }

  return std::any(static_cast<bool>(buf.Read()));
}

}  // namespace dingodb