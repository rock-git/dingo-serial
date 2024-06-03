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

#include "string_schema.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "serial/compiler.h"

namespace dingodb {

const int kGroupSize = 8;
const int kPadGroupSize = 9;
const uint8_t kMarker = 255;

int DingoSchema<std::string>::EncodeBytesComparable(const std::string& data, Buf& buf) {
  for (uint32_t i = 0; i < data.size(); ++i) {
    buf.Write(data.at(i));
    if ((i + 1) % kGroupSize == 0) {
      buf.Write(kMarker);
    }
  }

  int group_num = data.size() / kGroupSize + 1;
  int pad_count = group_num * kGroupSize - data.size();
  for (int i = 0; i < pad_count; ++i) {
    buf.Write(0);
  }
  buf.Write(kMarker - pad_count);

  return group_num * 9;
}

int DingoSchema<std::string>::DecodeBytesComparable(Buf& buf, std::string& data) {
  if (DINGO_UNLIKELY(buf.RestReadableSize() % kPadGroupSize != 0)) {
    return -1;
  }

  int size = 0;
  for (;;) {
    if (buf.RestReadableSize() < kPadGroupSize) {
      return -1;
    }

    uint8_t marker = buf.Read(buf.ReadOffset() + kGroupSize);

    int pad_count = kMarker - marker;
    for (int i = 0; i < kGroupSize - pad_count; ++i) {
      data.push_back(buf.Read());
    }

    size += kPadGroupSize;
    if (pad_count != 0) {
      for (int i = 0; i < pad_count; ++i) {
        if (buf.Read() != 0) {
          return -1;
        }
      }
      buf.Skip(1);  // skip marker

      break;
    }

    buf.Skip(1);  // skip marker
  }

  return size;
}

int DingoSchema<std::string>::EncodeBytesNotComparable(const std::string& data, Buf& buf) {
  buf.WriteInt(data.size());
  buf.WriteString(data);

  return data.size() + 4;
}

void DingoSchema<std::string>::DecodeBytesNotComparable(Buf& buf, std::string& data) {
  int size = buf.ReadInt();
  data.resize(size);
  for (int i = 0; i < size; ++i) {
    data[i] = buf.Read();
  }
}

int DingoSchema<std::string>::GetLength() { throw std::runtime_error("String unsupport length"); }

int DingoSchema<std::string>::SkipKey(Buf& buf) {
  if (buf.Read() == k_null) {
    return 1;
  }

  std::string data(1024, 0);
  int size = DecodeBytesComparable(buf, data);
  if (size == -1) {
    throw std::runtime_error("decode comparable string error.");
  }

  return size;
}

int DingoSchema<std::string>::SkipValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return 1;
  }

  int size = buf.ReadInt();
  buf.Skip(size);

  return size + 5;
}

// {is_null: 1byte}|{value: nbyte}
int DingoSchema<std::string>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const std::string&>(data);
    return EncodeBytesComparable(ref_data, buf) + 1;
  } else {
    buf.Write(k_null);
    return 1;
  }
}

int DingoSchema<std::string>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const std::string&>(data);
    return EncodeBytesNotComparable(ref_data, buf) + 1;
  } else {
    buf.Write(k_null);
    return 1;
  }
}

std::any DingoSchema<std::string>::DecodeKey(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  std::string data;
  int size = DecodeBytesComparable(buf, data);
  if (size == -1) {
    throw std::runtime_error("decode comparable string error.");
  }

  return std::move(std::any(std::move(data)));
}

std::any DingoSchema<std::string>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  std::string data;
  DecodeBytesNotComparable(buf, data);

  return std::move(std::any(std::move(data)));
}

}  // namespace dingodb