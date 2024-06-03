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

#include "double_list_schema.h"

#include <cstdint>
#include <utility>

#include "serial/compiler.h"

namespace dingodb {

void DingoSchema<std::vector<double>>::EncodeDoubleList(const std::vector<double>& data, Buf& buf) {
  buf.WriteInt(data.size());

  if (IsLe()) {
    for (const double& value : data) {
      uint64_t bits;
      memcpy(&bits, &value, 8);
      buf.Write(bits >> 56);
      buf.Write(bits >> 48);
      buf.Write(bits >> 40);
      buf.Write(bits >> 32);
      buf.Write(bits >> 24);
      buf.Write(bits >> 16);
      buf.Write(bits >> 8);
      buf.Write(bits);
    }
  } else {
    for (const double& value : data) {
      uint64_t bits;
      memcpy(&bits, &value, 8);
      buf.Write(bits);
      buf.Write(bits >> 8);
      buf.Write(bits >> 16);
      buf.Write(bits >> 24);
      buf.Write(bits >> 32);
      buf.Write(bits >> 40);
      buf.Write(bits >> 48);
      buf.Write(bits >> 56);
    }
  }
}

void DingoSchema<std::vector<double>>::DecodeDoubleList(Buf& buf, std::vector<double>& data) {
  int size = buf.ReadInt();
  data.resize(size);

  if (IsLe()) {
    for (int i = 0; i < size; ++i) {
      uint64_t l = 0;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);
      l <<= 8;
      l |= (static_cast<uint64_t>(buf.Read()) & 0xFF);

      void* v = &l;
      data[i] = *reinterpret_cast<double*>(v);
    }
  } else {
    for (int i = 0; i < size; ++i) {
      uint64_t l = 0;
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 0));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 1));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 2));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 3));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 4));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 5));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 6));
      l |= ((static_cast<uint64_t>(buf.Read()) & 0xFF) << (8 * 7));

      void* v = &l;
      data[i] = *reinterpret_cast<double*>(v);
    }
  }
}

int DingoSchema<std::vector<double>>::GetLength() {
  throw std::runtime_error("double list unsupport length");
  return -1;
}

int DingoSchema<std::vector<double>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

int DingoSchema<std::vector<double>>::SkipValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return 1;
  }

  int size = buf.ReadInt() * 8;
  buf.Skip(size);

  return size + 5;
}

int DingoSchema<std::vector<double>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
  return -1;
}

// {is_null: 1byte}|{n:4byte}|{value: 8byte}*n
int DingoSchema<std::vector<double>>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const std::vector<double>&>(data);
    EncodeDoubleList(ref_data, buf);

    return ref_data.size() * 8 + 5;
  } else {
    buf.Write(k_null);
    return 1;
  }
}

std::any DingoSchema<std::vector<double>>::DecodeKey(Buf&) {
  throw std::runtime_error("Unsupport encode key list type");
}

std::any DingoSchema<std::vector<double>>::DecodeValue(Buf& buf) {
  if (buf.Read() == k_null) {
    return std::any();
  }

  std::vector<double> data;
  DecodeDoubleList(buf, data);

  return std::move(std::any(std::move(data)));
}

}  // namespace dingodb