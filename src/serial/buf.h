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

#ifndef DINGO_SERIAL_BUF_H_
#define DINGO_SERIAL_BUF_H_

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "serial/compiler.h"

namespace dingodb {

// memory comparable is from low addr to high addr, so must use big endian
// little endian int32/int64 must transform to big endian(high num locate low addr)
// e.g. number: 1234567(Ox12d687)     <      2234500(0x221884)
// addr:          0     1     2             0     1     2
// little endian: 0x87  0xd6  0x12    >     0x84  0x18  0x22      compare wrong
// big endian:    0x12  0xd6  0x87    <     0x22  0x18  0x84      compare right

class Buf {
 public:
  Buf(size_t capacity, bool le);
  Buf(size_t capacity);

  Buf(const std::string& s, bool le);
  Buf(const std::string& s);

  Buf(std::string&& s, bool le);
  Buf(std::string&& s);

  ~Buf() = default;

  bool IsLe() const { return le_; }
  bool IsEnd() const { return read_offset_ == buf_.size(); }

  size_t Size() const { return buf_.size(); }
  void ReSize(size_t size) { buf_.resize(size); }

  size_t RestReadableSize() const { return buf_.size() - read_offset_; }
  size_t ReadOffset() const { return read_offset_; }
  void SetReadOffset(size_t offset) {
    if (DINGO_UNLIKELY(offset >= buf_.size())) {
      throw std::runtime_error("Out of range.");
    }
    read_offset_ = offset;
  }

  void Write(uint8_t data);
  void WriteWithNegation(uint8_t data);
  void WriteString(const std::string& data);
  void WriteInt(int32_t data);
  void WriteInt(size_t pos, int32_t data);
  void WriteLong(int64_t data);
  void WriteLongWithNegation(int64_t data);
  void WriteLongWithFirstBitNegation(int64_t data);

  uint8_t Peek();
  int32_t PeekInt();
  int64_t PeekLong();

  uint8_t Read();
  uint8_t Read(size_t pos);
  int32_t ReadInt();
  int64_t ReadLong();
  int64_t ReadLongWithFirstBitNegation();

  void Skip(size_t size);

  const std::string& GetString();
  void GetString(std::string& s);
  void GetString(std::string* s);

 private:
  bool le_{true};

  size_t read_offset_{0};

  // for memory comparable buf_ is big endian
  std::string buf_;
};

}  // namespace dingodb

#endif