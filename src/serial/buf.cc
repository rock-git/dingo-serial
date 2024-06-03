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

#include "buf.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "serial/compiler.h"

namespace dingodb {

Buf::Buf(size_t capacity, bool le) : le_(le) { buf_.reserve(capacity); }

Buf::Buf(size_t capacity) : Buf(capacity, true) {}

Buf::Buf(const std::string& s, bool le) : le_(le) { buf_ = s; }

Buf::Buf(const std::string& s) : Buf(s, true) {}

Buf::Buf(std::string&& s, bool le) : le_(le) { buf_.swap(s); }

Buf::Buf(std::string&& s) : Buf(s, true) {}

void Buf::Write(uint8_t data) { buf_.push_back(data); }

void Buf::WriteWithNegation(uint8_t data) { buf_.push_back(~data); }

void Buf::WriteInt(int32_t data) {
  size_t curr_size = buf_.size();
  buf_.resize(curr_size + 4);

  char* buf = buf_.data();
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf[curr_size] = *(i + 3);
    buf[curr_size + 1] = *(i + 2);
    buf[curr_size + 2] = *(i + 1);
    buf[curr_size + 3] = *i;
  } else {
    buf[curr_size] = *i;
    buf[curr_size + 1] = *(i + 1);
    buf[curr_size + 2] = *(i + 2);
    buf[curr_size + 3] = *(i + 3);
  }
}

void Buf::WriteInt(size_t pos, int32_t data) {
  if (DINGO_UNLIKELY(pos + 4 >= buf_.size())) {
    throw std::runtime_error("Out of range.");
  }

  char* buf = buf_.data();
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf[pos] = *(i + 3);
    buf[pos + 1] = *(i + 2);
    buf[pos + 2] = *(i + 1);
    buf[pos + 3] = *i;
  } else {
    buf[pos] = *i;
    buf[pos + 1] = *(i + 1);
    buf[pos + 2] = *(i + 2);
    buf[pos + 3] = *(i + 3);
  }
}

void Buf::WriteLong(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf_.push_back(*(i + 7));
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 1));
    buf_.push_back(*i);
  } else {
    buf_.push_back(*i);
    buf_.push_back(*(i + 1));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 7));
  }
}

void Buf::WriteLongWithNegation(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf_.push_back(~*(i + 7));
    buf_.push_back(~*(i + 6));
    buf_.push_back(~*(i + 5));
    buf_.push_back(~*(i + 4));
    buf_.push_back(~*(i + 3));
    buf_.push_back(~*(i + 2));
    buf_.push_back(~*(i + 1));
    buf_.push_back(~*i);

  } else {
    buf_.push_back(~*i);
    buf_.push_back(~*(i + 1));
    buf_.push_back(~*(i + 2));
    buf_.push_back(~*(i + 3));
    buf_.push_back(~*(i + 4));
    buf_.push_back(~*(i + 5));
    buf_.push_back(~*(i + 6));
    buf_.push_back(~*(i + 7));
  }
}

void Buf::WriteLongWithFirstBitNegation(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf_.push_back(*(i + 7) ^ 0x80);
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 1));
    buf_.push_back(*i);

  } else {
    buf_.push_back(*i ^ 0x80);
    buf_.push_back(*(i + 1));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 7));
  }
}

void Buf::WriteString(const std::string& data) {
  size_t curr_size = buf_.size();
  buf_.resize(curr_size + data.size());

  memcpy(buf_.data() + curr_size, data.data(), data.size());
}

uint8_t Buf::Peek() { return buf_.at(read_offset_); }

int32_t Buf::PeekInt() {
  if (DINGO_LIKELY(this->le_)) {
    char* buf = buf_.data();
    return ((buf[read_offset_] & 0xFF) << 24) | ((buf[read_offset_ + 1] & 0xFF) << 16) |
           ((buf[read_offset_ + 2] & 0xFF) << 8) | (buf[read_offset_ + 3] & 0xFF);
  } else {
    char* buf = buf_.data();
    return ((buf[read_offset_] & 0xFF) | ((buf[read_offset_ + 1] & 0xFF) << 8) |
            ((buf[read_offset_ + 2] & 0xFF) << 16) | ((buf[read_offset_ + 3] & 0xFF) << 24));
  }
}

int64_t Buf::PeekLong() {
  char* buf = buf_.data();
  uint64_t l = 0;
  if (DINGO_LIKELY(this->le_)) {
    l |= buf[read_offset_];
    l <<= 8;
    l |= buf[read_offset_ + 1];
    l <<= 8;
    l |= buf[read_offset_ + 2];
    l <<= 8;
    l |= buf[read_offset_ + 3];
    l <<= 8;
    l |= buf[read_offset_ + 4];
    l <<= 8;
    l |= buf[read_offset_ + 5];
    l <<= 8;
    l |= buf[read_offset_ + 6];
    l <<= 8;
    l |= buf[read_offset_ + 7];

  } else {
    l |= buf[read_offset_ + 7];
    l <<= 8;
    l |= buf[read_offset_ + 6];
    l <<= 8;
    l |= buf[read_offset_ + 5];
    l <<= 8;
    l |= buf[read_offset_ + 4];
    l <<= 8;
    l |= buf[read_offset_ + 3];
    l <<= 8;
    l |= buf[read_offset_ + 2];
    l <<= 8;
    l |= buf[read_offset_ + 1];
    l <<= 8;
    l |= buf[read_offset_];
  }
  return l;
}

uint8_t Buf::Read() { return buf_.at(read_offset_++); }

uint8_t Buf::Read(size_t pos) { return buf_.at(pos); }

int32_t Buf::ReadInt() {
  if (DINGO_LIKELY(this->le_)) {
    return ((Read() & 0xFF) << 24) | ((Read() & 0xFF) << 16) | ((Read() & 0xFF) << 8) | (Read() & 0xFF);
  } else {
    return (Read() & 0xFF) | ((Read() & 0xFF) << 8) | ((Read() & 0xFF) << 16) | ((Read() & 0xFF) << 24);
  }
}

int64_t Buf::ReadLong() {
  uint64_t l = Read() & 0xFF;
  if (DINGO_LIKELY(this->le_)) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)Read() & 0xFF) << (8 * i));
    }
  }
  return l;
}

int64_t Buf::ReadLongWithFirstBitNegation() {
  uint64_t l = (Read() & 0xFF) ^ 0x80;
  if (IsLe()) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)Read() & 0xFF) << (8 * i));
    }
  }

  return l;
}

void Buf::Skip(size_t size) {
  if (DINGO_UNLIKELY(read_offset_ + size > buf_.size())) {
    throw std::runtime_error("Out of range.");
  }

  read_offset_ += size;
}

const std::string& Buf::GetString() { return buf_; }

void Buf::GetString(std::string& s) { s.swap(buf_); }

void Buf::GetString(std::string* s) { s->swap(buf_); }

}  // namespace dingodb