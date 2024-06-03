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

#include <byteswap.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <any>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "serial/buf.h"

using namespace dingodb;

void Print(const unsigned char* addr, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    std::cout << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(*(addr + i)) << " ";
  }

  std::cout << std::endl;
}

class BufTest : public testing::Test {};

TEST_F(BufTest, CastType) {
  {
    int32_t a = -111111;
    uint32_t b = static_cast<uint32_t>(a);
    int32_t c = static_cast<int32_t>(b);
    EXPECT_EQ(a, c);
  }

  {
    int32_t a = 111111;
    uint32_t b = static_cast<uint32_t>(a);
    EXPECT_EQ(a, b);
  }

  {
    uint32_t a = 111111;
    int32_t b = static_cast<int32_t>(a);
    EXPECT_EQ(a, b);
  }
}

TEST_F(BufTest, Build) {
  {
    Buf buf(64, true);

    ASSERT_EQ(0, buf.Size());
    ASSERT_EQ(true, buf.IsLe());
  }

  {
    std::string s = "hello world";
    Buf buf(s, true);

    ASSERT_EQ(s.size(), buf.Size());
    ASSERT_EQ(true, buf.IsLe());
  }

  {
    std::string s = "hello world";
    size_t size = s.size();
    Buf buf(std::move(s), true);

    ASSERT_TRUE(s.empty());

    ASSERT_EQ(size, buf.Size());
    ASSERT_EQ(true, buf.IsLe());
  }
}

TEST_F(BufTest, WriteAndRead) {
  Buf buf(64, true);

  buf.Write(11);
  ASSERT_EQ(11, buf.Read());
  buf.WriteInt(13);
  ASSERT_EQ(13, buf.ReadInt());
  ASSERT_EQ(13, buf.Read(4));
  buf.WriteLong(15);
  ASSERT_EQ(15, buf.ReadLong());
}

TEST_F(BufTest, WriteAndPeek) {
  Buf buf(64, true);

  buf.Write(0x11);
  buf.Write(0x12);
  buf.Write(0x13);
  buf.Write(0x14);
  buf.Write(0x15);
  buf.Write(0x16);
  buf.Write(0x17);
  buf.Write(0x18);

  ASSERT_EQ(0x11, buf.Peek());
  ASSERT_EQ(0x11121314, buf.PeekInt());
  ASSERT_EQ(0x1112131415161718, buf.PeekLong());
}
