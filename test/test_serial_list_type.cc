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
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/schema/base_schema.h"
#include "serial/utils.h"

using namespace dingodb;

class DingoSerialListTypeTest : public testing::Test {
 public:
  void InitVector() {
    auto id = std::make_shared<DingoSchema<int32_t>>();
    id->SetIndex(0);
    id->SetAllowNull(false);
    id->SetIsKey(true);
    schemas_.push_back(id);

    auto name = std::make_shared<DingoSchema<std::string>>();
    name->SetIndex(1);
    name->SetAllowNull(false);
    name->SetIsKey(true);
    schemas_.push_back(name);

    auto gender = std::make_shared<DingoSchema<std::string>>();
    gender->SetIndex(2);
    gender->SetAllowNull(false);
    gender->SetIsKey(true);
    schemas_.push_back(gender);

    auto score = std::make_shared<DingoSchema<int64_t>>();
    score->SetIndex(3);
    score->SetAllowNull(false);
    score->SetIsKey(true);
    schemas_.push_back(score);

    auto addr = std::make_shared<DingoSchema<std::string>>();
    addr->SetIndex(4);
    addr->SetAllowNull(true);
    addr->SetIsKey(false);
    schemas_.push_back(addr);

    auto exist = std::make_shared<DingoSchema<bool>>();
    exist->SetIndex(5);
    exist->SetAllowNull(false);
    exist->SetIsKey(false);
    schemas_.push_back(exist);

    auto pic = std::make_shared<DingoSchema<std::string>>();
    pic->SetIndex(6);
    pic->SetAllowNull(true);
    pic->SetIsKey(false);
    schemas_.push_back(pic);

    auto test_null = std::make_shared<DingoSchema<int32_t>>();
    test_null->SetIndex(7);
    test_null->SetAllowNull(true);
    test_null->SetIsKey(false);
    schemas_.push_back(test_null);

    auto age = std::make_shared<DingoSchema<int32_t>>();
    age->SetIndex(8);
    age->SetAllowNull(false);
    age->SetIsKey(false);
    schemas_.push_back(age);

    auto prev = std::make_shared<DingoSchema<int64_t>>();
    prev->SetIndex(9);
    prev->SetAllowNull(false);
    prev->SetIsKey(false);
    schemas_.push_back(prev);

    auto salary = std::make_shared<DingoSchema<double>>();
    salary->SetIndex(10);
    salary->SetAllowNull(true);
    salary->SetIsKey(false);
    schemas_.push_back(salary);

    auto boollist_null = std::make_shared<DingoSchema<std::vector<bool>>>();
    boollist_null->SetIndex(11);
    boollist_null->SetAllowNull(true);
    boollist_null->SetIsKey(false);
    schemas_.push_back(boollist_null);

    auto boollist = std::make_shared<DingoSchema<std::vector<bool>>>();
    boollist->SetIndex(12);
    boollist->SetAllowNull(false);
    boollist->SetIsKey(false);
    schemas_.push_back(boollist);

    auto stringlist_null = std::make_shared<DingoSchema<std::vector<std::string>>>();
    stringlist_null->SetIndex(13);
    stringlist_null->SetAllowNull(true);
    stringlist_null->SetIsKey(false);
    schemas_.push_back(stringlist_null);

    auto stringlist = std::make_shared<DingoSchema<std::vector<std::string>>>();
    stringlist->SetIndex(14);
    stringlist->SetAllowNull(false);
    stringlist->SetIsKey(false);
    schemas_.push_back(stringlist);

    auto doublelist_null = std::make_shared<DingoSchema<std::vector<double>>>();
    doublelist_null->SetIndex(15);
    doublelist_null->SetAllowNull(true);
    doublelist_null->SetIsKey(false);
    schemas_.push_back(doublelist_null);

    auto doublelist = std::make_shared<DingoSchema<std::vector<double>>>();
    doublelist->SetIndex(16);
    doublelist->SetAllowNull(false);
    doublelist->SetIsKey(false);
    schemas_.push_back(doublelist);

    auto floatlist_null = std::make_shared<DingoSchema<std::vector<float>>>();
    floatlist_null->SetIndex(17);
    floatlist_null->SetAllowNull(true);
    floatlist_null->SetIsKey(false);
    schemas_.push_back(floatlist_null);

    auto floatlist = std::make_shared<DingoSchema<std::vector<float>>>();
    floatlist->SetIndex(18);
    floatlist->SetAllowNull(false);
    floatlist->SetIsKey(false);
    schemas_.push_back(floatlist);

    auto integerlist_null = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    integerlist_null->SetIndex(19);
    integerlist_null->SetAllowNull(true);
    integerlist_null->SetIsKey(false);
    schemas_.push_back(integerlist_null);

    auto integerlist = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    integerlist->SetIndex(20);
    integerlist->SetAllowNull(false);
    integerlist->SetIsKey(false);
    schemas_.push_back(integerlist);

    auto longlist_null = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    longlist_null->SetIndex(21);
    longlist_null->SetAllowNull(true);
    longlist_null->SetIsKey(false);
    schemas_.push_back(longlist_null);

    auto longlist = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    longlist->SetIndex(22);
    longlist->SetAllowNull(false);
    longlist->SetIsKey(false);
    schemas_.push_back(longlist);

    auto stringlist_null1 = std::make_shared<DingoSchema<std::vector<std::string>>>();
    stringlist_null1->SetIndex(23);
    stringlist_null1->SetAllowNull(true);
    stringlist_null1->SetIsKey(false);
    schemas_.push_back(stringlist_null1);

    auto stringlist1 = std::make_shared<DingoSchema<std::vector<std::string>>>();
    stringlist1->SetIndex(24);
    stringlist1->SetAllowNull(false);
    stringlist1->SetIsKey(false);
    schemas_.push_back(stringlist1);
  }

  void InitRecord() {
    int32_t id = 0;
    std::string name = "tn";
    std::string gender = "f";
    int64_t score = 214748364700L;
    std::string addr =
        "test address test 中文 表情😊🏷️👌 test "
        "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试"
        "测"
        "试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫";
    bool exist = false;
    int32_t age = -20;
    int64_t prev = -214748364700L;
    double salary = 873485.4234;

    std::vector<bool> bool1 = {};
    std::vector<bool> bool2 = {true, false, false, false, true};

    std::vector<std::string> string1 = {};
    std::vector<std::string> string2 = {"qwe", "中文"};

    std::vector<double> double1 = {};

    std::vector<double> double2 = {3323232333.21221, 23232.2111111, 222334455566.23, 2222, 67889.246};

    std::vector<float> float1 = {};
    std::vector<float> float2 = {33232.21221, 23232.2111111, 2222, 67889.246};

    std::vector<int32_t> int1 = {};
    std::vector<int32_t> int2 = {33232, 2111111, 2222, 246};

    std::vector<int64_t> long1 = {};
    std::vector<int64_t> long2 = {33232998776555l, 2111111l, 2222l, 2469999883732l};

    std::vector<std::string> string3 = {
        "测试测试测试三🤣😂😁🐱‍🐉👏🐱‍💻✔🤳🤦‍♂️🤦‍♀️🙌测试测试",
        "试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫"};
    std::vector<std::string> string4 = {"test address test 中文 表情😊🏷️👌 test ", "中文"};

    record_.resize(25);
    record_[0] = std::any(id);
    record_[1] = std::any(name);
    record_[2] = std::any(gender);
    record_[3] = std::any(score);
    record_[4] = std::any(addr);
    record_[5] = std::any(exist);
    record_[6] = std::any();
    record_[7] = std::any();
    record_[8] = std::any(age);
    record_[9] = std::any(prev);
    record_[10] = std::any(salary);
    record_[11] = std::any(bool1);
    record_[12] = std::any(bool2);
    record_[13] = std::any(string1);
    record_[14] = std::any(string2);
    record_[15] = std::any(double1);
    record_[16] = std::any(double2);
    record_[17] = std::any(float1);
    record_[18] = std::any(float2);
    record_[19] = std::any(int1);
    record_[20] = std::any(int2);
    record_[21] = std::any(long1);
    record_[22] = std::any(long2);
    record_[23] = std::any(string3);
    record_[24] = std::any(string4);
  }

  void DeleteRecords() { record_.clear(); }

  const std::vector<BaseSchemaPtr>& GetSchemas() const { return schemas_; }
  const std::vector<std::any>& GetRecord() const { return record_; }

 protected:
  bool le = IsLE();
  void SetUp() override {}
  void TearDown() override {}

 private:
  std::vector<BaseSchemaPtr> schemas_;
  std::vector<std::any> record_;
};

TEST_F(DingoSerialListTypeTest, boolListSchema) {
  DingoSchema<std::vector<bool>> schema;
  schema.SetIndex(0);
  schema.SetAllowNull(false);
  schema.SetIsKey(false);

  std::vector<bool> data1 = {false, true};
  Buf bf1(2 + 4, this->le);
  schema.EncodeValue(data1, bf1);

  std::string bs1 = bf1.GetString();
  Buf bf2(bs1, this->le);

  auto data2 = schema.DecodeValue(bf2);
  ASSERT_TRUE(data2.has_value());

  std::vector<bool> vector2 = std::any_cast<std::vector<bool>>(data2);
  EXPECT_EQ(data1.size(), vector2.size());

  for (size_t i = 0; i < data1.size(); ++i) {
    EXPECT_EQ(data1[i], vector2[i]);
  }

  {
    DingoSchema<bool> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    bool value = true;
    Buf encode_buf(1, this->le);
    schema.EncodeValue(value, encode_buf);

    std::string bs = encode_buf.GetString();
    Buf decode_buf(bs, this->le);
    auto data4 = schema.DecodeValue(decode_buf);
    ASSERT_TRUE(data4.has_value());

    auto origin_value = std::any_cast<bool>(data4);
    EXPECT_EQ(value, origin_value);

    std::any data5;
    Buf bf5(1, this->le);
    schema.EncodeValue(data5, bf5);
    std::string bs3 = bf5.GetString();
    Buf bf6(bs3, this->le);
    auto data6 = schema.DecodeValue(bf6);
    EXPECT_FALSE(data6.has_value());
  }

  {
    DingoSchema<bool> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(true);
    Buf bf7(100, this->le);
    schema.EncodeValue(std::any(), bf7);
    std::string bs4 = bf7.GetString();
    Buf bf8(bs4, this->le);
    EXPECT_FALSE(schema.DecodeKey(bf8).has_value());
  }
}

TEST_F(DingoSerialListTypeTest, integerSchema) {
  {
    DingoSchema<int32_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(false);
    schema.SetIsKey(true);

    int32_t origin_data = 1543234;
    Buf encode_buf(1, this->le);
    schema.EncodeKey(origin_data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);

    auto decode_data = schema.DecodeKey(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(origin_data, std::any_cast<int32_t>(decode_data));
  }

  {
    DingoSchema<int32_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    int32_t origin_data = 532142;
    Buf encode_buf(1, this->le);
    schema.EncodeValue(origin_data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);

    auto decode_data = schema.DecodeValue(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(origin_data, std::any_cast<int32_t>(decode_data));
  }

  {
    DingoSchema<int32_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);

    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_FALSE(decode_data.has_value());
  }

  {
    DingoSchema<int32_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(true);
    Buf encode_buf(100, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    EXPECT_FALSE(schema.DecodeKey(decode_buf).has_value());
  }
}

TEST_F(DingoSerialListTypeTest, integerSchemaLeBe) {
  uint32_t data = 1543234;
  // bitset<32> key_data("10000000000101111000110001000010");
  std::bitset<8> key_data_0("10000000");
  std::bitset<8> key_data_1("00010111");
  std::bitset<8> key_data_2("10001100");
  std::bitset<8> key_data_3("01000010");
  // bitset<32> value_data("00000000000101111000110001000010");
  std::bitset<8> value_data_0("00000000");
  std::bitset<8> value_data_1("00010111");
  std::bitset<8> value_data_2("10001100");
  std::bitset<8> value_data_3("01000010");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<int32_t> schema;
  schema.SetIndex(0);
  int32_t data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    Buf decode_buf(bs1, this->le);

    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<int32_t>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data1, encode_buf);

    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    Buf decode_buf(bs2, this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<int32_t>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, integerSchemaFakeLeBe) {
  uint32_t data = bswap_32(1543234);
  // bitset<32> key_data("10000000000101111000110001000010");
  std::bitset<8> key_data_0("10000000");
  std::bitset<8> key_data_1("00010111");
  std::bitset<8> key_data_2("10001100");
  std::bitset<8> key_data_3("01000010");
  // bitset<32> value_data("00000000000101111000110001000010");
  std::bitset<8> value_data_0("00000000");
  std::bitset<8> value_data_1("00010111");
  std::bitset<8> value_data_2("10001100");
  std::bitset<8> value_data_3("01000010");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<int32_t> schema;
  schema.SetIndex(0);
  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (!this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }

  int32_t data1 = data;

  {
    Buf encode_buf(1, !this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    Buf decode_buf(bs1, !this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data1, std::any_cast<int32_t>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, !this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    Buf decode_buf(bs2, !this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data1, std::any_cast<int32_t>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, floatSchemaLeBe) {
  float data = -43225.23;
  // bitset<32> key_data("00111000110101110010011011000100");
  std::bitset<8> key_data_0("00111000");
  std::bitset<8> key_data_1("11010111");
  std::bitset<8> key_data_2("00100110");
  std::bitset<8> key_data_3("11000100");
  // bitset<32> value_data("11000111001010001101100100111011");
  std::bitset<8> value_data_0("11000111");
  std::bitset<8> value_data_1("00101000");
  std::bitset<8> value_data_2("11011001");
  std::bitset<8> value_data_3("00111011");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<float> schema;
  schema.SetIndex(0);
  float data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }

  {
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    Buf decode_buf(bs1, this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<float>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    Buf decode_buf(bs2, this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<float>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, floatSchemaFakeLeBe) {
  float ori_data = -43225.53;
  uint32_t ori_data_bits;
  memcpy(&ori_data_bits, &ori_data, 4);
  uint32_t data_bits = bswap_32(ori_data_bits);
  float data;
  memcpy(&data, &data_bits, 4);
  // bitset<32> key_data("00111000110101110010011011000100");
  std::bitset<8> key_data_0("00111000");
  std::bitset<8> key_data_1("11010111");
  std::bitset<8> key_data_2("00100110");
  std::bitset<8> key_data_3("01110111");
  // bitset<32> value_data("11000111001010001101100110001000");
  std::bitset<8> value_data_0("11000111");
  std::bitset<8> value_data_1("00101000");
  std::bitset<8> value_data_2("11011001");
  std::bitset<8> value_data_3("10001000");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<float> schema;
  schema.SetIndex(0);
  float data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (!this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }

  {
    Buf encode_buf(1, !this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    Buf decode_buf(bs1, !this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<float>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, !this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    Buf decode_buf(bs2, !this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<float>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, longSchemaLeBe) {
  uint64_t data = 8237583920453957801;
  // bitset<64> key_data("1111001001010001110001101110111001011010001000001011100010101001");
  std::bitset<8> key_data_0("11110010");
  std::bitset<8> key_data_1("01010001");
  std::bitset<8> key_data_2("11000110");
  std::bitset<8> key_data_3("11101110");
  std::bitset<8> key_data_4("01011010");
  std::bitset<8> key_data_5("00100000");
  std::bitset<8> key_data_6("10111000");
  std::bitset<8> key_data_7("10101001");
  // bitset<64> value_data("0111001001010001110001101110111001011010001000001011100010101001");
  std::bitset<8> value_data_0("01110010");
  std::bitset<8> value_data_1("01010001");
  std::bitset<8> value_data_2("11000110");
  std::bitset<8> value_data_3("11101110");
  std::bitset<8> value_data_4("01011010");
  std::bitset<8> value_data_5("00100000");
  std::bitset<8> value_data_6("10111000");
  std::bitset<8> value_data_7("10101001");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<int64_t> schema;
  schema.SetIndex(0);
  int64_t data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    std::bitset<8> bs15(bs1.at(5));
    EXPECT_EQ(bs15, key_data_4);
    std::bitset<8> bs16(bs1.at(6));
    EXPECT_EQ(bs16, key_data_5);
    std::bitset<8> bs17(bs1.at(7));
    EXPECT_EQ(bs17, key_data_6);
    std::bitset<8> bs18(bs1.at(8));
    EXPECT_EQ(bs18, key_data_7);
    Buf decode_buf(bs1, this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<int64_t>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    std::bitset<8> bs25(bs2.at(5));
    EXPECT_EQ(bs25, value_data_4);
    std::bitset<8> bs26(bs2.at(6));
    EXPECT_EQ(bs26, value_data_5);
    std::bitset<8> bs27(bs2.at(7));
    EXPECT_EQ(bs27, value_data_6);
    std::bitset<8> bs28(bs2.at(8));
    EXPECT_EQ(bs28, value_data_7);
    Buf decode_buf(bs2, this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<int64_t>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, longSchemaFakeLeBe) {
  uint64_t data = bswap_64(8237583920453957801);
  // bitset<64> key_data("1111001001010001110001101110111001011010001000001011100010101001");
  std::bitset<8> key_data_0("11110010");
  std::bitset<8> key_data_1("01010001");
  std::bitset<8> key_data_2("11000110");
  std::bitset<8> key_data_3("11101110");
  std::bitset<8> key_data_4("01011010");
  std::bitset<8> key_data_5("00100000");
  std::bitset<8> key_data_6("10111000");
  std::bitset<8> key_data_7("10101001");
  // bitset<64> value_data("0111001001010001110001101110111001011010001000001011100010101001");
  std::bitset<8> value_data_0("01110010");
  std::bitset<8> value_data_1("01010001");
  std::bitset<8> value_data_2("11000110");
  std::bitset<8> value_data_3("11101110");
  std::bitset<8> value_data_4("01011010");
  std::bitset<8> value_data_5("00100000");
  std::bitset<8> value_data_6("10111000");
  std::bitset<8> value_data_7("10101001");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<int64_t> schema;
  schema.SetIndex(0);
  int64_t data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (!this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, !this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    std::bitset<8> bs15(bs1.at(5));
    EXPECT_EQ(bs15, key_data_4);
    std::bitset<8> bs16(bs1.at(6));
    EXPECT_EQ(bs16, key_data_5);
    std::bitset<8> bs17(bs1.at(7));
    EXPECT_EQ(bs17, key_data_6);
    std::bitset<8> bs18(bs1.at(8));
    EXPECT_EQ(bs18, key_data_7);
    Buf decode_buf(bs1, !this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<int64_t>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, !this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    std::bitset<8> bs25(bs2.at(5));
    EXPECT_EQ(bs25, value_data_4);
    std::bitset<8> bs26(bs2.at(6));
    EXPECT_EQ(bs26, value_data_5);
    std::bitset<8> bs27(bs2.at(7));
    EXPECT_EQ(bs27, value_data_6);
    std::bitset<8> bs28(bs2.at(8));
    EXPECT_EQ(bs28, value_data_7);
    Buf decode_buf(bs2, !this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<int64_t>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, doubleSchemaPosLeBe) {
  double data = 345235.32656;
  // bitset<64> key_data("1100000100010101000100100100110101001110011001011011111010100001");
  std::bitset<8> key_data_0("11000001");
  std::bitset<8> key_data_1("00010101");
  std::bitset<8> key_data_2("00010010");
  std::bitset<8> key_data_3("01001101");
  std::bitset<8> key_data_4("01001110");
  std::bitset<8> key_data_5("01100101");
  std::bitset<8> key_data_6("10111110");
  std::bitset<8> key_data_7("10100001");
  // bitset<64> value_data("0100000100010101000100100100110101001110011001011011111010100001");
  std::bitset<8> value_data_0("01000001");
  std::bitset<8> value_data_1("00010101");
  std::bitset<8> value_data_2("00010010");
  std::bitset<8> value_data_3("01001101");
  std::bitset<8> value_data_4("01001110");
  std::bitset<8> value_data_5("01100101");
  std::bitset<8> value_data_6("10111110");
  std::bitset<8> value_data_7("10100001");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<double> schema;
  schema.SetIndex(0);
  double data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (IsLE()) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    std::bitset<8> bs15(bs1.at(5));
    EXPECT_EQ(bs15, key_data_4);
    std::bitset<8> bs16(bs1.at(6));
    EXPECT_EQ(bs16, key_data_5);
    std::bitset<8> bs17(bs1.at(7));
    EXPECT_EQ(bs17, key_data_6);
    std::bitset<8> bs18(bs1.at(8));
    EXPECT_EQ(bs18, key_data_7);
    Buf decode_buf(bs1, this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    std::bitset<8> bs25(bs2.at(5));
    EXPECT_EQ(bs25, value_data_4);
    std::bitset<8> bs26(bs2.at(6));
    EXPECT_EQ(bs26, value_data_5);
    std::bitset<8> bs27(bs2.at(7));
    EXPECT_EQ(bs27, value_data_6);
    std::bitset<8> bs28(bs2.at(8));
    EXPECT_EQ(bs28, value_data_7);
    Buf decode_buf(bs2, this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, doubleSchemaPosFakeLeBe) {
  double ori_data = 345235.3265;
  uint64_t ori_data_bits;
  memcpy(&ori_data_bits, &ori_data, 8);
  uint64_t data_bits = bswap_64(ori_data_bits);
  double data;
  memcpy(&data, &data_bits, 8);
  // bitset<64> key_data("1100000100010101000100100100110101001110010101100000010000011001");
  std::bitset<8> key_data_0("11000001");
  std::bitset<8> key_data_1("00010101");
  std::bitset<8> key_data_2("00010010");
  std::bitset<8> key_data_3("01001101");
  std::bitset<8> key_data_4("01001110");
  std::bitset<8> key_data_5("01010110");
  std::bitset<8> key_data_6("00000100");
  std::bitset<8> key_data_7("00011001");
  // bitset<64> value_data("0100000100010101000100100100110101001110010101100000010000011001");
  std::bitset<8> value_data_0("01000001");
  std::bitset<8> value_data_1("00010101");
  std::bitset<8> value_data_2("00010010");
  std::bitset<8> value_data_3("01001101");
  std::bitset<8> value_data_4("01001110");
  std::bitset<8> value_data_5("01010110");
  std::bitset<8> value_data_6("00000100");
  std::bitset<8> value_data_7("00011001");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<double> schema;
  schema.SetIndex(0);
  double data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (!this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, !this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    std::bitset<8> bs15(bs1.at(5));
    EXPECT_EQ(bs15, key_data_4);
    std::bitset<8> bs16(bs1.at(6));
    EXPECT_EQ(bs16, key_data_5);
    std::bitset<8> bs17(bs1.at(7));
    EXPECT_EQ(bs17, key_data_6);
    std::bitset<8> bs18(bs1.at(8));
    EXPECT_EQ(bs18, key_data_7);
    Buf decode_buf(bs1, !this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, !this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    std::bitset<8> bs25(bs2.at(5));
    EXPECT_EQ(bs25, value_data_4);
    std::bitset<8> bs26(bs2.at(6));
    EXPECT_EQ(bs26, value_data_5);
    std::bitset<8> bs27(bs2.at(7));
    EXPECT_EQ(bs27, value_data_6);
    std::bitset<8> bs28(bs2.at(8));
    EXPECT_EQ(bs28, value_data_7);
    Buf decode_buf(bs2, !this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, doubleSchemaNegLeBe) {
  double data = -345235.32656;
  // bitset<64> key_data("0011111011101010111011011011001010110001100110100100000101011110");
  std::bitset<8> key_data_0("00111110");
  std::bitset<8> key_data_1("11101010");
  std::bitset<8> key_data_2("11101101");
  std::bitset<8> key_data_3("10110010");
  std::bitset<8> key_data_4("10110001");
  std::bitset<8> key_data_5("10011010");
  std::bitset<8> key_data_6("01000001");
  std::bitset<8> key_data_7("01011110");
  // bitset<64> value_data("1100000100010101000100100100110101001110011001011011111010100001");
  std::bitset<8> value_data_0("11000001");
  std::bitset<8> value_data_1("00010101");
  std::bitset<8> value_data_2("00010010");
  std::bitset<8> value_data_3("01001101");
  std::bitset<8> value_data_4("01001110");
  std::bitset<8> value_data_5("01100101");
  std::bitset<8> value_data_6("10111110");
  std::bitset<8> value_data_7("10100001");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<double> schema;
  schema.SetIndex(0);
  double data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    std::bitset<8> bs15(bs1.at(5));
    EXPECT_EQ(bs15, key_data_4);
    std::bitset<8> bs16(bs1.at(6));
    EXPECT_EQ(bs16, key_data_5);
    std::bitset<8> bs17(bs1.at(7));
    EXPECT_EQ(bs17, key_data_6);
    std::bitset<8> bs18(bs1.at(8));
    EXPECT_EQ(bs18, key_data_7);
    Buf decode_buf(bs1, this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    std::bitset<8> bs25(bs2.at(5));
    EXPECT_EQ(bs25, value_data_4);
    std::bitset<8> bs26(bs2.at(6));
    EXPECT_EQ(bs26, value_data_5);
    std::bitset<8> bs27(bs2.at(7));
    EXPECT_EQ(bs27, value_data_6);
    std::bitset<8> bs28(bs2.at(8));
    EXPECT_EQ(bs28, value_data_7);
    Buf decode_buf(bs2, this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, doubleSchemaNegFakeLeBe) {
  double ori_data = -345235.32656;
  uint64_t ori_data_bits;
  memcpy(&ori_data_bits, &ori_data, 8);
  uint64_t data_bits = bswap_64(ori_data_bits);
  double data;
  memcpy(&data, &data_bits, 8);
  // bitset<64> key_data("0011111011101010111011011011001010110001100110100100000101011110");
  std::bitset<8> key_data_0("00111110");
  std::bitset<8> key_data_1("11101010");
  std::bitset<8> key_data_2("11101101");
  std::bitset<8> key_data_3("10110010");
  std::bitset<8> key_data_4("10110001");
  std::bitset<8> key_data_5("10011010");
  std::bitset<8> key_data_6("01000001");
  std::bitset<8> key_data_7("01011110");
  // bitset<64> value_data("1100000100010101000100100100110101001110011001011011111010100001");
  std::bitset<8> value_data_0("11000001");
  std::bitset<8> value_data_1("00010101");
  std::bitset<8> value_data_2("00010010");
  std::bitset<8> value_data_3("01001101");
  std::bitset<8> value_data_4("01001110");
  std::bitset<8> value_data_5("01100101");
  std::bitset<8> value_data_6("10111110");
  std::bitset<8> value_data_7("10100001");
  std::bitset<8> not_null_tag("00000001");

  DingoSchema<double> schema;
  schema.SetIndex(0);
  double data1 = data;

  schema.SetAllowNull(true);
  schema.SetIsKey(true);
  if (!this->le) {
    schema.SetIsLe(true);
  } else {
    schema.SetIsLe(false);
  }
  {
    Buf encode_buf(1, !this->le);
    schema.EncodeKey(data1, encode_buf);
    std::string bs1 = encode_buf.GetString();
    std::bitset<8> bs10(bs1.at(0));
    EXPECT_EQ(bs10, not_null_tag);
    std::bitset<8> bs11(bs1.at(1));
    EXPECT_EQ(bs11, key_data_0);
    std::bitset<8> bs12(bs1.at(2));
    EXPECT_EQ(bs12, key_data_1);
    std::bitset<8> bs13(bs1.at(3));
    EXPECT_EQ(bs13, key_data_2);
    std::bitset<8> bs14(bs1.at(4));
    EXPECT_EQ(bs14, key_data_3);
    std::bitset<8> bs15(bs1.at(5));
    EXPECT_EQ(bs15, key_data_4);
    std::bitset<8> bs16(bs1.at(6));
    EXPECT_EQ(bs16, key_data_5);
    std::bitset<8> bs17(bs1.at(7));
    EXPECT_EQ(bs17, key_data_6);
    std::bitset<8> bs18(bs1.at(8));
    EXPECT_EQ(bs18, key_data_7);
    Buf decode_buf(bs1, !this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }

  {
    schema.SetIsKey(false);
    Buf encode_buf(1, !this->le);
    schema.EncodeValue(data1, encode_buf);
    std::string bs2 = encode_buf.GetString();
    std::bitset<8> bs20(bs2.at(0));
    EXPECT_EQ(bs20, not_null_tag);
    std::bitset<8> bs21(bs2.at(1));
    EXPECT_EQ(bs21, value_data_0);
    std::bitset<8> bs22(bs2.at(2));
    EXPECT_EQ(bs22, value_data_1);
    std::bitset<8> bs23(bs2.at(3));
    EXPECT_EQ(bs23, value_data_2);
    std::bitset<8> bs24(bs2.at(4));
    EXPECT_EQ(bs24, value_data_3);
    std::bitset<8> bs25(bs2.at(5));
    EXPECT_EQ(bs25, value_data_4);
    std::bitset<8> bs26(bs2.at(6));
    EXPECT_EQ(bs26, value_data_5);
    std::bitset<8> bs27(bs2.at(7));
    EXPECT_EQ(bs27, value_data_6);
    std::bitset<8> bs28(bs2.at(8));
    EXPECT_EQ(bs28, value_data_7);
    Buf decode_buf(bs2, !this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_EQ(data1, std::any_cast<double>(decode_data));
  }
}

TEST_F(DingoSerialListTypeTest, longSchema) {
  {
    DingoSchema<int64_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(false);
    schema.SetIsKey(true);

    int64_t data1 = 1543234;
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data1, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data1, std::any_cast<int64_t>(decode_data));
  }

  {
    DingoSchema<int64_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    int64_t data = 532142;
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data, std::any_cast<int64_t>(decode_data));
  }

  {
    DingoSchema<int64_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_FALSE(decode_data.has_value());
  }

  {
    DingoSchema<int64_t> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(true);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    EXPECT_FALSE(schema.DecodeKey(decode_buf).has_value());
  }
}

TEST_F(DingoSerialListTypeTest, doubleSchema) {
  {
    DingoSchema<double> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(false);
    schema.SetIsKey(true);

    double data = 154.3234;
    Buf encode_buf(1, this->le);
    schema.EncodeKey(data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data, std::any_cast<double>(decode_data));
  }

  {
    DingoSchema<double> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);
    double data = 5321.42;
    Buf encode_buf(1, this->le);
    schema.EncodeValue(data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data, std::any_cast<double>(decode_data));
  }

  {
    DingoSchema<double> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    EXPECT_FALSE(decode_data.has_value());
  }

  {
    DingoSchema<double> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(true);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    EXPECT_FALSE(schema.DecodeKey(decode_buf).has_value());
  }
}

TEST_F(DingoSerialListTypeTest, stringSchema) {
  {
    DingoSchema<std::string> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(false);
    schema.SetIsKey(true);
    Buf encode_buf(1, this->le);

    std::string data =
        "test address test 中文 表情😊🏷️👌 test "
        "测试测试测试三🤣😂😁🐱‍🐉👏";

    schema.EncodeKey(data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeKey(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data, std::any_cast<std::string>(decode_data));
  }

  {
    DingoSchema<std::string> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);
    std::string data =
        "test address test 中文 表情😊🏷️👌 test "
        "测试测试测试三🤣😂😁🐱‍🐉👏";

    Buf encode_buf(1, this->le);
    schema.EncodeValue(data, encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeValue(decode_buf);
    ASSERT_TRUE(decode_data.has_value());
    EXPECT_EQ(data, std::any_cast<std::string>(decode_data));
  }

  {
    DingoSchema<std::string> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(false);

    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);

    auto decode_data = schema.DecodeValue(decode_buf);

    EXPECT_FALSE(decode_data.has_value());
  }

  {
    DingoSchema<std::string> schema;
    schema.SetIndex(0);
    schema.SetAllowNull(true);
    schema.SetIsKey(true);
    Buf encode_buf(1, this->le);
    schema.EncodeValue(std::any(), encode_buf);
    Buf decode_buf(encode_buf.GetString(), this->le);
    auto decode_data = schema.DecodeKey(decode_buf);

    EXPECT_FALSE(decode_data.has_value());
  }
}

TEST_F(DingoSerialListTypeTest, bufLeBe) {
  uint32_t int_data = 1543234;
  uint64_t long_data = -8237583920453957801;
  uint32_t int_atad = bswap_32(1543234);
  uint64_t long_atad = bswap_64(-8237583920453957801);

  std::bitset<8> bit0("00000000");
  std::bitset<8> bit1("00010111");
  std::bitset<8> bit2("10001100");
  std::bitset<8> bit3("01000010");
  std::bitset<8> bit4("10001101");
  std::bitset<8> bit5("10101110");
  std::bitset<8> bit6("00111001");
  std::bitset<8> bit7("00010001");
  std::bitset<8> bit8("10100101");
  std::bitset<8> bit9("11011111");
  std::bitset<8> bit10("01000111");
  std::bitset<8> bit11("01010111");
  std::bitset<8> bit12("01000010");
  std::bitset<8> bit13("10001100");
  std::bitset<8> bit14("00010111");
  std::bitset<8> bit15("00000000");

  Buf bf1(16, this->le);
  bf1.WriteInt(int_data);
  bf1.WriteLong(long_data);
  //  bf1.ReverseWriteInt(int_data);
  std::string bs1 = bf1.GetString();
  std::bitset<8> bs10(bs1.at(0));
  EXPECT_EQ(bs10, bit0);
  std::bitset<8> bs11(bs1.at(1));
  EXPECT_EQ(bs11, bit1);
  std::bitset<8> bs12(bs1.at(2));
  EXPECT_EQ(bs12, bit2);
  std::bitset<8> bs13(bs1.at(3));
  EXPECT_EQ(bs13, bit3);
  std::bitset<8> bs14(bs1.at(4));
  EXPECT_EQ(bs14, bit4);
  std::bitset<8> bs15(bs1.at(5));
  EXPECT_EQ(bs15, bit5);
  std::bitset<8> bs16(bs1.at(6));
  EXPECT_EQ(bs16, bit6);
  std::bitset<8> bs17(bs1.at(7));
  EXPECT_EQ(bs17, bit7);
  std::bitset<8> bs18(bs1.at(8));
  EXPECT_EQ(bs18, bit8);
  std::bitset<8> bs19(bs1.at(9));
  EXPECT_EQ(bs19, bit9);
  std::bitset<8> bs110(bs1.at(10));
  EXPECT_EQ(bs110, bit10);
  std::bitset<8> bs111(bs1.at(11));
  EXPECT_EQ(bs111, bit11);
  std::bitset<8> bs112(bs1.at(12));
  EXPECT_EQ(bs112, bit12);
  std::bitset<8> bs113(bs1.at(13));
  EXPECT_EQ(bs113, bit13);
  std::bitset<8> bs114(bs1.at(14));
  EXPECT_EQ(bs114, bit14);
  std::bitset<8> bs115(bs1.at(15));
  EXPECT_EQ(bs115, bit15);

  Buf bf2(bs1, this->le);
  // EXPECT_EQ(int_data, bf2.ReverseReadInt());
  EXPECT_EQ(int_data, bf2.ReadInt());
  EXPECT_EQ(long_data, bf2.ReadLong());

  Buf bf3(16, !this->le);
  bf3.WriteInt(int_atad);
  bf3.WriteLong(long_atad);
  // bf3.ReverseWriteInt(int_atad);
  std::string bs2 = bf3.GetString();
  std::bitset<8> bs20(bs2.at(0));
  EXPECT_EQ(bs20, bit0);
  std::bitset<8> bs21(bs2.at(1));
  EXPECT_EQ(bs21, bit1);
  std::bitset<8> bs22(bs2.at(2));
  EXPECT_EQ(bs22, bit2);
  std::bitset<8> bs23(bs2.at(3));
  EXPECT_EQ(bs23, bit3);
  std::bitset<8> bs24(bs2.at(4));
  EXPECT_EQ(bs24, bit4);
  std::bitset<8> bs25(bs2.at(5));
  EXPECT_EQ(bs25, bit5);
  std::bitset<8> bs26(bs2.at(6));
  EXPECT_EQ(bs26, bit6);
  std::bitset<8> bs27(bs2.at(7));
  EXPECT_EQ(bs27, bit7);
  std::bitset<8> bs28(bs2.at(8));
  EXPECT_EQ(bs28, bit8);
  std::bitset<8> bs29(bs2.at(9));
  EXPECT_EQ(bs29, bit9);
  std::bitset<8> bs210(bs2.at(10));
  EXPECT_EQ(bs210, bit10);
  std::bitset<8> bs211(bs2.at(11));
  EXPECT_EQ(bs211, bit11);
  std::bitset<8> bs212(bs2.at(12));
  EXPECT_EQ(bs212, bit12);
  std::bitset<8> bs213(bs2.at(13));
  EXPECT_EQ(bs213, bit13);
  std::bitset<8> bs214(bs2.at(14));
  EXPECT_EQ(bs214, bit14);
  std::bitset<8> bs215(bs2.at(15));
  EXPECT_EQ(bs215, bit15);

  Buf bf4(bs2, !this->le);
  // EXPECT_EQ(int_atad, bf4.ReverseReadInt());
  EXPECT_EQ(int_atad, bf4.ReadInt());
  EXPECT_EQ(long_atad, bf4.ReadLong());
}

TEST_F(DingoSerialListTypeTest, recordTest) {
  InitVector();
  const auto schemas = GetSchemas();
  RecordEncoder re(0, schemas, 0L, this->le);
  InitRecord();

  const auto& record1 = GetRecord();

  std::string key, value;
  re.Encode('r', record1, key, value);

  RecordDecoder rd(0, schemas, 0L, this->le);
  std::vector<std::any> record2;
  rd.Decode(key, value, record2);

  int i = 0;
  for (const auto& bs : schemas) {
    BaseSchema::Type type = bs->GetType();
    auto r1 = record1.at(i);
    auto r2 = record2.at(i);

    ASSERT_TRUE(r1.has_value() == r1.has_value());

    switch (type) {
      case BaseSchema::kBool: {
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(std::any_cast<bool>(r1), std::any_cast<bool>(r2));
        }
        break;
      }
      case BaseSchema::kInteger: {
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(std::any_cast<int32_t>(r1), std::any_cast<int32_t>(r2));
        }

        break;
      }
      case BaseSchema::kLong: {
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(std::any_cast<int64_t>(r1), std::any_cast<int64_t>(r2));
        }

        break;
      }
      case BaseSchema::kDouble: {
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(std::any_cast<double>(r1), std::any_cast<double>(r2));
        }

        break;
      }
      case BaseSchema::kString: {
        if (r1.has_value() && r2.has_value()) {
          EXPECT_EQ(std::any_cast<std::string>(r1), std::any_cast<std::string>(r2));
        }

        break;
      }
      case BaseSchema::kBoolList: {
        if (r1.has_value() && r2.has_value()) {
          const auto& vec1 = std::any_cast<const std::vector<bool>&>(r1);
          const auto& vec2 = std::any_cast<const std::vector<bool>&>(r2);
          EXPECT_EQ(vec1.size(), vec2.size());
          for (int j = 0; j < vec1.size(); ++j) {
            EXPECT_EQ(vec1[j], vec2[j]);
          }
        }

        break;
      }
      case BaseSchema::kStringList: {
        if (r1.has_value() && r2.has_value()) {
          const auto& vec1 = std::any_cast<const std::vector<std::string>&>(r1);
          const auto& vec2 = std::any_cast<const std::vector<std::string>&>(r2);
          EXPECT_EQ(vec1.size(), vec2.size());
          for (int j = 0; j < vec1.size(); ++j) {
            EXPECT_EQ(vec1[j], vec2[j]);
          }
        }

        break;
      }
      case BaseSchema::kDoubleList: {
        if (r1.has_value() && r2.has_value()) {
          const auto& vec1 = std::any_cast<const std::vector<double>&>(r1);
          const auto& vec2 = std::any_cast<const std::vector<double>&>(r2);
          EXPECT_EQ(vec1.size(), vec2.size());
          for (int j = 0; j < vec1.size(); ++j) {
            EXPECT_EQ(vec1[j], vec2[j]);
          }
        }

        break;
      }
      case BaseSchema::kFloatList: {
        if (r1.has_value() && r2.has_value()) {
          const auto& vec1 = std::any_cast<const std::vector<float>&>(r1);
          const auto& vec2 = std::any_cast<const std::vector<float>&>(r2);
          EXPECT_EQ(vec1.size(), vec2.size());
          for (int j = 0; j < vec1.size(); ++j) {
            EXPECT_EQ(vec1[j], vec2[j]);
          }
        }

        break;
      }
      case BaseSchema::kIntegerList: {
        if (r1.has_value() && r2.has_value()) {
          const auto& vec1 = std::any_cast<const std::vector<int32_t>&>(r1);
          const auto& vec2 = std::any_cast<const std::vector<int32_t>&>(r2);
          EXPECT_EQ(vec1.size(), vec2.size());
          for (int j = 0; j < vec1.size(); ++j) {
            EXPECT_EQ(vec1[j], vec2[j]);
          }
        }

        break;
      }
      case BaseSchema::kLongList: {
        if (r1.has_value() && r2.has_value()) {
          const auto& vec1 = std::any_cast<const std::vector<int64_t>&>(r1);
          const auto& vec2 = std::any_cast<const std::vector<int64_t>&>(r2);
          EXPECT_EQ(vec1.size(), vec2.size());
          for (int j = 0; j < vec1.size(); ++j) {
            EXPECT_EQ(vec1[j], vec2[j]);
          }
        }

        break;
      }
      default: {
        break;
      }
    }
    i++;
  }
  // delete record2;

  std::vector<int> index{0, 1, 3, 5, 12, 13, 15, 17, 19, 22, 24};
  std::vector<int> index_temp{0, 1, 3, 5, 12, 13, 15, 17, 19, 22, 24};
  std::vector<std::any> record3;
  rd.Decode(key, value, index, record3);
  i = 0;
  for (const auto& bs : schemas) {
    BaseSchema::Type type = bs->GetType();

    switch (type) {
      case BaseSchema::kBool: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(std::any_cast<bool>(r1), std::any_cast<bool>(r2));
          }
          i++;
        }
        break;
      }
      case BaseSchema::kInteger: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(std::any_cast<int32_t>(r1), std::any_cast<int32_t>(r2));
          }
          i++;
        }
        break;
      }
      case BaseSchema::kLong: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(std::any_cast<int64_t>(r1), std::any_cast<int64_t>(r2));
          }
          i++;
        }
        break;
      }
      case BaseSchema::kDouble: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(std::any_cast<double>(r1), std::any_cast<double>(r2));
          }
          i++;
        }
        break;
      }
      case BaseSchema::kString: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            EXPECT_EQ(std::any_cast<std::string>(r1), std::any_cast<std::string>(r2));
          }
          i++;
        }
        break;
      }
      case BaseSchema::kBoolList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            const auto& vec1 = std::any_cast<std::vector<bool>&>(r1);
            const auto& vec2 = std::any_cast<std::vector<bool>&>(r2);
            EXPECT_EQ(vec1.size(), vec2.size());
            for (size_t j = 0; j < vec1.size(); ++j) {
              EXPECT_EQ(vec1[j], vec2[j]);
            }
          }

          i++;
        }
        break;
      }
      case BaseSchema::kStringList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            const auto& vec1 = std::any_cast<std::vector<std::string>&>(r1);
            const auto& vec2 = std::any_cast<std::vector<std::string>&>(r2);
            EXPECT_EQ(vec1.size(), vec2.size());
            for (size_t j = 0; j < vec1.size(); ++j) {
              EXPECT_EQ(vec1[j], vec2[j]);
            }
          }

          i++;
        }
        break;
      }
      case BaseSchema::kDoubleList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            const auto& vec1 = std::any_cast<std::vector<double>&>(r1);
            const auto& vec2 = std::any_cast<std::vector<double>&>(r2);
            EXPECT_EQ(vec1.size(), vec2.size());
            for (size_t j = 0; j < vec1.size(); ++j) {
              EXPECT_EQ(vec1[j], vec2[j]);
            }
          }

          i++;
        }
        break;
      }
      case BaseSchema::kFloatList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            const auto& vec1 = std::any_cast<std::vector<float>&>(r1);
            const auto& vec2 = std::any_cast<std::vector<float>&>(r2);
            EXPECT_EQ(vec1.size(), vec2.size());
            for (size_t j = 0; j < vec1.size(); ++j) {
              EXPECT_EQ(vec1[j], vec2[j]);
            }
          }

          i++;
        }
        break;
      }
      case BaseSchema::kIntegerList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            const auto& vec1 = std::any_cast<std::vector<int32_t>&>(r1);
            const auto& vec2 = std::any_cast<std::vector<int32_t>&>(r2);
            EXPECT_EQ(vec1.size(), vec2.size());
            for (size_t j = 0; j < vec1.size(); ++j) {
              EXPECT_EQ(vec1[j], vec2[j]);
            }
          }

          i++;
        }
        break;
      }
      case BaseSchema::kLongList: {
        if (binary_search(index_temp.begin(), index_temp.end(), bs->GetIndex())) {
          auto r1 = record1.at(bs->GetIndex());
          auto r2 = record3.at(i);
          ASSERT_TRUE(r1.has_value() == r1.has_value());

          if (r1.has_value() && r2.has_value()) {
            const auto& vec1 = std::any_cast<std::vector<int64_t>&>(r1);
            const auto& vec2 = std::any_cast<std::vector<int64_t>&>(r2);
            EXPECT_EQ(vec1.size(), vec2.size());
            for (size_t j = 0; j < vec1.size(); ++j) {
              EXPECT_EQ(vec1[j], vec2[j]);
            }
          }

          i++;
        }
        break;
      }
      default: {
        break;
      }
    }
  }

  DeleteRecords();
}
