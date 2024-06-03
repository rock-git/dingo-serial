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

#include <memory>
#include <optional>
#include <string>

#include "serial/counter.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/schema/base_schema.h"
#include "serial/utils.h"

using namespace dingodb;

class DingoSerialTest : public testing::Test {
 public:
  void InitVector() {
    schemas_.resize(11);

    auto id = std::make_shared<DingoSchema<int32_t>>();
    id->SetIndex(0);
    id->SetAllowNull(false);
    id->SetIsKey(true);
    schemas_.at(0) = id;

    auto name = std::make_shared<DingoSchema<std::string>>();
    name->SetIndex(1);
    name->SetAllowNull(false);
    name->SetIsKey(true);
    schemas_.at(1) = name;

    auto gender = std::make_shared<DingoSchema<std::string>>();
    gender->SetIndex(2);
    gender->SetAllowNull(false);
    gender->SetIsKey(true);
    schemas_.at(2) = gender;

    auto score = std::make_shared<DingoSchema<int64_t>>();
    score->SetIndex(3);
    score->SetAllowNull(false);
    score->SetIsKey(true);
    schemas_.at(3) = score;

    auto addr = std::make_shared<DingoSchema<std::string>>();
    addr->SetIndex(4);
    addr->SetAllowNull(true);
    addr->SetIsKey(false);
    schemas_.at(4) = addr;

    auto exist = std::make_shared<DingoSchema<bool>>();
    exist->SetIndex(5);
    exist->SetAllowNull(false);
    exist->SetIsKey(false);
    schemas_.at(5) = exist;

    auto pic = std::make_shared<DingoSchema<std::string>>();
    pic->SetIndex(6);
    pic->SetAllowNull(true);
    pic->SetIsKey(false);
    schemas_.at(6) = pic;

    auto test_null = std::make_shared<DingoSchema<int32_t>>();
    test_null->SetIndex(7);
    test_null->SetAllowNull(true);
    test_null->SetIsKey(false);
    schemas_.at(7) = test_null;

    auto age = std::make_shared<DingoSchema<int32_t>>();
    age->SetIndex(8);
    age->SetAllowNull(false);
    age->SetIsKey(false);
    schemas_.at(8) = age;

    auto prev = std::make_shared<DingoSchema<int64_t>>();
    prev->SetIndex(9);
    prev->SetAllowNull(false);
    prev->SetIsKey(false);
    schemas_.at(9) = prev;

    auto salary = std::make_shared<DingoSchema<double>>();
    salary->SetIndex(10);
    salary->SetAllowNull(true);
    salary->SetIsKey(false);
    schemas_.at(10) = salary;
  }

  void DeleteSchemas() {
    schemas_.clear();
    schemas_.shrink_to_fit();
  }

  void InitRecord() {
    record_.resize(11);

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

    record_.at(0) = id;
    record_.at(1) = name;
    record_.at(2) = gender;
    record_.at(3) = score;
    record_.at(4) = addr;
    record_.at(5) = exist;
    record_.at(8) = age;
    record_.at(9) = prev;
    record_.at(10) = salary;
  }

  void DeleteRecords() {
    record_.clear();
    record_.shrink_to_fit();
  }

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

TEST_F(DingoSerialTest, keyvaluecodeStringLoopTest) {
  int32_t n = 10000;
  // Define column definitions and records
  std::vector<std::any> record1(n);
  std::vector<BaseSchemaPtr> schemas(n);
  for (int i = 0; i < n; i++) {
    std::string column_value = "value_" + std::to_string(i);
    auto str = std::make_shared<DingoSchema<std::string>>();
    str->SetIndex(i);
    str->SetAllowNull(false);
    str->SetIsKey(false);
    schemas.at(i) = str;
    record1.at(i) = column_value;
  }
  ASSERT_EQ(n, record1.size());

  // encode record
  RecordEncoder re(0, schemas, 0L, this->le);
  std::string key;
  std::string value;
  Counter load_cnter1;
  load_cnter1.ReStart();
  re.Encode('r', record1, key, value);

  // Decode record and verify values
  RecordDecoder rd(0, schemas, 0L, this->le);
  std::vector<std::any> decoded_records;
  Counter load_cnter2;
  load_cnter2.ReStart();
  rd.Decode(key, value, decoded_records);

  // Decode record selection columns
  int selection_columns_size = n - 3;
  {
    std::vector<int> indexes;
    indexes.reserve(selection_columns_size);
    for (int i = 0; i < selection_columns_size; i++) {
      indexes.push_back(i);
    }
    std::vector<int>& column_indexes = indexes;
    std::vector<std::any> decoded_s_records;
    Counter load_cnter3;
    load_cnter3.ReStart();

    rd.Decode(key, value, column_indexes, decoded_s_records);
  }

  {
    std::vector<int> indexes;
    selection_columns_size = n - selection_columns_size;
    indexes.reserve(selection_columns_size);
    for (int i = 0; i < selection_columns_size; i++) {
      indexes.push_back(i);
    }
    std::vector<int>& column_indexes = indexes;
    std::vector<std::any> decoded_s_records;
    Counter load_cnter3;
    load_cnter3.ReStart();

    rd.Decode(key, value, column_indexes, decoded_s_records);
  }
}
