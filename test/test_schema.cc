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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/schema/base_schema.h"
#include "serial/utils.h"

using namespace dingodb;

class SchemaTest : public testing::Test {};

TEST_F(SchemaTest, boolType) {
  {
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<bool>(true);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<bool>(actual_data), std::any_cast<bool>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<bool>(false);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<bool>(actual_data), std::any_cast<bool>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<bool>(true);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<bool>(actual_data), std::any_cast<bool>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<bool>(false);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<bool>(actual_data), std::any_cast<bool>(data));
  }
}

TEST_F(SchemaTest, boolListType) {
  {
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<bool> data = {true, false, true};
    schema->EncodeValue(std::make_any<std::vector<bool>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<bool>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<bool> data = {};
    schema->EncodeValue(std::make_any<std::vector<bool>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<bool>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    schema->EncodeValue(std::any(), buf);

    auto decode_value = schema->DecodeValue(buf);
    EXPECT_EQ(false, decode_value.has_value());
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<bool> data = {true, false, true};
    schema->EncodeValue(std::make_any<std::vector<bool>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<bool>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, intType) {
  {
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<int32_t>(101);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<int32_t>(actual_data), std::any_cast<int32_t>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<int32_t>(101);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<int32_t>(actual_data), std::any_cast<int32_t>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data;
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(false, actual_data.has_value());
  }
}

TEST_F(SchemaTest, intListType) {
  {
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<int32_t> data = {1, 2, 3};
    schema->EncodeValue(std::make_any<std::vector<int32_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int32_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int32_t> data = {};
    schema->EncodeValue(std::make_any<std::vector<int32_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int32_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    schema->EncodeValue(std::any(), buf);

    auto decode_value = schema->DecodeValue(buf);
    EXPECT_EQ(false, decode_value.has_value());
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int32_t> data = {3, 6, 9};
    schema->EncodeValue(std::make_any<std::vector<int32_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int32_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, longType) {
  {
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<int64_t>(101);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<int64_t>(actual_data), std::any_cast<int64_t>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<int64_t>(101);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<int64_t>(actual_data), std::any_cast<int64_t>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data;
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(false, actual_data.has_value());
  }
}

TEST_F(SchemaTest, longListType) {
  {
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<int64_t> data = {1, 2, 3};
    schema->EncodeValue(std::make_any<std::vector<int64_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int64_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int64_t> data = {};
    schema->EncodeValue(std::make_any<std::vector<int64_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int64_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    schema->EncodeValue(std::any(), buf);

    auto decode_value = schema->DecodeValue(buf);
    EXPECT_EQ(false, decode_value.has_value());
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int64_t> data = {3, 6, 9};
    schema->EncodeValue(std::make_any<std::vector<int64_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int64_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, floatType) {
  {
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<float>(101.12);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<float>(actual_data), std::any_cast<float>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<float>(101.2132);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<float>(actual_data), std::any_cast<float>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data;
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(false, actual_data.has_value());
  }
}

TEST_F(SchemaTest, floatListType) {
  {
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<float> data = {1.1, 2.2, 3.3};
    schema->EncodeValue(std::make_any<std::vector<float>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<float>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<float> data = {};
    schema->EncodeValue(std::make_any<std::vector<float>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<float>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    schema->EncodeValue(std::any(), buf);

    auto decode_value = schema->DecodeValue(buf);
    EXPECT_EQ(false, decode_value.has_value());
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<float> data = {3, 6, 9};
    schema->EncodeValue(std::make_any<std::vector<float>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<float>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, doubleType) {
  {
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<double>(101.12);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<double>(actual_data), std::any_cast<double>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<double>(101.2132);
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<double>(actual_data), std::any_cast<double>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data;
    EXPECT_EQ(schema->GetLength(), schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(false, actual_data.has_value());
  }
}

TEST_F(SchemaTest, doubleListType) {
  {
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<double> data = {1, 2, 3};
    schema->EncodeValue(std::make_any<std::vector<double>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<double>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<double> data = {};
    schema->EncodeValue(std::make_any<std::vector<double>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<double>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    schema->EncodeValue(std::any(), buf);

    auto decode_value = schema->DecodeValue(buf);
    EXPECT_EQ(false, decode_value.has_value());
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<double> data = {3, 6, 9};
    schema->EncodeValue(std::make_any<std::vector<double>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<double>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, stringType) {
  // encode/decode key
  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<std::string>("hello");
    EXPECT_EQ(10, schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<std::string>(actual_data), std::any_cast<std::string>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<std::string>("hello world");
    EXPECT_EQ(19, schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<std::string>(actual_data), std::any_cast<std::string>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<std::string>("hello");
    EXPECT_EQ(10, schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<std::string>(actual_data), std::any_cast<std::string>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<std::string>("hello world");
    EXPECT_EQ(19, schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(std::any_cast<std::string>(actual_data), std::any_cast<std::string>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data;
    EXPECT_EQ(1, schema->EncodeKey(data, buf));
    auto actual_data = schema->DecodeKey(buf);
    EXPECT_EQ(false, actual_data.has_value());
  }

  // encode/decode value
  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data = std::make_any<std::string>("hello world");
    EXPECT_EQ(16, schema->EncodeValue(data, buf));
    auto actual_data = schema->DecodeValue(buf);
    EXPECT_EQ(std::any_cast<std::string>(actual_data), std::any_cast<std::string>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data = std::make_any<std::string>("hello world");
    EXPECT_EQ(16, schema->EncodeValue(data, buf));
    auto actual_data = schema->DecodeValue(buf);
    EXPECT_EQ(std::any_cast<std::string>(actual_data), std::any_cast<std::string>(data));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::any data;
    EXPECT_EQ(1, schema->EncodeValue(data, buf));
    auto actual_data = schema->DecodeValue(buf);
    EXPECT_EQ(false, actual_data.has_value());
  }
}

TEST_F(SchemaTest, stringListType) {
  {
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<std::string> data = {"hello", "world", "nihao"};
    schema->EncodeValue(std::make_any<std::vector<std::string>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<std::string>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<std::string> data = {};
    schema->EncodeValue(std::make_any<std::vector<std::string>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<std::string>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    schema->EncodeValue(std::any(), buf);

    auto decode_value = schema->DecodeValue(buf);
    EXPECT_EQ(false, decode_value.has_value());
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<std::string> data = {"hello", "ninhao", "world"};
    schema->EncodeValue(std::make_any<std::vector<std::string>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<std::string>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}
