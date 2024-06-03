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

#ifndef DINGO_SERIAL_UTILS_H_
#define DINGO_SERIAL_UTILS_H_

#include <any>
#include <iostream>
#include <memory>
#include <vector>

#include "schema/base_schema.h"
#include "schema/boolean_schema.h"
#include "schema/double_list_schema.h"
#include "schema/double_schema.h"
#include "schema/float_list_schema.h"
#include "schema/integer_list_schema.h"
#include "schema/integer_schema.h"
#include "schema/long_list_schema.h"
#include "schema/long_schema.h"
#include "schema/string_list_schema.h"
#include "schema/string_schema.h"

namespace dingodb {

void SortSchema(std::vector<BaseSchemaPtr>& schemas);
void FormatSchema(std::vector<BaseSchemaPtr>& schemas, bool le);

bool VectorFindAndRemove(std::vector<int>* v, int t);
// bool VectorFind(const std::vector<int>& v, int t);
// bool VectorFind(const std::vector<int>& v, int t, int n);
inline bool VectorFind(const std::vector<int>& v, int t, int n) { return v[n] == t; }

bool IsLE();

// string type cast
bool StringToBool(const std::string& str);
int32_t StringToInt32(const std::string& str);
int64_t StringToInt64(const std::string& str);
float StringToFloat(const std::string& str);
double StringToDouble(const std::string& str);

}  // namespace dingodb

#endif
