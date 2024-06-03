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

#ifndef DINGO_SERIAL_COMPILER_H_
#define DINGO_SERIAL_COMPILER_H_

namespace dingodb {

#define COMPILER_GCC

#if defined(COMPILER_GCC)
#if defined(__cplusplus)
#define DINGO_LIKELY(expr) (__builtin_expect((bool)(expr), true))
#define DINGO_UNLIKELY(expr) (__builtin_expect((bool)(expr), false))
#else
#define DINGO_LIKELY(expr) (__builtin_expect(!!(expr), 1))
#define DINGO_UNLIKELY(expr) (__builtin_expect(!!(expr), 0))
#endif
#else
#define DINGO_LIKELY(expr) (expr)
#define DINGO_UNLIKELY(expr) (expr)
#endif

}  // namespace dingodb

#endif  // DINGO_SERIAL_COMPILER_H_
