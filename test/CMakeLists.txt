# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/test)

find_path(GTEST_HEADER NAMES gtest/gtest.h)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
endif()

find_library(GTEST_LIB NAMES gtest)
find_library(GTEST_MAIN_LIB NAMES gtest_main)

message(STATUS "GTEST_LIB: ${GTEST_LIB}")
message(STATUS "GTEST_MAIN_LIB: ${GTEST_MAIN_LIB}")

# set(CMAKE_CPP_FLAGS "-DGFLAGS_NS=${GFLAGS_NS}")
# set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -D__const__=__unused__ -D_GNU_SOURCE -DUSE_SYMBOLIZE -DNO_TCMALLOC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DUNIT_TEST -g -Dprivate=public -Dprotected=public -D__STRICT_ANSI__ ")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer -Wno-unused-result")

# bthread_* functions are used in logging.cc, and they need to be marked as
# weak symbols explicitly in Darwin system.
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        "-Wl,-U,_bthread_getspecific"
        "-Wl,-U,_bthread_setspecific"
        "-Wl,-U,_bthread_key_create")
endif()

file(GLOB TEST_DINGO_SERIAL_SRCS "test_*.cc")
foreach(DINGO_SERIAL_UT ${TEST_DINGO_SERIAL_SRCS})
    get_filename_component(DINGO_SERIAL_UT_WE ${DINGO_SERIAL_UT} NAME_WE)
    add_executable(${DINGO_SERIAL_UT_WE} ${DINGO_SERIAL_UT}
                   $<TARGET_OBJECTS:OBJ_LIB>)
    target_link_libraries(${DINGO_SERIAL_UT_WE}
                          ${GTEST_MAIN_LIB}
                          ${GTEST_LIB}
                          ${DYNAMIC_LIB}
                          )
endforeach()

