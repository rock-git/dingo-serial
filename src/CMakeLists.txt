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

if(BUILD_UNIT_TESTS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUNIT_TEST")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DUNIT_TEST")
elseif(NOT DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DNDEBUG")
endif()

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/src)

add_library(OBJ_LIB OBJECT ${SOURCES})

set_property(TARGET ${OBJ_LIB} PROPERTY POSITION_INDEPENDENT_CODE 1)
add_library(dingo-serial-shared SHARED $<TARGET_OBJECTS:OBJ_LIB>)
add_library(dingo-serial-static STATIC $<TARGET_OBJECTS:OBJ_LIB>)
target_link_libraries(dingo-serial-shared ${DYNAMIC_LIB})
target_link_libraries(dingo-serial-static ${DYNAMIC_LIB})

SET_TARGET_PROPERTIES(dingo-serial-static PROPERTIES OUTPUT_NAME dingo-serial CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(dingo-serial-shared PROPERTIES OUTPUT_NAME dingo-serial CLEAN_DIRECT_OUTPUT 1)

install(TARGETS dingo-serial-static
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib${LIBSUFFIX}
        ARCHIVE DESTINATION lib${LIBSUFFIX}
        )

install(TARGETS dingo-serial-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib${LIBSUFFIX}
        ARCHIVE DESTINATION lib${LIBSUFFIX}
        )

