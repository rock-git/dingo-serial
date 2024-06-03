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

#ifndef DINGO_SERIAL_COUNTER_H_
#define DINGO_SERIAL_COUNTER_H_

#include <sys/time.h>

#include <cstdint>
#include <iostream>

namespace dingodb {

class Counter {
 public:
  Counter() { gettimeofday(&s_, nullptr); }
  virtual ~Counter() = default;

  void ReStart() { gettimeofday(&s_, nullptr); }
  void SaveCounter() { gettimeofday(&sc_, nullptr); }
  int TimeElapsedBeforeLastSave() const { return (((sc_.tv_sec - s_.tv_sec) * 1000000) + (sc_.tv_usec - s_.tv_usec)); }

  int64_t TimeElapsed() const {
    timeval e;
    gettimeofday(&e, nullptr);
    return (((e.tv_sec - s_.tv_sec) * 1000000) + (e.tv_usec - s_.tv_usec));
  }

  int64_t MtimeElapsed() const {
    timeval e;
    gettimeofday(&e, nullptr);
    return (((e.tv_sec - s_.tv_sec) * 1000) + (e.tv_usec - s_.tv_usec) / 1000);
  }

  static std::string GetSysTime() {
    char c[128];
    struct tm timeinfo;
    time_t rawtime = time(nullptr);
    localtime_r(&rawtime, &timeinfo);
    asctime_r(&timeinfo, c);
    return std::string(c);
  }

 private:
  timeval s_;
  timeval sc_;
};

class Clock {
 public:
  Clock() {
    start_.tv_sec = 0;
    start_.tv_nsec = 0;
    total_.tv_sec = 0;
    total_.tv_nsec = 0;
  }

  void Start() {
    if (!is_start_) {
      clock_gettime(CLOCK_REALTIME, &start_);
      is_start_ = true;
    }
  }

  void Stop() {
    if (is_start_) {
      struct timespec end;
      clock_gettime(CLOCK_REALTIME, &end);
      Add(total_, Diff(start_, end));
      is_start_ = false;
    }
  }

  time_t Second() const { return total_.tv_sec; }

  int64_t Nsecond() const { return total_.tv_nsec; }

 private:
  static void Add(struct timespec& dst, const struct timespec& src) {
    dst.tv_sec += src.tv_sec;
    dst.tv_nsec += src.tv_nsec;
    if (dst.tv_nsec > 1000000000) {
      dst.tv_nsec -= 1000000000;
      dst.tv_sec++;
    }
  }

  static struct timespec Diff(const struct timespec& start, const struct timespec& end) {
    struct timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
      temp.tv_sec = end.tv_sec - start.tv_sec - 1;
      temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
      temp.tv_sec = end.tv_sec - start.tv_sec;
      temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
  }

  bool is_start_{false};
  struct timespec start_;
  struct timespec total_;
};

}  // namespace dingodb

#endif
