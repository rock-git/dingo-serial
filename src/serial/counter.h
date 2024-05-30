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

class Counter {
 private:
  timeval s_;
  timeval sc_;

 public:
  Counter() { gettimeofday(&s_, nullptr); }
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
    time_t rawtime;
    char c[128];
    struct tm timeinfo;
    auto r = time(&rawtime);
    localtime_r(&rawtime, &timeinfo);
    auto r1 = strftime(c, sizeof(c), "%c", &timeinfo);  // Replace asctime_r with strftime
    return std::string(c);
  }
  virtual ~Counter() = default;
};

class Clock {
 public:
  Clock() {
    m_total_.tv_sec = 0;
    m_total_.tv_nsec = 0;
    m_isStart_ = false;
  }

  void Start() {
    if (!m_isStart_) {
      clock_gettime(CLOCK_REALTIME, &m_start_);
      m_isStart_ = true;
    }
  }

  void Stop() {
    if (m_isStart_) {
      struct timespec end;
      clock_gettime(CLOCK_REALTIME, &end);
      Add(m_total_, Diff(m_start_, end));
      m_isStart_ = false;
    }
  }

  time_t Second() const { return m_total_.tv_sec; }

  int64_t Nsecond() const { return m_total_.tv_nsec; }

 private:
  bool m_isStart_;

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
  struct timespec m_start_;
  struct timespec m_total_;
};

#endif
