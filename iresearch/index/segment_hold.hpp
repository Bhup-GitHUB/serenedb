////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/functional/any_invocable.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/utils/noncopyable.hpp"

namespace irs {

struct ReleasedSegment {
  std::string name;
  uint8_t kinds = 0;
};

class ReleasedSegments {
 public:
  using Released = absl::AnyInvocable<void() const noexcept>;

  void Acquire() noexcept { _held.fetch_add(1, std::memory_order_relaxed); }
  void Release(std::string name, uint8_t kinds);
  uint64_t Held() const noexcept {
    return _held.load(std::memory_order_acquire);
  }

  std::vector<ReleasedSegment> Take();
  void Return(std::vector<ReleasedSegment> segments);
  bool Empty() const;
  void Close();
  void OnRelease(Released released);

 private:
  std::atomic<uint64_t> _held{0};
  mutable absl::Mutex _mutex;
  std::vector<ReleasedSegment> _segments ABSL_GUARDED_BY(_mutex);
  std::shared_ptr<const Released> _released ABSL_GUARDED_BY(_mutex);
  bool _closed ABSL_GUARDED_BY(_mutex) = false;
};

class SegmentHold : private util::Noncopyable {
 public:
  SegmentHold(std::shared_ptr<ReleasedSegments> released, std::string name,
              uint8_t kinds) noexcept
    : _released{std::move(released)}, _name{std::move(name)}, _kinds{kinds} {
    _released->Acquire();
  }

  ~SegmentHold() { _released->Release(std::move(_name), _kinds); }

 private:
  std::shared_ptr<ReleasedSegments> _released;
  std::string _name;
  uint8_t _kinds;
};

}  // namespace irs
