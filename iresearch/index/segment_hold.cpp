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

#include "iresearch/index/segment_hold.hpp"

#include <iterator>
#include <utility>

namespace irs {

void ReleasedSegments::Release(std::string name, uint8_t kinds) {
  std::shared_ptr<const Released> released;
  {
    absl::MutexLock lock{&_mutex};
    if (!_closed) {
      _segments.emplace_back(std::move(name), kinds);
      released = _released;
    }
  }
  _held.fetch_sub(1, std::memory_order_release);
  if (released) {
    (*released)();
  }
}

std::vector<ReleasedSegment> ReleasedSegments::Take() {
  absl::MutexLock lock{&_mutex};
  return std::exchange(_segments, {});
}

void ReleasedSegments::Return(std::vector<ReleasedSegment> segments) {
  absl::MutexLock lock{&_mutex};
  _segments.insert(_segments.end(), std::make_move_iterator(segments.begin()),
                   std::make_move_iterator(segments.end()));
}

bool ReleasedSegments::Empty() const {
  absl::MutexLock lock{&_mutex};
  return _segments.empty();
}

void ReleasedSegments::Close() {
  absl::MutexLock lock{&_mutex};
  _closed = true;
}

void ReleasedSegments::OnRelease(Released released) {
  auto shared = std::make_shared<const Released>(std::move(released));
  absl::MutexLock lock{&_mutex};
  _released = std::move(shared);
}

}  // namespace irs
