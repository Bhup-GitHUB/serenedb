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

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/index/segment_hold.hpp"

namespace irs {

struct TrackedFiles {
  std::vector<std::string> files;
  uint64_t bytes = 0;
};

class FileTracker {
 public:
  void Created(std::string name) { _files.push_back(std::move(name)); }
  void Written(uint64_t bytes) noexcept { _bytes += bytes; }

  TrackedFiles Flush() noexcept;
  void Remove(duckdb::FileSystem& fs, std::string_view root) noexcept;

 private:
  std::vector<std::string> _files;
  uint64_t _bytes = 0;
};

std::unique_ptr<duckdb::BufferedFileWriter> OpenOutput(duckdb::FileSystem& fs,
                                                       std::string_view root,
                                                       std::string_view name,
                                                       FileTracker& tracker,
                                                       bool keep_cached);

void CloseOutput(duckdb::BufferedFileWriter& out, FileTracker& tracker);

bool RemoveIndexFiles(duckdb::FileSystem& fs, std::string_view root,
                      std::span<const std::string> names);

void RemoveReleased(duckdb::FileSystem& fs, std::string_view root,
                    ReleasedSegments& released);

}  // namespace irs
