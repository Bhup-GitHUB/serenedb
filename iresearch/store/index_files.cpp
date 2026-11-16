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

#include "iresearch/store/index_files.hpp"

#include <duckdb/common/error_data.hpp>

#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/log.hpp"

namespace irs {

TrackedFiles FileTracker::Flush() noexcept {
  return {.files = std::exchange(_files, {}),
          .bytes = std::exchange(_bytes, uint64_t{0})};
}

void FileTracker::Remove(duckdb::FileSystem& fs,
                         std::string_view root) noexcept {
  RemoveIndexFiles(fs, root, _files);
  _files.clear();
  _bytes = 0;
}

std::unique_ptr<duckdb::BufferedFileWriter> OpenOutput(duckdb::FileSystem& fs,
                                                       std::string_view root,
                                                       std::string_view name,
                                                       FileTracker& tracker,
                                                       bool keep_cached) {
  auto path = fs.JoinPath(root, name);
  auto writer = fs.OpenFileWriter(path, keep_cached);
  auto out = std::make_unique<duckdb::BufferedFileWriter>(fs, std::move(path),
                                                          std::move(writer));
  tracker.Created(std::string{name});
  return out;
}

void CloseOutput(duckdb::BufferedFileWriter& out, FileTracker& tracker) {
  const auto size = out.Position();
  out.Close();
  tracker.Written(size);
}

bool RemoveIndexFiles(duckdb::FileSystem& fs, std::string_view root,
                      std::span<const std::string> names) {
  if (names.empty()) {
    return true;
  }
  try {
    duckdb::vector<std::string> paths;
    paths.reserve(names.size());
    for (const auto& name : names) {
      paths.push_back(fs.JoinPath(root, name));
    }
    fs.RemoveFiles(paths);
    return true;
  } catch (const std::exception& e) {
    SDB_WARN(IRESEARCH, "Failed to remove index files of '", root,
             "': ", duckdb::ErrorData{e}.RawMessage());
    return false;
  }
}

void RemoveReleased(duckdb::FileSystem& fs, std::string_view root,
                    ReleasedSegments& released) {
  auto segments = released.Take();
  std::vector<std::string> names;
  for (const auto& segment : segments) {
    for (auto& name : SegmentFiles(segment.name, segment.kinds)) {
      names.push_back(std::move(name));
    }
  }
  if (!RemoveIndexFiles(fs, root, names)) {
    released.Return(std::move(segments));
  }
}

}  // namespace irs
