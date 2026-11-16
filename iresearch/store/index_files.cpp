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

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <duckdb/common/error_data.hpp>

#include "iresearch/index/file_names.hpp"
#include "iresearch/utils/log.hpp"

namespace irs {

void FileTracker::Created(std::string_view name) { _files.emplace(name); }

void FileTracker::Written(uint64_t bytes) { _files_size += bytes; }

void FileTracker::Removed(std::string_view name) { _files.erase(name); }

TrackedFiles FileTracker::Flush() {
  TrackedFiles flushed;
  flushed.files.reserve(_files.size());
  for (auto it = _files.begin(), end = _files.end(); it != end;) {
    flushed.files.emplace_back(std::move(_files.extract(it++).value()));
  }
  flushed.bytes = std::exchange(_files_size, uint64_t{0});
  return flushed;
}

void FileTracker::Remove(duckdb::FileSystem& fs,
                         std::string_view root) noexcept {
  const std::vector<std::string> names{_files.begin(), _files.end()};
  RemoveIndexFiles(fs, root, names);
  _files_size = 0;
  _files.clear();
}

std::unique_ptr<duckdb::BufferedFileWriter> OpenOutput(
  duckdb::FileSystem& fs, std::string_view root, std::string_view name,
  const FileObservers& observers, bool keep_cached) {
  for (auto* observer : observers) {
    observer->Created(name);
  }
  absl::Cleanup abandon = [&] {
    for (auto* observer : observers) {
      observer->Removed(name);
    }
  };
  auto path = fs.JoinPath(root, name);
  auto writer = fs.OpenFileWriter(path, keep_cached);
  auto out = std::make_unique<duckdb::BufferedFileWriter>(fs, std::move(path),
                                                          std::move(writer));
  std::move(abandon).Cancel();
  return out;
}

void CloseOutput(duckdb::BufferedFileWriter& out,
                 const FileObservers& observers) {
  const auto size = out.Position();
  out.Close();
  for (auto* observer : observers) {
    observer->Written(size);
  }
}

std::vector<std::string> ListIndexFiles(duckdb::FileSystem& fs,
                                        std::string_view root) {
  std::vector<std::string> files;
  fs.ListFiles(std::string{root},
               [&](duckdb::OpenFileInfo& info) {
                 if (!duckdb::FileSystem::IsDirectory(info)) {
                   files.push_back(std::move(info.path));
                 }
               },
               nullptr, {.strict = true});
  return files;
}

uint64_t MaxSegmentId(std::span<const std::string> files) {
  uint64_t max_id = 0;
  for (const auto& name : files) {
    if (uint64_t id = 0; ParseSegmentId(name, id)) {
      max_id = std::max(max_id, id);
    }
  }
  return max_id;
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
