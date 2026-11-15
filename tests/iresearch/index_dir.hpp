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
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_reader_options.hpp>
#include <iresearch/index/segment_hold.hpp>
#include <iresearch/store/index_files.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tests {

enum class StorageKind : uint8_t {
  Local,
  Memory,
  Remote,
};

class IndexDir {
 public:
  IndexDir(duckdb::DatabaseInstance& db, std::string_view root);
  IndexDir(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
           std::string_view root);

  std::unique_ptr<duckdb::BufferedFileWriter> Create(
    std::string_view name) const;
  void Close(duckdb::BufferedFileWriter& out) const;
  duckdb::shared_ptr<duckdb::InputFile> Open(
    std::string_view name, duckdb::MappedFileAccess access) const;
  irs::bstring ReadWhole(std::string_view name) const;
  bool Has(std::string_view name) const;
  bool Remove(std::string_view name) const;
  void Rename(std::string_view src, std::string_view dst) const;
  void Sync(std::span<const std::string_view> names) const;
  void Compact(std::string_view name) const;
  std::vector<std::string> Files() const;
  void Visit(absl::FunctionRef<void(std::string_view)> visitor) const;

  duckdb::DatabaseInstance& Db() const noexcept { return _state->db; }
  duckdb::FileSystem& Fs() const noexcept { return _state->fs; }
  const std::string& Root() const noexcept { return _state->root; }
  StorageKind Kind() const noexcept { return _state->kind; }
  irs::ReleasedSegments& Released() const noexcept { return *_state->released; }
  const irs::FileObservers& Observers() const noexcept { return _observers; }

  irs::IndexReaderOptions ReaderOptions(
    irs::IndexReaderOptions options = {}) const;

  std::vector<irs::SegmentMeta> Committed() const;
  void Publish(std::vector<irs::SegmentMeta> segments) const;
  size_t RemoveReleased() const;

  IndexDir Observed(irs::FileObserver& observer) const;

 private:
  struct State {
    State(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
          std::string root, StorageKind kind)
      : db{db}, fs{fs}, root{std::move(root)}, kind{kind} {}
    ~State();

    duckdb::DatabaseInstance& db;
    duckdb::FileSystem& fs;
    std::string root;
    StorageKind kind;
    std::shared_ptr<irs::ReleasedSegments> released =
      std::make_shared<irs::ReleasedSegments>();
    absl::Mutex mutex;
    std::vector<irs::SegmentMeta> committed ABSL_GUARDED_BY(mutex);
  };

  std::shared_ptr<State> _state;
  irs::FileObservers _observers;
};

}  // namespace tests
