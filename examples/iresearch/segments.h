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
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace examples {

class Segments {
 public:
  Segments(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
           std::string_view root, const irs::IndexReaderOptions& options)
    : _db{db}, _fs{fs}, _root{root}, _options{options} {}

  void Add(std::span<const irs::IndexWriter::FlushedSegment> flushed);
  void Remove(const irs::DocRemoval& removal);
  bool Replace(const irs::IndexWriter::Merged& merged);
  irs::DirectoryReader Reader() const;

 private:
  duckdb::DatabaseInstance& _db;
  duckdb::FileSystem& _fs;
  std::string _root;
  irs::IndexReaderOptions _options;
  std::vector<irs::SegmentMeta> _segments;
};

}  // namespace examples
