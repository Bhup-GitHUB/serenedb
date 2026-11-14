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

#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/write_stream.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <iresearch/utils/string.hpp>
#include <iterator>
#include <memory>

namespace irs {

class MemoryFile {
 public:
  uint64_t Length() const noexcept { return _bytes.size(); }
  bytes_view Bytes() const noexcept { return _bytes; }

  void Append(const byte_type* data, size_t size) { _bytes.append(data, size); }

 private:
  bstring _bytes;
};

class MemoryFileOutput final : public duckdb::WriteStream {
 public:
  explicit MemoryFileOutput(MemoryFile& file) noexcept : _file{file} {
    SetWindow(_buf, sizeof(_buf));
    write_offset = file.Length();
  }

  void Flush() {
    Append(write_begin, WindowSize());
    write_pos = write_begin;
  }

 private:
  void Overflow(const byte_type* data, size_t size) final {
    Flush();
    Append(data, size);
  }

  void Append(const byte_type* data, size_t size) {
    if (size == 0) {
      return;
    }
    _file.Append(data, size);
    write_offset += size;
  }

  byte_type _buf[64];
  MemoryFile& _file;
};

}  // namespace irs
namespace tests {

inline irs::bstring Bytes(const irs::MemoryFile& file) {
  return irs::bstring{file.Bytes()};
}

class BytesMapping final : public duckdb::MemoryMappedFile {
 public:
  explicit BytesMapping(irs::bytes_view bytes)
    : MemoryMappedFile{{},
                       duckdb::FileOpenFlags::FILE_FLAGS_READ,
                       const_cast<duckdb::data_ptr_t>(bytes.data()),
                       static_cast<duckdb::idx_t>(bytes.size())} {}

  void Sync() final {}
  bool Trim(duckdb::idx_t /*offset*/, duckdb::idx_t /*length*/) final {
    return false;
  }
  bool OnDiskFile() const final { return false; }
  void Close() final {}
};

inline duckdb::shared_ptr<duckdb::InputFile> OpenBytes(irs::bytes_view bytes) {
  return duckdb::make_shared_ptr<duckdb::InputFile>(
    duckdb::make_uniq<BytesMapping>(bytes));
}

}  // namespace tests
