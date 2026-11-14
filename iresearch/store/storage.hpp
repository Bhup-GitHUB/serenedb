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

#include <absl/functional/function_ref.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb/main/database.hpp>
#include <string>
#include <string_view>

namespace irs {

duckdb::unique_ptr<duckdb::FileHandle> OpenWhole(duckdb::FileSystem& fs,
                                                 const std::string& path);

template<typename Bytes>
Bytes ReadWhole(duckdb::QueryContext context, duckdb::FileSystem& fs,
                const std::string& path) {
  const auto handle = OpenWhole(fs, path);
  Bytes bytes(duckdb::NumericCast<size_t>(handle->GetFileSize()),
              typename Bytes::value_type{});
  if (!bytes.empty()) {
    handle->Read(context, bytes.data(), bytes.size(), 0);
    return bytes;
  }
  typename Bytes::value_type buffer[4096];
  while (const auto read = handle->Read(context, buffer, sizeof(buffer))) {
    bytes.append(buffer, static_cast<size_t>(read));
  }
  return bytes;
}

void WriteWhole(duckdb::FileSystem& fs, const std::string& path,
                std::string_view content, bool durable);

void ForEach(duckdb::DatabaseInstance& db, std::string_view root, size_t count,
             absl::FunctionRef<void(size_t)> visit);

}  // namespace irs
