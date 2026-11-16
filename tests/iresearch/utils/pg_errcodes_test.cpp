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

#include <cerrno>
#include <iresearch/utils/pg/errcodes.hpp>
#include <utility>

#include "tests_shared.hpp"

TEST(pg_errcodes_test, file_access_errors_map_as_postgres_maps_them) {
  const std::pair<int, int> expected[] = {
    {EPERM, ERRCODE_INSUFFICIENT_PRIVILEGE},
    {EACCES, ERRCODE_INSUFFICIENT_PRIVILEGE},
    {EROFS, ERRCODE_INSUFFICIENT_PRIVILEGE},
    {ENOENT, ERRCODE_UNDEFINED_FILE},
    {EEXIST, ERRCODE_DUPLICATE_FILE},
    {ENOTDIR, ERRCODE_WRONG_OBJECT_TYPE},
    {EISDIR, ERRCODE_WRONG_OBJECT_TYPE},
    {ENOTEMPTY, ERRCODE_WRONG_OBJECT_TYPE},
    {ENOSPC, ERRCODE_DISK_FULL},
    {ENOMEM, ERRCODE_OUT_OF_MEMORY},
    {ENFILE, ERRCODE_INSUFFICIENT_RESOURCES},
    {EMFILE, ERRCODE_INSUFFICIENT_RESOURCES},
    {EIO, ERRCODE_IO_ERROR},
    {ENAMETOOLONG, ERRCODE_FILE_NAME_TOO_LONG},
    {EINVAL, ERRCODE_INTERNAL_ERROR},
  };
  for (const auto [error, code] : expected) {
    EXPECT_EQ(code, irs::pg::ErrcodeForFileAccess(error)) << error;
  }
}
