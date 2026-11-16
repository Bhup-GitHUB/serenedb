////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "database_path_feature.h"

#include <absl/flags/flag.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <exception>
#include <iresearch/utils/application_exit.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/operating_system.hpp>
#include <iresearch/utils/string_utils.hpp>
#include <string>

#include "backup/pg_control.h"
#include "server/utils/lifecycle.h"
#include "server/utils/lockfile.h"

ABSL_FLAG(std::string, server_directory, "serenedb-data",
          "Path to the database directory. A positional argument, if given, "
          "takes precedence.");

ABSL_FLAG(std::string, hba_config, "",
          "Path to the host-based authentication config file. Empty (the "
          "default) uses <datadir>/pg_hba.conf; a relative path is resolved "
          "against the data directory.");

namespace sdb {

DatabasePathFeature::DatabasePathFeature()
  : _directory(
      lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory))) {
  auto& fs = irs::DuckDBEngine::Instance().instance().GetInternalFileSystem();
  // Resolve the HBA config path against the (now absolute) data directory.
  if (const std::string hba = absl::GetFlag(FLAGS_hba_config); hba.empty()) {
    _hba_config_file = fs.JoinPath(_directory, "pg_hba.conf");
  } else if (fs.IsPathAbsolute(hba)) {
    _hba_config_file = hba;
  } else {
    _hba_config_file = fs.JoinPath(_directory, hba);
  }

  try {
    if (fs.CreateDirectoryExtended(
          _directory, {.mode = duckdb::CreateDirectoryMode::RECURSIVE})) {
      SDB_INFO(STARTUP, "Created database directory: ", _directory);
    }
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "Unable to create database directory '", _directory,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }

  const auto version = fs.JoinPath(_directory, backup::kPgVersionFile);
  std::string locked;
  try {
    _initialized = fs.FileExists(version);
    locked = _initialized ? version : backup::PendingPgVersion(_directory);
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "Unable to check '", version,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
  auto lock = FileLock::Take(locked, FileLock::Mode::Exclusive);
  if (!lock.ok()) {
    SDB_FATAL(GENERAL, lock.status().message());
  }
  _lock = std::move(*lock);

  gInstance = this;
}

DatabasePathFeature::~DatabasePathFeature() { gInstance = nullptr; }

}  // namespace sdb
