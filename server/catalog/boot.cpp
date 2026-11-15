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

#include "catalog/boot.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backup/pg_control.h"
#include "backup/restore.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/log.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "pg/catalog/oids.h"
#include "search/store.h"
#include "search/wal_recovery.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kLogDir = "wal";

std::vector<StorageEntry> ListStoragesOrDie(const std::string& directory) {
  try {
    return ListStorages(directory);
  } catch (const std::exception& e) {
    SDB_FATAL(STARTUP, "cannot list directory '", directory,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

bool RemoveUnowned(const std::string& directory,
                   const absl::flat_hash_set<duckdb::idx_t>& owned) {
  bool removed = true;
  for (const auto& [oid, path] : ListStoragesOrDie(directory)) {
    if (!owned.contains(oid)) {
      removed = RemoveTree(path) && removed;
    }
  }
  return removed;
}

void OpenLive(search::Store& storage, const duckdb::CatalogEntry& entry) {
  if (!storage.Opened()) {
    storage.Open();
  }
  if (storage.Absent()) {
    SDB_FATAL(STARTUP, "the directory '", storage.Path(), "' of ",
              entry.name.GetIdentifierName(), " (oid ", entry.oid,
              ") is missing");
  }
}

void OpenStorages(SereneDBCatalog& catalog,
                  absl::flat_hash_set<duckdb::idx_t>& oids) {
  ForEachSearchStorage(
    catalog,
    [&](SearchTableEntry& table) {
      OpenLive(*table.Storage(), table);
      oids.insert(table.oid);
    },
    [&](InvertedIndexEntry& index) {
      OpenLive(*index.Storage(), index);
      oids.insert(index.oid);
    });
}

}  // namespace

DataDirectory::DataDirectory(std::string directory_p)
  : directory{std::move(directory_p)},
    engine{directory.empty()
             ? std::string{}
             : duckdb::FileSystem::JoinPath(directory, kEngineDir)} {}

std::string DataDirectory::LogDir() const {
  return duckdb::FileSystem::JoinPath(engine, kLogDir);
}

std::string DataDirectory::StoreDir() const { return StoreDirOf(engine); }

std::string DataDirectory::StorageDir(duckdb::idx_t oid) const {
  return StoragePathIn(StoreDir(), oid);
}

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AccessMode access_mode,
            duckdb::AttachVisibility visibility) {
  duckdb::AttachOptions options{info.options, access_mode};
  options.db_type.assign(type);
  options.visibility = visibility;
  duckdb::DatabaseManager::Get(context).AttachDatabase(context, info, options);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo>,
  duckdb::AttachedDatabase& db, duckdb::Catalog&) {
  return duckdb::make_uniq<TransactionManager>(db);
}

duckdb::InternalCatalog CreateInternalCatalog(duckdb::AttachedDatabase& db,
                                              const DataDirectory& layout) {
  duckdb::unique_ptr<duckdb::Catalog> catalog;
  if (db.IsSystem()) {
    catalog = duckdb::make_uniq<ClusterCatalog>(db, layout);
  } else {
    catalog = duckdb::make_uniq<SereneDBCatalog>(
      db, std::make_shared<DatabaseDirectory>(std::string{}), false);
  }
  return {.catalog = std::move(catalog),
          .transaction_manager = duckdb::make_uniq<TransactionManager>(db)};
}

void InitCatalog(bool default_database, bool initialized) {
  auto& instance = irs::DuckDBEngine::Instance().instance();
  auto& cluster = ClusterOf(instance);
  const auto& layout = cluster.Layout();
  if (!layout.InMemory()) {
    BeginStorageWork();
    StorageFileSystem().CreateDirectoryExtended(
      layout.StoreDir(),
      {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
    backup::PrepareDataDirectory(layout);
    if (initialized && !Log::Exists(StorageFileSystem(), layout.LogDir())) {
      SDB_FATAL(STARTUP, "'", layout.directory, "' holds ",
                backup::kPgVersionFile, " but '", layout.LogDir(),
                "' holds no catalog log: refusing to start an empty catalog "
                "in it");
    }
  }
  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  auto& transactions = Transactions::Get();
  bool clean = false;
  if (!layout.InMemory()) {
    auto log = std::make_unique<Log>(StorageFileSystem(), layout.LogDir());
    const auto target = [&](duckdb::idx_t id) {
      return InstanceCatalog(context, id);
    };
    transactions.SetReplaying(true);
    log->Replay(
      [&](std::span<uint8_t> payload) {
        context.RunFunctionInTransaction(
          [&] { clean = ReplayRecord(context, target, payload); });
      },
      &CatalogPart);
    transactions.SetReplaying(false);
    if (clean) {
      log->Append(StartupRecord());
    }
    transactions.Open(std::move(log));
  }
  context.RunFunctionInTransaction(
    [&] { cluster.Bootstrap(context, default_database); });
  auth::PublishRoles(cluster);
  if (layout.InMemory()) {
    return;
  }
  if (!initialized) {
    backup::PublishPgVersion(layout.directory);
  }
  const auto attached =
    SereneDBDatabases(duckdb::DatabaseManager::Get(instance));
  absl::flat_hash_set<duckdb::idx_t> live;
  for (const auto& db : attached) {
    OpenStorages(db->GetCatalog().Cast<SereneDBCatalog>(), live);
  }
  bool removed = true;
  if (!clean) {
    std::vector<std::string> tablespaces;
    context.RunFunctionInTransaction([&] {
      tablespaces =
        cluster.TablespaceDirs(cluster.GetCatalogTransaction(context));
    });
    for (const auto& tablespace : tablespaces) {
      removed = RemoveUnowned(StoreDirOf(tablespace), live) && removed;
    }
  }
  const bool recovered =
    search::RecoverSearchStorages(context, attached, /*orphans=*/!clean);
  EndStorageWork(removed && recovered);
  std::vector<duckdb::reference<ForeignServerCatalogEntry>> servers;
  for (const auto& db : attached) {
    db->GetCatalog()
      .Cast<SereneDBCatalog>()
      .Set(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
      .ScanCommitted([&](duckdb::CatalogEntry& entry) {
        servers.emplace_back(entry.Cast<ForeignServerCatalogEntry>());
      });
  }
  for (auto& server : servers) {
    try {
      context.RunFunctionInTransaction([&] { server.get().Attach(context); });
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "failed to re-attach foreign server '",
               server.get().name.GetIdentifierName(),
               "': ", duckdb::ErrorData{e}.RawMessage());
    }
  }
}

void CloseLog() {
  auto& transactions = Transactions::Get();
  if (auto* log = transactions.GetLog(); log && StorageTidy()) {
    log->Append(ShutdownRecord());
  }
  transactions.Close();
}

}  // namespace sdb::catalog
