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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <duckdb/common/error_data.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/duckdb_engine.hpp>
#include <store/simulated_object_store.hpp>
#include <string>
#include <thread>
#include <vector>

#include "catalog/database_directory.h"
#include "scheduler/background_scheduler.h"

namespace sdb::catalog {
namespace {

irs::SimulatedObjectStore& ObjectStore() {
  static auto& store = irs::SimulatedObjectStore::Register(
    irs::DuckDBEngine::Instance().instance());
  return store;
}

bool Eventually(const auto& done) {
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds{30};
  while (!done()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
  return true;
}

std::vector<duckdb::idx_t> Oids(const std::vector<StorageEntry>& storages) {
  std::vector<duckdb::idx_t> oids;
  for (const auto& storage : storages) {
    oids.push_back(storage.oid);
  }
  absl::c_sort(oids);
  return oids;
}

class DatabaseDirectoryTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("database_directory_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
    _scheduler.start();
  }

  void TearDown() final {
    _scheduler.stop();
    std::filesystem::remove_all(_root);
  }

  static bool Gone(const std::filesystem::path& path) {
    return Eventually([&] { return !std::filesystem::exists(path); });
  }

  static bool ObjectGone(const std::string& path) {
    return Eventually([&] { return !ObjectStore().Find(path); });
  }

  std::string Path(const std::filesystem::path& relative) const {
    return (_root / relative).string();
  }

  std::filesystem::path _root;
  BackgroundScheduler _scheduler;
};

TEST_F(DatabaseDirectoryTest, StoragesSitInTheStoreDirectoryOfATablespace) {
  const auto store = StoreDirOf(Path("ts/engine_v1"));
  EXPECT_EQ(store, Path("ts/engine_v1/store"));
  EXPECT_EQ(StoragePathIn(store, 7), Path("ts/engine_v1/store/7"));
}

TEST_F(DatabaseDirectoryTest, CreatesOnceWithEveryMissingParent) {
  const auto storage = Path("ts/engine_v1/store/7");
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(storage));
  CreateDirectory(storage);
  EXPECT_TRUE(std::filesystem::is_directory(storage));
  EXPECT_ANY_THROW(CreateDirectory(storage));
  std::ofstream{_root / "file"} << "data";
  EXPECT_ANY_THROW(CreateDirectory(Path("file/sub")));
}

TEST_F(DatabaseDirectoryTest, ARemovedStorageLeavesItsSiblings) {
  const auto store = StoreDirOf(_root.string());
  CreateDirectory(StoragePathIn(store, 7));
  CreateDirectory(StoragePathIn(store, 8));
  std::ofstream{_root / "store" / "7" / "segment"} << "rows";
  RemoveStorage(StoragePathIn(store, 7), [this] {
    return std::filesystem::remove(_root / "store" / "7" / "segment");
  });
  EXPECT_TRUE(Gone(_root / "store" / "7"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "store" / "8"));
}

TEST_F(DatabaseDirectoryTest, AStorageWhoseFilesStayIsKept) {
  const auto storage = StoragePathIn(StoreDirOf(_root.string()), 7);
  CreateDirectory(storage);
  std::ofstream{_root / "store" / "7" / "segment"} << "rows";
  std::atomic_bool tried{false};
  RemoveStorage(storage, [&] {
    tried.store(true);
    return false;
  });
  EXPECT_TRUE(Eventually([&] { return tried.load(); }));
  EXPECT_TRUE(std::filesystem::exists(_root / "store" / "7" / "segment"));
}

TEST_F(DatabaseDirectoryTest, RemovalsWaitWhileTheyAreDeferred) {
  const auto storage = StoragePathIn(StoreDirOf(_root.string()), 7);
  CreateDirectory(storage);
  DeferRemovals();
  RemoveStorage(storage, [] { return true; });
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_TRUE(std::filesystem::is_directory(storage));
  ResumeRemovals();
  EXPECT_TRUE(Gone(storage));
}

TEST_F(DatabaseDirectoryTest, StoragesOnAnObjectStore) {
  ObjectStore();
  const auto store = StoreDirOf(absl::StrCat(
    irs::SimulatedObjectStore::kPrefix, "bucket/database_directory_",
    ::getpid()));
  const auto path = StoragePathIn(store, 7);
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(path));
  CreateDirectory(path);
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(path, "/")));
  EXPECT_TRUE(StorageFileSystem().DirectoryExists(path));
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(StoragePathIn(store, 8)));

  ObjectStore().Publish(absl::StrCat(path, "/segment"), "rows");
  EXPECT_EQ(4, StorageBytes(path));
  RemoveStorage(path, [] { return true; });
  EXPECT_TRUE(ObjectGone(absl::StrCat(path, "/")));
  EXPECT_TRUE(ObjectGone(absl::StrCat(path, "/segment")));
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(path));
}

TEST_F(DatabaseDirectoryTest, RemoveTreeSaysWhetherTheTreeIsGone) {
  EXPECT_TRUE(RemoveTree(Path("missing")));
  std::filesystem::create_directories(_root / "tree" / "sub");
  std::ofstream{_root / "tree" / "sub" / "file"} << "data";
  EXPECT_TRUE(RemoveTree(Path("tree")));
  EXPECT_FALSE(std::filesystem::exists(_root / "tree"));
}

TEST_F(DatabaseDirectoryTest, RemoveTreeOrThrowSaysWhyTheTreeStays) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "root removes a file whatever the mode of its directory";
  }
  const auto sub = _root / "kept" / "sub";
  std::filesystem::create_directories(sub);
  std::ofstream{sub / "file"} << "data";
  std::filesystem::permissions(sub, std::filesystem::perms::owner_read |
                                      std::filesystem::perms::owner_exec);
  absl::Cleanup writable = [&] {
    std::filesystem::permissions(sub, std::filesystem::perms::owner_all);
  };
  try {
    RemoveTreeOrThrow(Path("kept"));
    FAIL() << "the removal reported no failure";
  } catch (const std::exception& e) {
    EXPECT_TRUE(
      absl::StrContains(duckdb::ErrorData{e}.RawMessage(), "Permission denied"))
      << duckdb::ErrorData{e}.RawMessage();
  }
  EXPECT_FALSE(RemoveTree(Path("kept")));
  EXPECT_TRUE(std::filesystem::exists(sub / "file"));
}

TEST_F(DatabaseDirectoryTest, ListsTheStoragesOfADirectory) {
  const auto store = StoreDirOf(_root.string());
  CreateDirectory(StoragePathIn(store, 7));
  CreateDirectory(StoragePathIn(store, 8));
  std::ofstream{_root / "store" / "8" / "segment"} << "rows";
  std::ofstream{_root / "store" / "notes"} << "kept";
  std::filesystem::create_directory(_root / "store" / "named");

  const auto storages = ListStorages(store);
  EXPECT_EQ(Oids(storages), (std::vector<duckdb::idx_t>{7, 8}));
  for (const auto& storage : storages) {
    EXPECT_EQ(storage.path, StoragePathIn(store, storage.oid));
  }
  EXPECT_EQ(4, StorageBytes(StoragePathIn(store, 8)));
  EXPECT_EQ(8, StorageBytes(store));
  EXPECT_TRUE(ListStorages(Path("missing")).empty());
}

TEST_F(DatabaseDirectoryTest, ListsTheStoragesOfAnObjectStore) {
  ObjectStore();
  const auto store = StoreDirOf(absl::StrCat(
    irs::SimulatedObjectStore::kPrefix, "bucket/listed_", ::getpid()));
  CreateDirectory(StoragePathIn(store, 7));
  CreateDirectory(StoragePathIn(store, 8));
  ObjectStore().Publish(absl::StrCat(store, "/8/segment"), "rows");
  ObjectStore().Publish(absl::StrCat(store, "/9/orphan"), "rows");

  EXPECT_EQ(Oids(ListStorages(store)), (std::vector<duckdb::idx_t>{7, 8, 9}));
  EXPECT_TRUE(RemoveTree(StoragePathIn(store, 9)));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(store, "/9/orphan")));
  EXPECT_EQ(Oids(ListStorages(store)), (std::vector<duckdb::idx_t>{7, 8}));
}

}  // namespace
}  // namespace sdb::catalog
