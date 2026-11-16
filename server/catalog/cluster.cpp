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

#include "catalog/cluster.h"

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/time/time.h>

#include <cstdlib>
#include <duckdb/catalog/default/default_coordinate_systems.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_table_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/default/default_views.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception/catalog_exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/function/built_in_functions.hpp>
#include <duckdb/function/function_list.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/parser/parsed_data/create_database_info.hpp>
#include <duckdb/parser/parsed_data/create_role_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/replication_lsn.h"
#include "catalog/schema.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/acl.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "query/config.h"

namespace sdb::catalog {
namespace {

struct PredefinedRole {
  duckdb::idx_t oid;
  std::string_view name;
  RoleOption options;
  std::span<const duckdb::idx_t> member_of;
};

#include "pg/catalog/generated/predefined_roles.gen.inc"

void RefuseDependents(duckdb::ClientContext& context, ClusterCatalog& cluster,
                      const duckdb::CatalogEntry& role) {
  std::vector<std::string> lines;
  std::vector<std::pair<duckdb::idx_t, size_t>> remote;
  const auto current = DefaultSereneDBCatalog(context);
  const auto local = current ? current->GetAttached().oid : pg::kInvalidOid;
  pg::VisitRoleReferences(
    context, role.oid,
    [&](const pg::RoleReference& reference,
        absl::FunctionRef<std::string()> describe) {
      if (reference.dbid != pg::kInvalidOid && reference.dbid != local) {
        if (remote.empty() || remote.back().first != reference.dbid) {
          remote.emplace_back(reference.dbid, 0);
        }
        ++remote.back().second;
        return;
      }
      lines.push_back(
        absl::StrCat(reference.deptype == 'o' ? "owner of " : "privileges for ",
                     describe()));
    });
  cluster.Set(duckdb::CatalogType::ROLE_ENTRY)
    .Scan(
      cluster.GetCatalogTransaction(context),
      [&](duckdb::CatalogEntry& member) {
        for (const auto& edge : member.Cast<RoleCatalogEntry>().MemberOf()) {
          if (edge.grantor == role.oid) {
            lines.push_back(absl::StrCat("privileges for membership of role ",
                                         member.name.GetIdentifierName(),
                                         " in role ",
                                         RolesOf(&context)->NameOf(edge.role)));
          }
        }
      });
  const auto databases =
    SereneDBDatabases(duckdb::DatabaseManager::Get(context));
  for (const auto& [dbid, count] : remote) {
    const auto database = absl::c_find_if(
      databases, [&](const auto& db) { return db->oid == dbid; });
    lines.push_back(absl::StrCat(count, count == 1 ? " object" : " objects",
                                 " in database ",
                                 (*database)->GetName().GetIdentifierName()));
  }
  if (lines.empty()) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                  ERR_MSG("role \"", role.name.GetIdentifierName(),
                          "\" cannot be dropped because some objects depend "
                          "on it"),
                  ERR_DETAIL(absl::StrJoin(lines, "\n")));
}

void AssignObjectOid(duckdb::Catalog& catalog, duckdb::CreateInfo& info) {
  if (!info.oid) {
    info.oid = duckdb::DatabaseManager::Get(catalog.GetDatabase()).NextOid();
  }
}

void AttachDatabaseCatalog(duckdb::ClientContext& context,
                           const DatabaseCatalogEntry& database, bool durable) {
  auto& manager = duckdb::DatabaseManager::Get(context);
  if (auto attached = manager.GetDatabase(database.name)) {
    if (attached->oid == database.oid) {
      return;
    }
    manager.DetachDatabase(context, database.name,
                           duckdb::OnEntryNotFound::RETURN_NULL);
  }
  duckdb::AttachInfo info;
  info.name = database.name;
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.create_catalog = [&](duckdb::AttachedDatabase& db) {
    db.oid = database.oid;
    return duckdb::InternalCatalog{
      .catalog =
        duckdb::make_uniq<SereneDBCatalog>(db, database.Directory(), durable),
      .transaction_manager = duckdb::make_uniq<TransactionManager>(db),
    };
  };
  manager.AttachDatabase(context, info, options);
}

void RequireDatabaseUnused(duckdb::ClientContext& context,
                           duckdb::AttachedDatabase& database) {
  if (duckdb::DatabaseManager::GetDefaultDatabase(context) ==
      database.GetName()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("cannot drop the currently open database"));
  }
  auto& catalog = database.GetCatalog().Cast<SereneDBCatalog>();
  size_t subscriptions = 0;
  catalog.Subscriptions().Scan(catalog.GetCatalogTransaction(context),
                               [&](duckdb::CatalogEntry&) { ++subscriptions; });
  if (subscriptions != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("database \"", database.GetName().GetIdentifierName(),
              "\" is being used by logical replication subscription"),
      ERR_DETAIL(subscriptions == 1 ? std::string{"There is 1 subscription."}
                                    : absl::StrCat("There are ", subscriptions,
                                                   " subscriptions.")));
  }
  const auto others = pg::ProgressRegistry::Instance().OtherSessions(
    static_cast<int64_t>(database.oid),
    ConnectionContext::Get(context).GetBackendPid(), absl::Seconds(5));
  if (others == 0) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_OBJECT_IN_USE),
    ERR_MSG("database \"", database.GetName().GetIdentifierName(),
            "\" is being accessed by other users"),
    ERR_DETAIL(others == 1 ? std::string{"There is 1 other session using "
                                         "the database."}
                           : absl::StrCat("There are ", others,
                                          " other sessions using the "
                                          "database.")));
}

}  // namespace

void ClusterCatalog::Initialize(bool load_builtin) {
  auto data = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  for (const auto* name : {"information_schema", "pg_catalog"}) {
    duckdb::CreateSchemaInfo info;
    info.SetQualifiedName(
      duckdb::QualifiedName({duckdb::Identifier{name}}, duckdb::Identifier()));
    info.internal = true;
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    CreateSchema(data, info);
  }
  ScanSchemas([&](Schema& schema) {
    schema.Set(duckdb::CatalogType::TABLE_ENTRY)
      .SetDefaults(
        duckdb::make_uniq<duckdb::DefaultViewGenerator>(*this, schema));
    schema.Set(duckdb::CatalogType::TABLE_FUNCTION_ENTRY)
      .SetDefaults(duckdb::make_uniq<duckdb::DefaultTableFunctionGenerator>(
        *this, schema));
    schema.Set(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY)
      .SetDefaults(
        duckdb::make_uniq<duckdb::DefaultFunctionGenerator>(*this, schema));
    schema.Set(duckdb::CatalogType::COORDINATE_SYSTEM_ENTRY)
      .SetDefaults(duckdb::make_uniq<duckdb::DefaultCoordinateSystemGenerator>(
        *this, schema));
    schema.Set(duckdb::CatalogType::TYPE_ENTRY)
      .SetDefaults(
        duckdb::make_uniq<duckdb::DefaultTypeGenerator>(*this, schema));
  });
  if (!load_builtin) {
    return;
  }
  duckdb::BuiltinFunctions builtin{data, *this};
  builtin.Initialize();
  duckdb::FunctionList::RegisterFunctions(*this, data);
}

EntrySet* ClusterCatalog::FindSet(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      return &_roles;
    case duckdb::CatalogType::DATABASE_ENTRY:
      return &_databases;
    case duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY:
      return &_origins;
    default:
      return BaseCatalog::FindSet(type);
  }
}

void ClusterCatalog::OnCommit() { PublishSecurity(); }

void ClusterCatalog::PublishSecurity() {
  auto& db = GetDatabase();
  auto& transactions = Transactions::Get();
  if (db.SecurityCurrent(transactions.CatalogVersion())) {
    return;
  }
  const CommittedView view;
  db.PublishSecurity(
    duckdb::SecuritySnapshot::Build(*this, view.Transaction(db)),
    transactions.CatalogEpoch(view.Snapshot()));
}

void ClusterCatalog::Bootstrap(duckdb::ClientContext& context,
                               bool default_database) {
  const auto transaction = GetCatalogTransaction(context);
  const duckdb::Identifier root{kRootRole};
  if (!_roles.Get(transaction, root)) {
    duckdb::CreateRoleInfo info;
    info.SetName(root);
    info.oid = pg::kRootUser;
    info.options = RoleOption::Superuser | RoleOption::Inherit |
                   RoleOption::CreateRole | RoleOption::CreateDb |
                   RoleOption::Login | RoleOption::Replication |
                   RoleOption::BypassRls;
    if (const char* password = std::getenv("POSTGRES_PASSWORD");
        password && *password) {
      info.password = StoredPassword(context, kRootRole, password);
      SDB_INFO(STARTUP, "bootstrap: initial password set for role '", kRootRole,
               "' from POSTGRES_PASSWORD");
    }
    DeclareModified(transaction, *this);
    _roles.Create(transaction,
                  duckdb::make_uniq<RoleCatalogEntry>(*this, info));
  }
  for (const auto& predefined : kPredefinedRoles) {
    const duckdb::Identifier name{predefined.name};
    if (_roles.Get(transaction, name)) {
      continue;
    }
    duckdb::CreateRoleInfo info;
    info.SetName(name);
    info.oid = predefined.oid;
    info.options = predefined.options;
    for (const auto role : predefined.member_of) {
      info.member_of.push_back({.role = role, .grantor = pg::kRootUser});
    }
    DeclareModified(transaction, *this);
    _roles.Create(transaction,
                  duckdb::make_uniq<RoleCatalogEntry>(*this, info));
  }
  const duckdb::Identifier postgres{irs::StaticStrings::kDefaultDatabase};
  if (default_database && !_databases.Get(transaction, postgres)) {
    duckdb::CreateDatabaseInfo info;
    info.SetName(postgres);
    info.oid = pg::kPgPostgresDatabase;
    info.permissions.owner = pg::kRootUser;
    CreateDatabase(transaction, info);
  }
}

void ClusterCatalog::Alter(duckdb::CatalogTransaction transaction,
                           duckdb::AlterInfo& info) {
  if (info.type == duckdb::AlterType::REASSIGN_OWNED) {
    ReassignOwned(transaction, info.Cast<duckdb::ReassignOwnedInfo>());
    return;
  }
  if (info.type == duckdb::AlterType::DROP_OWNED) {
    DropOwned(transaction, info.Cast<duckdb::DropOwnedInfo>());
    return;
  }
  if (info.type != duckdb::AlterType::ALTER_ROLE) {
    ApplyAlter(transaction, info);
    return;
  }
  auto& alter = info.Cast<duckdb::AlterRoleInfo>();
  if (alter.revoke && !alter.grant_role.empty() &&
      info.bind_mode == duckdb::AlterBindMode::BIND_ON_ALTER) {
    RevokeMembership(transaction, alter);
    return;
  }
  if (!HasOption(alter.set_options | alter.clear_options,
                 RoleOption::Superuser) ||
      !transaction.HasContext() ||
      info.bind_mode != duckdb::AlterBindMode::BIND_ON_ALTER) {
    ApplyAlter(transaction, info);
    return;
  }
  const auto& name = info.GetQualifiedName().Name();
  const auto previous = _roles.Get(transaction, name);
  const bool was_superuser =
    previous && previous->Cast<RoleCatalogEntry>().IsSuperuser();
  ApplyAlter(transaction, info);
  const auto& role = _roles.Get(transaction, name)->Cast<RoleCatalogEntry>();
  if (role.IsSuperuser() != was_superuser) {
    FollowOwnerStatus(transaction.GetContext(), role.oid, role.IsSuperuser());
  }
}

void ClusterCatalog::RevokeMembership(duckdb::CatalogTransaction transaction,
                                      duckdb::AlterRoleInfo& alter) {
  const auto granted =
    _roles.Get(transaction, duckdb::Identifier{alter.grant_role});
  const auto member = _roles.Get(transaction, alter.GetQualifiedName().Name());
  if (!granted || !member || !alter.grantor_id ||
      (alter.option_only && alter.admin_option != 0)) {
    ApplyAlter(transaction, alter);
    return;
  }
  struct Grant {
    duckdb::Identifier member;
    duckdb::idx_t member_oid;
    duckdb::idx_t grantor;
    bool admin;
  };
  enum class Plan : uint8_t {
    Keep,
    RevokeAdmin,
    Revoke,
  };
  std::vector<Grant> grants;
  _roles.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
    for (const auto& edge : entry.Cast<RoleCatalogEntry>().MemberOf()) {
      if (edge.role == granted->oid) {
        grants.push_back(
          {entry.name, entry.oid, edge.grantor, edge.admin_option});
      }
    }
  });
  const auto target = absl::c_find_if(grants, [&](const Grant& grant) {
    return grant.member_oid == member->oid && grant.grantor == alter.grantor_id;
  });
  if (target == grants.end()) {
    ApplyAlter(transaction, alter);
    return;
  }
  std::vector<Plan> plans(grants.size(), Plan::Keep);
  const auto plan = [&](this const auto& self, size_t index,
                        bool admin_only) -> void {
    if (plans[index] == Plan::Revoke ||
        (plans[index] == Plan::RevokeAdmin && admin_only)) {
      return;
    }
    if (!grants[index].admin) {
      if (!admin_only) {
        plans[index] = Plan::Revoke;
      }
      return;
    }
    plans[index] = admin_only ? Plan::RevokeAdmin : Plan::Revoke;
    const auto holder = grants[index].member_oid;
    for (size_t i = 0; i < grants.size(); ++i) {
      if (grants[i].member_oid == holder && grants[i].admin &&
          plans[i] == Plan::Keep) {
        return;
      }
    }
    for (size_t i = 0; i < grants.size(); ++i) {
      if (grants[i].grantor != holder || plans[i] == Plan::Revoke) {
        continue;
      }
      if (!alter.cascade) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                        ERR_MSG("dependent privileges exist"),
                        ERR_HINT("Use CASCADE to revoke them too."));
      }
      self(i, false);
    }
  };
  const size_t revoked = target - grants.begin();
  plan(revoked, alter.option_only);
  ApplyAlter(transaction, alter);
  for (size_t i = 0; i < grants.size(); ++i) {
    if (i == revoked || plans[i] != Plan::Revoke) {
      continue;
    }
    duckdb::AlterRoleInfo cascaded{grants[i].member};
    cascaded.grant_role_id = granted->oid;
    cascaded.grantor_id = grants[i].grantor;
    cascaded.revoke = true;
    ApplyAlter(transaction, cascaded);
  }
}

void ClusterCatalog::ApplyAlter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  const auto type = info.GetCatalogType();
  if (type != duckdb::CatalogType::ROLE_ENTRY &&
      type != duckdb::CatalogType::DATABASE_ENTRY) {
    BaseCatalog::Alter(transaction, info);
    return;
  }
  AlterIn(Set(type), transaction, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) {
  DeclareModified(transaction, *this);
  if (!IsPredefinedRole(info.oid)) {
    RequireUnreservedRoleName(info.GetQualifiedName().Name());
  }
  AssignObjectOid(*this, info);
  auto& context = transaction.GetContext();
  if (!info.password.empty()) {
    info.password = StoredPassword(
      context, info.GetQualifiedName().Name().GetIdentifierName(),
      info.password);
  }
  auto entry = duckdb::make_uniq<RoleCatalogEntry>(*this, info);
  duckdb::optional_ptr<duckdb::CatalogEntry> role = entry.get();
  if (!_roles.Create(transaction, std::move(entry))) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("role \"", info.GetQualifiedName().Name().GetIdentifierName(),
              "\" already exists"));
  }
  const auto grant = [&](const duckdb::Identifier& member,
                         const duckdb::Identifier& granted, bool admin) {
    duckdb::AlterRoleInfo alter{member};
    alter.grant_role = granted.GetIdentifierName();
    alter.admin_option = admin;
    ApplyAlter(transaction, alter);
  };
  for (const auto& name : info.in_roles) {
    grant(role->name, name, false);
  }
  for (const auto& name : info.role_members) {
    grant(name, role->name, false);
  }
  for (const auto& name : info.admin_members) {
    grant(name, role->name, true);
  }
  if (const auto creator = GrantorOfMembership(context);
      creator != pg::kRootUser) {
    duckdb::AlterRoleInfo alter{
      duckdb::Identifier{RolesOf(&context)->NameOf(creator)}};
    alter.grant_role = role->name.GetIdentifierName();
    alter.grantor_id = pg::kRootUser;
    alter.admin_option = true;
    alter.inherit_option = false;
    alter.set_option = false;
    ApplyAlter(transaction, alter);
    if (const auto self_grant = GetCreateroleSelfGrant(context);
        self_grant.inherit || self_grant.set) {
      duckdb::AlterRoleInfo own{alter.GetQualifiedName().Name()};
      own.grant_role = alter.grant_role;
      own.grantor_id = creator;
      own.admin_option = false;
      own.inherit_option = self_grant.inherit;
      own.set_option = self_grant.set;
      ApplyAlter(transaction, own);
    }
  }
  return role;
}

void ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  auto& context = transaction.GetContext();
  const auto& session = ConnectionContext::Get(context);
  std::vector<duckdb::idx_t> dropped;
  const auto check = [&](const duckdb::DropInfo& target) {
    const auto& name = target.GetQualifiedName().Name();
    auto role = _roles.Get(transaction, name);
    if (!role) {
      if (target.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
        return;
      }
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("role \"", name.GetIdentifierName(), "\" does not exist"));
    }
    if (role->oid == session.GetRoleId() ||
        role->oid == session.GetSessionRoleId() ||
        role->oid == session.GetLoginRoleId()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("current user cannot be dropped"));
    }
    if (role->oid == pg::kRootUser) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OBJECT_IN_USE),
        ERR_MSG("cannot drop role \"", role->name.GetIdentifierName(),
                "\" because it is required by the database system"));
    }
    if (!absl::c_linear_search(dropped, role->oid)) {
      dropped.push_back(role->oid);
    }
  };
  check(info);
  for (const auto& target : info.additional_drops) {
    check(*target);
  }
  struct Membership {
    duckdb::Identifier member;
    duckdb::idx_t role;
  };
  std::vector<Membership> memberships;
  std::vector<duckdb::Identifier> names;
  _roles.Scan(transaction, [&](duckdb::CatalogEntry& other) {
    const bool listed = absl::c_linear_search(dropped, other.oid);
    if (listed) {
      names.push_back(other.name);
    }
    std::vector<duckdb::idx_t> granted;
    for (const auto& membership : other.Cast<RoleCatalogEntry>().MemberOf()) {
      if ((listed || absl::c_linear_search(dropped, membership.role)) &&
          !absl::c_linear_search(granted, membership.role)) {
        granted.push_back(membership.role);
        memberships.push_back({other.name, membership.role});
      }
    }
  });
  for (const auto& membership : memberships) {
    duckdb::AlterRoleInfo alter{membership.member};
    alter.grant_role_id = membership.role;
    alter.revoke = true;
    ApplyAlter(transaction, alter);
  }
  struct Settings {
    duckdb::Identifier database;
    duckdb::idx_t role;
  };
  std::vector<Settings> settings;
  _databases.Scan(transaction, [&](duckdb::CatalogEntry& database) {
    for (const auto role : dropped) {
      if (database.Cast<DatabaseCatalogEntry>().SettingsOf(role)) {
        settings.push_back({database.name, role});
      }
    }
  });
  for (const auto& setting : settings) {
    duckdb::AlterDatabaseSettingsInfo alter{setting.database};
    alter.role_id = setting.role;
    alter.reset_all_config = true;
    ApplyAlter(transaction, alter);
  }
  for (const auto& name : names) {
    RefuseDependents(context, *this, *_roles.Get(transaction, name));
  }
  for (const auto& name : names) {
    _roles.Drop(transaction, name);
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateDatabase(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  for (auto& [name, value] : info.options) {
    value = connector::ValidateSetting(transaction.GetContext(), name, value);
  }
  DeclareModified(transaction, *this);
  AssignObjectOid(*this, info);
  auto entry = duckdb::make_uniq<DatabaseCatalogEntry>(*this, info);
  auto& database = *entry;
  const auto attached = duckdb::DatabaseManager::Get(transaction.GetContext())
                          .GetDatabase(info.GetQualifiedName().Name());
  if ((attached && !IsSereneDB(attached->GetCatalog())) ||
      !_databases.Create(transaction, std::move(entry))) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
      ERR_MSG("database \"", info.GetQualifiedName().Name().GetIdentifierName(),
              "\" already exists"));
  }
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  AttachDatabaseCatalog(transaction.GetContext(), database,
                        !_layout.InMemory());
  return &database;
}

void ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  duckdb::DropInfo& info) {
  auto& context = transaction.GetContext();
  ConnectionContext::Get(context).PreventInTransactionBlock("DROP DATABASE");
  if (auto database = duckdb::DatabaseManager::Get(context).GetDatabase(
        info.GetQualifiedName().Name());
      database && IsSereneDB(database->GetCatalog()) &&
      !database->IsReadOnly()) {
    RequireDatabaseUnused(context, *database);
  }
  if (auto entry =
        _databases.Get(transaction, info.GetQualifiedName().Name())) {
    DropDependents(transaction, *entry, info.cascade, info.dropped_together);
  }
  if (!DropFrom(_databases, transaction, duckdb::CatalogType::DATABASE_ENTRY,
                info)) {
    return;
  }
  duckdb::DatabaseManager::Get(context).DetachDatabase(
    context, info.GetQualifiedName().Name(),
    duckdb::OnEntryNotFound::RETURN_NULL);
}

duckdb::optional_ptr<duckdb::CatalogEntry>
ClusterCatalog::CreateReplicationOrigin(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateReplicationOriginInfo& info) {
  AssignObjectOid(*this, info);
  return AddTo(_origins, transaction,
               duckdb::make_uniq<ReplicationOriginEntry>(*this, info),
               info.on_conflict);
}

void ClusterCatalog::DropReplicationOrigin(
  duckdb::CatalogTransaction transaction, duckdb::DropInfo& info) {
  DropFrom(_origins, transaction, duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY,
           info);
}

void ClusterCatalog::ReplayCreate(duckdb::ClientContext& context,
                                  duckdb::unique_ptr<duckdb::CreateInfo> info) {
  const auto transaction = GetCatalogTransaction(context);
  DeclareModified(transaction, *this);
  switch (info->type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      _roles.Create(transaction,
                    duckdb::make_uniq<RoleCatalogEntry>(
                      *this, info->Cast<duckdb::CreateRoleInfo>()));
      return;
    case duckdb::CatalogType::DATABASE_ENTRY: {
      auto entry = duckdb::make_uniq<DatabaseCatalogEntry>(
        *this, info->Cast<duckdb::CreateDatabaseInfo>());
      auto& database = *entry;
      _databases.Create(transaction, std::move(entry));
      AttachDatabaseCatalog(context, database, !_layout.InMemory());
      return;
    }
    case duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY:
      CreateReplicationOrigin(
        transaction, info->Cast<duckdb::CreateReplicationOriginInfo>());
      return;
    default:
      BaseCatalog::ReplayCreate(context, std::move(info));
  }
}

void ClusterCatalog::ReplayDrop(duckdb::ClientContext& context,
                                duckdb::CatalogEntry& entry) {
  const auto type = entry.type;
  if (type != duckdb::CatalogType::ROLE_ENTRY &&
      type != duckdb::CatalogType::DATABASE_ENTRY &&
      type != duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY) {
    BaseCatalog::ReplayDrop(context, entry);
    return;
  }
  const auto transaction = GetCatalogTransaction(context);
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  const auto name = entry.name;
  Set(type).Drop(transaction, name);
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    duckdb::DatabaseManager::Get(context).DetachDatabase(
      context, name, duckdb::OnEntryNotFound::RETURN_NULL);
  }
}

ClusterCatalog& ClusterOf(duckdb::ClientContext& context) {
  return duckdb::Catalog::GetSystemCatalog(context).Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db) {
  return duckdb::Catalog::GetSystemCatalog(db).Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf() {
  return ClusterOf(irs::DuckDBEngine::Instance().instance());
}

duckdb::shared_ptr<const duckdb::SecuritySnapshot> RolesOf(
  duckdb::ClientContext* context) {
  if (context && context->security) {
    return context->security;
  }
  auto& cluster = context ? ClusterOf(*context->db) : ClusterOf();
  auto& db = cluster.GetDatabase();
  if (auto roles = db.GetSecurity()) {
    return roles;
  }
  cluster.PublishSecurity();
  return db.GetSecurity();
}

}  // namespace sdb::catalog
