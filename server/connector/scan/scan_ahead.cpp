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

#include <algorithm>
#include <atomic>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

constexpr uint64_t kScanStart = duckdb::ExternalFileCache::MAX_FILE_IN_FLIGHT;
constexpr uint64_t kSampleRowGroups = 8;
constexpr uint64_t kScanAhead = uint64_t{256} << 20;
constexpr uint64_t kMinWorkerAhead = uint64_t{4} << 20;
constexpr uint64_t kMaxWorkerAhead = uint64_t{32} << 20;
constexpr uint64_t kMaxUnitAhead = uint64_t{64} << 20;
constexpr uint64_t kMaxAheadRowGroups = 256;
constexpr uint64_t kProjectedPass = 1024;

struct AheadScratch {
  irs::ColFilterStateCache& states;
  irs::ColFilterClassification& cls;
  duckdb::ClientContext& context;
  std::vector<duckdb::FileRange>& extents;
};

UnitRows RowGroupRows(const ScanGlobalState& g, UnitRows rows, uint64_t rg) {
  return {.begin = std::max(rows.begin, rg * g.rg_size),
          .end = std::min(rows.end, (rg + 1) * g.rg_size)};
}

bool RowGroupLive(const ScanGlobalState& g, const SegmentWork& work,
                  const RowGroupChecks& checks, UnitRows rows, uint64_t rg) {
  if (work.live.size() != 0) {
    return work.live.test(rg);
  }
  const auto group = RowGroupRows(g, rows, rg);
  return RowsLive(checks, group.begin, group.end);
}

uint64_t CollectRowGroup(const irs::ColReader& col_reader,
                         std::span<const irs::field_id> fields, UnitRows group,
                         std::vector<duckdb::FileRange>& extents) {
  const auto from = extents.size();
  for (const auto field : fields) {
    if (const auto* column = col_reader.Column(field)) {
      column->CollectBlocks(group.begin, group.end - 1, extents);
    }
  }
  uint64_t bytes = 0;
  for (auto i = from; i != extents.size(); ++i) {
    bytes += extents[i].size;
  }
  return bytes;
}

bool Cached(const irs::ColReader& col_reader,
            std::span<const duckdb::FileRange> extents) {
  return absl::c_all_of(extents, [&](const auto& extent) {
    return col_reader.File()->Warm(extent.offset, extent.size) == extent.size;
  });
}

void DeclareRowGroups(ScanGlobalState& g, const AheadScratch& scratch,
                      uint32_t seg, uint64_t from, uint64_t to) {
  const auto& sub = (*g.reader)[seg];
  const auto* col_reader = sub.GetColReader();
  const uint64_t docs = sub.Meta().docs_count;
  if (!col_reader || docs == 0 || from >= to) {
    return;
  }
  const auto& work = g.segments[seg];
  const bool classify = work.live.size() == 0 && !g.col_filters.empty();
  bool classified = false;
  RowGroupChecks checks;
  const UnitRows rows{.begin = 0, .end = docs};
  auto& extents = scratch.extents;
  extents.clear();
  for (auto rg = from; rg != to; ++rg) {
    const auto kept = extents.size();
    CollectRowGroup(*col_reader, g.ahead_fields, RowGroupRows(g, rows, rg),
                    extents);
    if (Cached(*col_reader, std::span{extents}.subspan(kept))) {
      extents.resize(kept);
      continue;
    }
    if (classify && !classified) {
      ClassifySegmentColFilters(sub, g, g.col_filters, scratch.states,
                                scratch.cls, false);
      if (scratch.cls.segment_dead) {
        return;
      }
      checks = MakeRowGroupChecks(*col_reader, scratch.cls.active, docs,
                                  scratch.context, scratch.states);
      classified = true;
    }
    if (!RowGroupLive(g, work, checks, rows, rg)) {
      extents.resize(kept);
    }
  }
  if (!extents.empty()) {
    col_reader->File()->Prefetch(extents);
  }
}

void DeclareNextSegments(ScanGlobalState& g, const AheadScratch& scratch) {
  const auto claimed =
    std::min(g.next_segment.load(std::memory_order_relaxed), g.live_segments);
  auto declared = g.ahead_segment.load(std::memory_order_relaxed);
  for (;;) {
    const auto begin = std::max(declared, claimed);
    auto end = begin;
    while (end != g.live_segments &&
           g.ahead_before[end] - g.ahead_before[claimed] < g.ahead_rgs) {
      ++end;
    }
    if (end <= declared) {
      return;
    }
    if (!g.ahead_segment.compare_exchange_weak(declared, end,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
      continue;
    }
    for (auto i = begin; i != end; ++i) {
      const auto seg = g.segment_order[i];
      auto& work = g.segments[seg];
      const auto count = static_cast<uint32_t>(std::min<uint64_t>(
        work.rg_count,
        g.ahead_rgs - (g.ahead_before[i] - g.ahead_before[claimed])));
      DeclareRowGroups(g, scratch, seg, 0, count);
      auto ahead = work.ahead_rg.load(std::memory_order_relaxed);
      while (ahead < count && !work.ahead_rg.compare_exchange_weak(
                                ahead, count, std::memory_order_relaxed,
                                std::memory_order_relaxed)) {
      }
    }
    return;
  }
}

void DeclareNextRowGroups(ScanGlobalState& g, const AheadScratch& scratch,
                          uint32_t seg) {
  auto& work = g.segments[seg];
  const auto next = work.next_claim.load(std::memory_order_relaxed);
  const uint64_t claimed =
    work.claims.empty()
      ? std::min(next, work.rg_count)
      : work.claims[std::min<size_t>(next, work.claims.size() - 1)].rg;
  const auto end = static_cast<uint32_t>(
    std::min<uint64_t>(work.rg_count, claimed + g.ahead_rgs));
  auto declared = work.ahead_rg.load(std::memory_order_relaxed);
  while (declared < end) {
    if (work.ahead_rg.compare_exchange_weak(declared, end,
                                            std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
      DeclareRowGroups(g, scratch, seg, std::max<uint64_t>(declared, claimed),
                       end);
      return;
    }
  }
}

void CollectProjected(const ScanGlobalState& g, ScanLocalState& l,
                      const irs::ColReader& col_reader, UnitRows rows,
                      uint64_t row) {
  if (row < l.projected_until) {
    return;
  }
  const auto rg = row / g.rg_size;
  auto group = RowGroupRows(g, rows, rg);
  group.begin = row;
  const auto bytes =
    CollectRowGroup(col_reader, g.ahead_projections, group, l.ahead_extents);
  if (l.projected_rgs == 0 && group.end - group.begin == g.rg_size) {
    l.projected_rgs = std::clamp<uint64_t>(
      g.ahead_bytes / std::max<uint64_t>(bytes, 1), 1, kMaxAheadRowGroups);
  }
  l.projected_until = (rg + 1) * g.rg_size;
}

void CollectProjectedAhead(ScanGlobalState& g, ScanLocalState& l,
                           const irs::ColReader& col_reader, uint64_t rg) {
  auto& work = g.segments[l.unit.seg];
  const UnitRows rows{.begin = 0,
                      .end = (*g.reader)[l.unit.seg].Meta().docs_count};
  const auto end = static_cast<uint32_t>(std::min<uint64_t>(
    work.rg_count, rg + 1 + std::max<uint64_t>(l.projected_rgs, 1)));
  auto declared = work.projected_rg.load(std::memory_order_relaxed);
  while (declared < end) {
    if (!work.projected_rg.compare_exchange_weak(declared, end,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_relaxed)) {
      continue;
    }
    for (auto next = std::max<uint64_t>(declared, rg + 1); next != end;
         ++next) {
      if (RowGroupLive(g, work, l.ahead_checks, rows, next)) {
        CollectRowGroup(col_reader, g.ahead_projections,
                        RowGroupRows(g, rows, next), l.ahead_extents);
      }
    }
    return;
  }
}

void DeclareBatch(ScanGlobalState& g, ScanLocalState& l,
                  const AheadScratch& scratch) {
  const auto begin = std::max(
    {l.ahead_batch, l.batch_next,
     std::min(g.ahead_segment.load(std::memory_order_relaxed), l.batch_end)});
  auto end = begin;
  while (end < l.batch_end &&
         g.ahead_before[end] - g.ahead_before[l.batch_next] <
           g.ahead_worker_rgs) {
    ++end;
  }
  for (auto i = begin; i < end; ++i) {
    const auto seg = g.segment_order[i];
    DeclareRowGroups(g, scratch, seg, 0, g.segments[seg].rg_count);
  }
  l.ahead_batch = std::max(l.ahead_batch, end);
}

}  // namespace

void PlanReadAhead(ScanGlobalState& g, duckdb::ClientContext& context) {
  for (const auto& filter : g.col_filters) {
    if (!filter.is_score && !filter.zonemap_only) {
      g.ahead_fields.push_back(filter.field);
    }
  }
  for (const auto& projection : g.cs_projections) {
    const auto field = static_cast<irs::field_id>(projection.column_id);
    if (g.col_filters.empty()) {
      g.ahead_fields.push_back(field);
    } else if (!absl::c_linear_search(g.ahead_fields, field)) {
      g.ahead_projections.push_back(field);
    }
  }
  if (g.ahead_fields.empty()) {
    return;
  }
  g.ahead_projected =
    !g.Ordered() && absl::c_none_of(g.col_filters, [](const auto& filter) {
      return filter.is_dynamic;
    });
  g.ahead_bytes =
    std::clamp(kScanAhead / g.workers, kMinWorkerAhead, kMaxWorkerAhead);
  g.ahead_before.assign(g.live_segments + 1, 0);
  for (uint32_t i = 0; i != g.live_segments; ++i) {
    g.ahead_before[i + 1] =
      g.ahead_before[i] + g.segments[g.segment_order[i]].rg_count;
  }
  irs::ColFilterStateCache states;
  irs::ColFilterClassification cls;
  std::vector<duckdb::FileRange> extents;
  uint64_t rg_bytes = 0;
  for (uint32_t i = 0; i != g.live_segments && rg_bytes == 0; ++i) {
    const auto seg = g.segment_order[i];
    const auto& sub = (*g.reader)[seg];
    const auto* col_reader = sub.GetColReader();
    const uint64_t docs = sub.Meta().docs_count;
    if (!col_reader || docs == 0) {
      continue;
    }
    const auto rgs =
      std::min<uint64_t>(g.segments[seg].rg_count, kSampleRowGroups);
    extents.clear();
    uint64_t bytes = 0;
    for (uint64_t rg = 0; rg != rgs; ++rg) {
      bytes += CollectRowGroup(*col_reader, g.ahead_fields,
                               RowGroupRows(g, {.begin = 0, .end = docs}, rg),
                               extents);
    }
    rg_bytes = bytes / rgs;
  }
  rg_bytes = std::max<uint64_t>(rg_bytes, 1);
  g.ahead_rgs = static_cast<uint32_t>(
    std::clamp<uint64_t>(kScanStart / rg_bytes, 1, kMaxAheadRowGroups));
  g.ahead_worker_rgs = static_cast<uint32_t>(
    std::clamp<uint64_t>(g.ahead_bytes / rg_bytes, 1, kMaxAheadRowGroups));
  DeclareNextSegments(g, {states, cls, context, extents});
}

void StartReadAhead(ScanGlobalState& g, ScanLocalState& l) {
  l.ahead_check = std::numeric_limits<uint64_t>::max();
  l.projected_until = std::numeric_limits<uint64_t>::max();
  const auto& sub = (*g.reader)[l.unit.seg];
  const auto* col_reader = sub.GetColReader();
  if (const auto rows = g.RowsOf(l.unit);
      col_reader && rows.begin != rows.end) {
    l.ahead_checks.clear();
    if (g.segments[l.unit.seg].live.size() == 0) {
      l.ahead_checks =
        MakeRowGroupChecks(*col_reader, l.seg_cls.active, sub.Meta().docs_count,
                           *g.client_context, l.filter_states);
    }
    l.ahead_live.clear();
    l.ahead_rg = rows.begin / g.rg_size;
    l.ahead_rgs = 0;
    if (!g.ahead_projections.empty()) {
      l.projected_until = rows.begin;
    }
    l.projected_rgs = 0;
    MoveReadAhead(g, l, rows.begin);
  }
  if (g.Ordered()) {
    return;
  }
  const AheadScratch scratch{l.filter_states, l.ahead_cls, *g.client_context,
                             l.ahead_extents};
  if (l.unit.whole) {
    DeclareBatch(g, l, scratch);
  } else {
    DeclareNextRowGroups(g, scratch, l.unit.seg);
  }
  DeclareNextSegments(g, scratch);
}

void MoveReadAhead(ScanGlobalState& g, ScanLocalState& l, uint64_t row) {
  const auto* col_reader = (*g.reader)[l.unit.seg].GetColReader();
  const auto rows = g.RowsOf(l.unit);
  SDB_ASSERT(col_reader && rows.begin <= row && row < rows.end);
  const auto& work = g.segments[l.unit.seg];
  const auto rg = row / g.rg_size;
  const auto end = (rows.end - 1) / g.rg_size + 1;
  l.ahead_live.erase(l.ahead_live.begin(),
                     absl::c_lower_bound(l.ahead_live, rg));
  l.ahead_rg = std::max(l.ahead_rg, rg);
  l.ahead_extents.clear();
  while (l.ahead_rg != end &&
         (l.ahead_rgs == 0 || l.ahead_live.size() < l.ahead_rgs)) {
    const auto next = l.ahead_rg++;
    const auto kept = l.ahead_extents.size();
    const auto bytes =
      CollectRowGroup(*col_reader, g.ahead_fields, RowGroupRows(g, rows, next),
                      l.ahead_extents);
    if (Cached(*col_reader, std::span{l.ahead_extents}.subspan(kept))) {
      l.ahead_extents.resize(kept);
    } else if (!RowGroupLive(g, work, l.ahead_checks, rows, next)) {
      l.ahead_extents.resize(kept);
      continue;
    }
    l.ahead_live.push_back(next);
    if (l.ahead_rgs == 0) {
      const auto budget =
        g.shape == ScanShape::Count ? kMaxUnitAhead : g.ahead_bytes;
      l.ahead_rgs = std::clamp<uint64_t>(budget / std::max<uint64_t>(bytes, 1),
                                         1, kMaxAheadRowGroups);
    }
  }
  const auto passed =
    l.produced_rows - std::exchange(l.ahead_produced, l.produced_rows);
  if (passed * kProjectedPass >= g.rg_size && !g.ahead_projections.empty()) {
    CollectProjected(g, l, *col_reader, rows, row);
    if (g.ahead_projected) {
      CollectProjectedAhead(g, l, *col_reader, rg);
    }
  }
  if (!l.ahead_extents.empty()) {
    col_reader->File()->Prefetch(l.ahead_extents);
  }
  l.ahead_check = l.ahead_rg == end && g.ahead_projections.empty()
                    ? std::numeric_limits<uint64_t>::max()
                    : (rg + 1) * g.rg_size;
}

void DeclareProjected(const ScanGlobalState& g, ScanLocalState& l,
                      uint64_t row) {
  const auto* col_reader = (*g.reader)[l.unit.seg].GetColReader();
  const auto rows = g.RowsOf(l.unit);
  SDB_ASSERT(col_reader && rows.begin <= row && row < rows.end);
  l.ahead_extents.clear();
  CollectProjected(g, l, *col_reader, rows, row);
  if (!l.ahead_extents.empty()) {
    col_reader->File()->Prefetch(l.ahead_extents);
  }
}

}  // namespace sdb::connector
