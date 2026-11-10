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

struct AheadScratch {
  irs::ColFilterStateCache& states;
  irs::ColFilterClassification& cls;
  duckdb::ClientContext& context;
  std::vector<duckdb::ExternalFileCacheRange>& extents;
};

bool RowGroupLive(const ScanGlobalState& g, const SegmentWork& work,
                  const RowGroupChecks& checks, UnitRows rows, uint64_t rg) {
  if (work.live.size() != 0) {
    return work.live.test(rg);
  }
  return RowsLive(checks, std::max(rows.begin, rg * g.rg_size),
                  std::min(rows.end, (rg + 1) * g.rg_size));
}

uint64_t CollectRowGroup(const ScanGlobalState& g,
                         const irs::ColReader& col_reader, UnitRows rows,
                         uint64_t rg,
                         std::vector<duckdb::ExternalFileCacheRange>& extents) {
  const auto from = extents.size();
  const auto begin = std::max(rows.begin, rg * g.rg_size);
  const auto last = std::min(rows.end, (rg + 1) * g.rg_size) - 1;
  for (const auto field : g.ahead_fields) {
    if (const auto* column = col_reader.Column(field)) {
      column->CollectBlocks(begin, last, extents);
    }
  }
  uint64_t bytes = 0;
  for (auto i = from; i != extents.size(); ++i) {
    bytes += extents[i].size;
  }
  return bytes;
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
  RowGroupChecks checks;
  if (work.live.size() == 0 && !g.col_filters.empty()) {
    ClassifySegmentColFilters(sub, g, g.col_filters, scratch.states,
                              scratch.cls, false);
    if (scratch.cls.segment_dead) {
      return;
    }
    checks = MakeRowGroupChecks(*col_reader, scratch.cls.active, docs,
                                scratch.context, scratch.states);
  }
  const UnitRows rows{.begin = 0, .end = docs};
  scratch.extents.clear();
  for (auto rg = from; rg != to; ++rg) {
    if (RowGroupLive(g, work, checks, rows, rg)) {
      CollectRowGroup(g, *col_reader, rows, rg, scratch.extents);
    }
  }
  if (!scratch.extents.empty()) {
    col_reader->PrefetchExtents(scratch.extents);
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
  if (g.col_filters.empty()) {
    for (const auto& projection : g.cs_projections) {
      g.ahead_fields.push_back(
        static_cast<irs::field_id>(projection.column_id));
    }
  }
  if (g.ahead_fields.empty()) {
    return;
  }
  g.ahead_bytes =
    std::clamp(kScanAhead / g.workers, kMinWorkerAhead, kMaxWorkerAhead);
  g.ahead_before.assign(g.live_segments + 1, 0);
  for (uint32_t i = 0; i != g.live_segments; ++i) {
    g.ahead_before[i + 1] =
      g.ahead_before[i] + g.segments[g.segment_order[i]].rg_count;
  }
  irs::ColFilterStateCache states;
  irs::ColFilterClassification cls;
  std::vector<duckdb::ExternalFileCacheRange> extents;
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
      bytes +=
        CollectRowGroup(g, *col_reader, {.begin = 0, .end = docs}, rg, extents);
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

void StartReadAhead(const ScanGlobalState& g, ScanLocalState& l,
                    bool whole_unit) {
  l.ahead_check = std::numeric_limits<uint64_t>::max();
  const auto& sub = (*g.reader)[l.unit.seg];
  const auto* col_reader = sub.GetColReader();
  const auto rows = g.RowsOf(l.unit);
  if (!col_reader || rows.begin == rows.end) {
    return;
  }
  l.ahead_checks.clear();
  if (g.segments[l.unit.seg].live.size() == 0) {
    l.ahead_checks =
      MakeRowGroupChecks(*col_reader, l.seg_cls.active, sub.Meta().docs_count,
                         *g.client_context, l.filter_states);
  }
  l.ahead_live.clear();
  l.ahead_rg = rows.begin / g.rg_size;
  l.ahead_rgs = 0;
  l.ahead_budget = whole_unit ? kMaxUnitAhead : g.ahead_bytes;
  MoveReadAhead(g, l, rows.begin);
  if (whole_unit) {
    l.ahead_check = std::numeric_limits<uint64_t>::max();
  }
}

void MoveReadAhead(const ScanGlobalState& g, ScanLocalState& l, uint64_t row) {
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
    if (!RowGroupLive(g, work, l.ahead_checks, rows, next)) {
      continue;
    }
    const auto bytes =
      CollectRowGroup(g, *col_reader, rows, next, l.ahead_extents);
    l.ahead_live.push_back(next);
    if (l.ahead_rgs == 0) {
      l.ahead_rgs = std::clamp<uint64_t>(
        l.ahead_budget / std::max<uint64_t>(bytes, 1), 1, kMaxAheadRowGroups);
    }
  }
  if (!l.ahead_extents.empty()) {
    col_reader->PrefetchExtents(l.ahead_extents);
  }
  l.ahead_check = l.ahead_rg == end ? std::numeric_limits<uint64_t>::max()
                                    : (rg + 1) * g.rg_size;
}

void ExtendReadAhead(ScanGlobalState& g, ScanLocalState& l) {
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

}  // namespace sdb::connector
