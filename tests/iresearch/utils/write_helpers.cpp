////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "utils/write_helpers.hpp"

#include <absl/base/internal/endian.h>

#include <bit>
#include <iresearch/utils/numeric_utils.hpp>

namespace tests {

uint16_t ReadU16(duckdb::InputFileReader& in) {
  irs::byte_type bytes[sizeof(uint16_t)];
  in.ReadData(bytes, sizeof bytes);
  return absl::little_endian::Load16(bytes);
}

void WriteZvfloat(duckdb::WriteStream& out, float_t v) {
  const int32_t iv = irs::numeric_utils::Ftoi32(v);

  if (iv >= -1 && iv <= 125) {
    // small signed values between [-1 and 125]
    out.Write<irs::byte_type>(static_cast<irs::byte_type>(0x80 | (1 + iv)));
  } else if (!std::signbit(v)) {
    // positive value
    out.Write<uint32_t>(std::byteswap(iv));
  } else {
    // negative value
    out.Write<irs::byte_type>(0xFF);
    out.Write<uint32_t>(iv);
  }
};

float_t ReadZvfloat(duckdb::InputFileReader& in) {
  const uint32_t b = in.Read<irs::byte_type>();

  if (0xFF == b) {
    // negative value
    return irs::numeric_utils::I32tof(in.Read<int32_t>());
  } else if (0 != (b & 0x80)) {
    // small signed value
    return float_t((b & 0x7F) - 1);
  }

  // positive float (ensure read order)
  const auto part = ReadU16(in);

  return irs::numeric_utils::I32tof((b << 24) | (std::byteswap(part) << 8) |
                                    uint32_t(in.Read<irs::byte_type>()));
}

void WriteZvdouble(duckdb::WriteStream& out, double_t v) {
  const int64_t lv = irs::numeric_utils::Dtoi64(v);

  if (lv > -1 && lv <= 124) {
    // small signed values between [-1 and 124]
    out.Write<irs::byte_type>(static_cast<irs::byte_type>(0x80 | (1 + lv)));
  } else {
    const float_t fv = static_cast<float_t>(v);

    if (static_cast<double_t>(fv) == v) {
      out.Write<irs::byte_type>(0xFE);
      out.Write<uint32_t>(irs::numeric_utils::Ftoi32(fv));
    } else if (!std::signbit(v)) {
      // positive value
      out.Write<uint64_t>(std::byteswap(lv));
    } else {
      // negative value
      out.Write<irs::byte_type>(0xFF);
      out.Write<uint64_t>(lv);
    }
  }
}

double_t ReadZvdouble(duckdb::InputFileReader& in) {
  const uint64_t b = in.Read<irs::byte_type>();

  if (0xFF == b) {
    // negative value
    return irs::numeric_utils::I64tod(in.Read<int64_t>());
  } else if (0xFE == b) {
    // float value
    return static_cast<double_t>(
      irs::numeric_utils::I32tof(in.Read<int32_t>()));
  } else if (0 != (b & 0x80)) {
    // small signed value
    return double_t((b & 0x7F) - 1);
  }

  // positive double (ensure read order)
  const auto part1 = uint64_t(std::byteswap(uint32_t(in.Read<int32_t>())));
  const auto part2 = uint64_t(std::byteswap(ReadU16(in)));

  return irs::numeric_utils::I64tod((b << 56) | (part1 << 24) | (part2 << 8) |
                                    uint64_t(in.Read<irs::byte_type>()));
}

}  // namespace tests
