// block_codec.h — Self-describing on-the-wire/on-disk serialization of a Block.
// Plane: SHARED.
//
// WHY THIS IS A SHARED MODULE (not SSD-private): the byte layout produced here
// is the single canonical representation of a Block as an opaque blob. The SSD
// tier stores it verbatim as the LevelDB value; the future (stage-2) TCP
// transport will wrap this same blob in an outer envelope (frame length +
// opcode + BlockKey) WITHOUT touching the blob's interior. So the codec owns
// the blob ONLY — never the key, opcode, or frame length; those belong to the
// caller.
//
// WHY THE SHAPE METADATA IS INLINED (not split to the control plane): tidepool
// treats the KV payload as opaque bytes (like Mooncake Store); the SHAPE
// metadata (num_tokens/num_layers/dtype_size/kv_heads/...) is the only thing
// needed to *interpret* that payload, so a persisted blob must carry it to be
// independently readable. The LocalIndex that would otherwise hold shape info
// is process memory and volatile: after a node restart the index is gone and
// the SSD holds nothing but bytes. A self-contained blob (Mooncake's opaque
// self-describing object model) is the only thing that survives that restart.
// Do NOT move shape metadata to a side record or the control plane. Note this
// is distinct from LOCATION metadata (is-key-present / which-node / which-tier
// / size), which the Coordinator + LocalIndex own and which the codec ignores.
//
// WHY serde_id: it is the pluggable seam for serialization schemes, modelled on
// LMCache's SERDE interface (naive / cachegen / ...). We do NOT build a full
// SERDE abstract base here (scope creep); we reserve one header byte so a future
// compressed encoder can be added without a format break. This module
// implements serde_id=0 (raw: payload stored verbatim) only; 1+ are reserved.
//
// BLOB LAYOUT (all little-endian; fixed 36-byte header + variable payload):
//   off  len  field              type   note
//   0    2    magic = 'T','P'    u8[2]  identify + guard against stray bytes
//   2    1    version = 0x01     u8     format version for future fields
//   3    1    serde_id = 0       u8     0=raw; 1+ reserved for compression
//   4    4    num_tokens         u32   ┐
//   8    4    num_layers         u32   │
//   12   2    dtype_size         u16   ├ shape metadata (inlined, self-describing)
//   14   2    kv_heads           u16   │
//   16   8    created_unix_ns    u64   │
//   24   4    model_fingerprint  u32   ┘
//   28   8    payload_len        u64    payload byte count (streamable/self-sized)
//   36   N    payload            u8[N]  raw bytes for serde_id=0; len == payload_len
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "tidepool/api/block.h"
#include "tidepool/api/status.h"

namespace tidepool {

namespace block_codec {

// Fixed byte layout constants. Exposed so tests can assert on them and so both
// the writer and reader share one source of truth.
inline constexpr uint8_t kMagic0 = 'T';
inline constexpr uint8_t kMagic1 = 'P';
inline constexpr uint8_t kVersion = 0x01;
inline constexpr uint8_t kSerdeRaw = 0;   // payload stored verbatim
inline constexpr size_t kHeaderSize = 36;  // bytes before the payload

}  // namespace block_codec

// Serialize `block` into a single self-describing blob (36-byte header +
// payload). Uses serde_id=0 (raw): the payload is `block.data` copied verbatim.
// The returned string is the whole blob and nothing else — no key, no opcode,
// no frame length (those are the caller's concern; see file doc).
std::string SerializeBlock(const Block& block);

// Parse ONLY the header of `blob`: recover the shape metadata and locate the
// payload, WITHOUT copying the payload. This is the "probe" entry point the SSD
// tier's size-probe protocol relies on — the caller inspects `*payload_len_out`
// to size/resize its destination, then copies the payload itself from
// blob[*payload_offset_out, +*payload_len_out).
//
// On success returns Ok and fills the three out params (any may be null to skip
// it). On a malformed blob returns a specific error and leaves outputs
// unspecified; it never reads out of bounds or crashes:
//   * blob shorter than the 36-byte header       -> kInvalidArgument
//   * magic mismatch                              -> kInvalidArgument
//   * unrecognized version                        -> kInvalidArgument
//   * serde_id != 0 (compression not built yet)   -> kNotImplemented
//   * payload_len differs from bytes after header -> kInvalidArgument
Status DeserializeHeader(std::string_view blob, BlockMetadata* meta_out, size_t* payload_len_out,
                         size_t* payload_offset_out);

}  // namespace tidepool
