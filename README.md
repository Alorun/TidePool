# tidepool

**A distributed, cross-instance KV cache pool for LLM inference** — multiple
inference instances on different machines share one KV cache pool, so the KV
that a prefill instance computes can be read and reused by decode instances or
other requests instead of being recomputed.

> Status: **scaffold / skeleton**. Interfaces, module boundaries, build, and a
> runnable single-node DRAM path are in place; most data-plane networking and
> the SSD/RDMA/cost-aware paths are stubs marked `TODO` / `Status::NotImplemented`.

---

## One-line positioning

A KVCache-centric, prefill/decode-disaggregated shared cache pool (design
inspired by Mooncake, heavily simplified) that turns per-instance KV caches into
one horizontally-shared, globally-addressable pool.

---

## How this differs from a "tiered / multi-level KV cache"

A tiered KV cache (HBM → DRAM → SSD) is a **vertical** storage hierarchy
**inside a single instance** — its scope is one machine. tidepool is the
**horizontal**, cross-instance dimension and adds three things a tiered cache
does not have:

1. **Global reuse** — one KV block is shared by many instances, not trapped in
   the instance that computed it.
2. **Global addressing without a central lookup** — given a block key, any
   client *computes* the owning node via a consistent-hash ring; the get/put hot
   path never queries a central service.
3. **A control plane** — membership and the shard map are managed (versioned)
   separately from the data path.

In tidepool, tiered storage is demoted to *one submodule inside each storage
node* (`store/tier/*`); it is not the project itself.

---

## Architecture: strict data-plane / control-plane split

```
                          ┌──────────────────────────────────────────────┐
   inference instance     │                 CONTROL PLANE                 │
   (vLLM / LMCache)       │            (low frequency, consensus)         │
        │                 │                                              │
        │ Lookup/Get/Put  │   ┌────────────────────────────────────┐     │
        ▼                 │   │ Coordinator                         │     │
  ┌───────────────┐       │   │  - versioned ShardMap (members+ring)│     │
  │  Connector    │──register/heartbeat──▶  - MVP: single node     │     │
  │ (client API)  │◀──shard map (cached)──   - future: Raft/etcd    │     │
  │               │       │   └────────────────────────────────────┘     │
  │ consistent-   │       └──────────────────────────────────────────────┘
  │  hash routing │
  │ local shard   │                       DATA PLANE (hot path, no consensus)
  │  map cache    │       ┌──────────────────────────────────────────────┐
  │ (de)serialize │       │  Transfer Engine (Transport ABC)             │
  └──────┬────────┘       │   - TCP (MVP)      - RDMA/libibverbs (stub)   │
         │ register/read/write remote memory                            │
         ▼                │                                              │
  ┌──────────────────────────────────────────────────────────────────┐  │
  │  Storage Node × N   (together = the shared pool)                  │  │
  │  ┌───────────────┐  ┌──────────────┐  ┌───────────────────────┐   │  │
  │  │ Tiered Store  │  │ Local Index  │  │ Eviction Policy       │   │  │
  │  │ DRAM ⇄ SSD    │  │ key→Location │  │ LRU now / cost-aware  │   │  │
  │  │ (LevelDB)     │  │ pure local   │  │ (stub)                │   │  │
  │  └───────────────┘  └──────────────┘  └───────────────────────┘   │  │
  └──────────────────────────────────────────────────────────────────┘  │
                          └──────────────────────────────────────────────┘
```

**Data plane (low latency, never goes through consensus)**

- **Connector** (`client/`) — public API; routes `block → node` with a
  consistent-hash ring; caches the shard map and only re-queries the control
  plane when it is stale; (de)serializes and chunks KV blocks.
- **Transfer Engine** (`transport/`) — unified `RegisterMem / ReadRemote /
  WriteRemote` abstraction. TCP is the MVP; RDMA (libibverbs) is the same ABC,
  stubbed.
- **Storage Node** (`store/`) — N of them form the pool. Internals:
  - **Tiered Store** (`store/tier/`) — DRAM and SSD (LevelDB) physical I/O,
    interface-first so tiers can be added.
  - **Local Index** (`store/index/`) — node-local in-memory hash table mapping
    key → which tier/handle. Purely local, **no consensus**.
  - **Eviction** (`store/eviction/`) — LRU now; cost-aware interface reserved.

**Control plane (low frequency, strongly consistent)**

- **Coordinator** (`coordinator/`) — owns the versioned shard map, membership,
  config. MVP is an embedded single node; Raft/etcd plug in behind the same ABC.
  **Consensus belongs only here and never appears on the get/put hot path.**

### Key design principles

- **Prefix-hash block keys** — a block is keyed by a hash of its *token prefix*,
  so requests sharing a prompt prefix reuse each other's KV.
- **Locate by computation, not lookup** — consistent hashing + a locally cached
  shard map; no central query on the hot path.
- **Everything cross-cutting is an abstract base class** — `Transport`
  (TCP/RDMA), `Tier` (DRAM/SSD/future GPU), `EvictionPolicy` (LRU/cost-aware),
  `Coordinator` (single-node/Raft) are all pluggable.

---

## Public client API (`include/tidepool/client/connector.h`)

```cpp
Result<HitMap>  Lookup(const std::vector<BlockKey>& keys);  // batch hit bitmap
Result<Block>   Get(const BlockKey& key);
Status          Put(const BlockKey& key, const Block& block);
Status          Prefetch(const std::vector<BlockKey>& keys);  // optional
```

`Lookup` returns a hit bitmap so the engine can batch-probe which prefixes are
already cached and decide what to recompute before fetching.

## Internal interfaces (abstract base classes)

| Interface        | Header                                   | Methods |
|------------------|------------------------------------------|---------|
| `Transport`      | `transport/transport.h`                  | `RegisterMem` / `ReadRemote` / `WriteRemote` |
| `Tier`           | `store/tier.h`                           | `Put` / `Get` / `Evict` / `Stats` |
| `EvictionPolicy` | `store/eviction_policy.h`                | `OnAccess` / `OnInsert` / `Victim` |
| `Coordinator`    | `coordinator/coordinator.h`              | `GetShardMap` / `RegisterNode` / `Heartbeat` (versioned shard map) |

Shared types live in `include/tidepool/api/`: `BlockKey`, `Block`, `Location`,
`ShardMap`, `Status`/`Result`.

---

## Build

No third-party dependency is required for the default build (SSD/RDMA backends
are compiled as stubs):

```bash
cmake -B build
cmake --build build
ctest --test-dir build        # runs the smoke test
```

Optional backends:

```bash
cmake -B build -DTIDEPOOL_WITH_LEVELDB=ON   # real SSD tier (needs LevelDB)
cmake -B build -DTIDEPOOL_WITH_RDMA=ON       # link libibverbs (verbs still TODO)
```

Run the MVP binaries:

```bash
./build/bin/tidepool_coord                 # control-plane server (stubbed loop)
./build/bin/tidepool_node node-0 127.0.0.1:7001
```

### Layout

```
include/tidepool/   public headers (api, client, transport, store, coordinator, hashring)
src/                implementations (one library per module, each with its own CMakeLists)
apps/               tidepool_node (storage node) + tidepool_coord (coordinator) entry points
integration/vllm/   stub adapter onto a vLLM / LMCache KV backend
tests/              placeholder smoke test
```

---

## Roadmap

- **Stage 1 — single-node, two tiers.** Get `Put`/`Get` working on one node with
  DRAM + SSD (LevelDB), LRU eviction, local index. *(DRAM path is runnable today;
  SSD backend + DRAM⇄SSD demotion are the next TODOs.)*
- **Stage 2 — multi-node + addressing + control plane.** Consistent-hash routing
  in the Connector, the TCP Transfer Engine read/write path, and the Coordinator
  (register/heartbeat/shard-map versioning) wired end to end.
- **Stage 3 — RDMA + cost-aware eviction.** libibverbs Transport implementation
  and a cost-aware (recompute-cost vs. size vs. recency) eviction policy.

### Explicitly out of scope (for now)

- **No request-level global scheduler** (à la Mooncake Conductor). Scheduling is
  left to the upper inference framework. *Where it would plug in:* above the
  Connector — a scheduler would consume `Lookup` hit bitmaps across instances to
  place prefill/decode work and steer reuse, then drive `Get`/`Put`/`Prefetch`.
- **No GPU/HBM tier** — DRAM + SSD only; `TierType::kGpu` is reserved as an
  interface stub.
- **No production fault tolerance** — no replication / striping / failover; the
  hash ring is single-owner per key. Marked `TODO` (e.g. `HashRing::OwnerSet`).
```
