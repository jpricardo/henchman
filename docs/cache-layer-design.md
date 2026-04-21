# Henchman — Architecture Design Document

**Version:** 1.0  
**Status:** Draft  
**Last Updated:** 2026-04-21

---

## 1. Overview

This document describes the architecture of **Henchman**, a standalone, in-memory cache layer implemented in Go. Henchman operates as a sidecar container alongside consumer applications, providing isolated, configurable key-value caching over a gRPC interface. It is not intended to be a Redis replacement — it is a purpose-built cache with richer per-instance configuration and multi-tenant isolation as first-class concerns.

**Module:** `github.com/jpricardo/henchman`  
**Binary:** `henchd`  
**Environment prefix:** `HENCH_`  
**Default gRPC port:** `6474`  
**Default metrics port:** `9090`

---

## 2. Goals and Non-Goals

### Goals

- Provide fast, in-memory key-value caching for multiple consumer applications simultaneously
- Enforce hard memory isolation between consumer instances
- Support per-instance and per-operation configuration
- Be consumable from any major backend runtime (Node.js, C#, Java) via generated gRPC stubs
- Operate safely as a sidecar with no external authentication requirements
- Be deployable as a single Docker container in a Docker Compose network or cloud VM local network

### Non-Goals

- Persistence or durability across restarts
- Distributed operation across multiple hosts
- Drop-in Redis protocol compatibility
- External network exposure or multi-host replication
- Authentication or authorization beyond instance token identity

---

## 3. Functional Requirements

### 3.1 Instance Management

- A consumer application must register with the cache server before performing any operations, providing an instance identifier and configuration
- The server must return an opaque token on successful registration, used to identify the instance on all subsequent requests
- Registration must be rejected if the requested memory allocation would exceed the server's global memory budget
- On re-registration with an existing instance identifier, the server must flush the existing store and issue a new token (RESUME policy is explicitly out of scope for v1)
- Instances must be fully isolated: keys, configuration, eviction state, and memory accounting must not be shared across instances

### 3.2 Core Cache Operations

- `Get(key)` — retrieve a value by exact key; return a typed miss (not found) distinct from an empty value
- `Set(key, value, options)` — store a key-value pair with optional per-operation TTL override and invalidate-after-read flag
- `Delete(key)` — explicitly remove a key
- `Query(matcher, options)` — retrieve or invalidate multiple keys by prefix scan; glob and tag-based matching are out of scope for v1

### 3.3 Eviction

- The server must enforce hard memory caps per instance; a write that would exceed the cap must trigger synchronous eviction before insertion
- Eviction policy must be configurable per instance at registration time, supporting LRU (v1), LFU (v2), and FIFO (v2)
- LRU is the default eviction policy
- A single entry whose size exceeds the instance cap must be rejected with a typed error, not silently dropped

### 3.4 TTL and Expiry

- Each instance must have a configurable default TTL applied to all entries; a value of zero means no expiry
- Individual `Set` operations may override the instance default TTL
- Expired entries must be detected and treated as misses on read (lazy expiry)
- A background sweep goroutine per instance must periodically reclaim memory held by expired but unread entries
- The sweep interval must be configurable per instance

### 3.5 Memory Accounting

- The server must track memory usage per instance, accounting for key size, value size, and a fixed per-entry struct overhead
- Memory accounting must be maintained exactly; drift between tracked and actual usage is a correctness defect
- The eviction policy must own size tracking internally, returning both the evicted key and its size on each eviction call
- A secondary `MaxKeys` limit per instance must be enforceable independently of byte-based accounting

### 3.6 Observability

- The server must expose per-instance metrics including: hit rate, eviction count and rate, current bytes used, cap bytes, key count, and expired key sweep count
- Metrics must be available via a `GET /metrics` HTTP endpoint in Prometheus exposition format
- Metrics must be updated on every cache operation; they are not sampled

---

## 4. Non-Functional Requirements

### 4.1 Performance

- Read operations must not contend with reads or writes on other instances; locking is per-instance
- Write operations may block readers on the same instance during eviction; this is an accepted tradeoff and must be documented
- Token-to-instance resolution must not require a registry lock on the hot path; instance identity must be resolved once at connection establishment and cached for the lifetime of the connection
- All values must be stored as `[]byte`; no `interface{}` storage is permitted, ensuring GC pressure remains predictable and size accounting remains honest

### 4.2 Isolation

- A memory or eviction event in one instance must have no effect on any other instance
- The server must maintain a global memory budget; the sum of all registered instance caps must never exceed this budget
- Registration must atomically check and reserve capacity against the global budget

### 4.3 Reliability

- The cache must never be a hard dependency for consumers; all cache calls must carry a deadline, and connection failure or timeout must be treated as a cache miss, not an error
- The server must handle consumer disconnection and reconnection gracefully
- Background goroutines (TTL sweep) must be tied to a `context.Context` per instance and cancelled on instance flush or server shutdown; goroutine leaks are a correctness defect

### 4.4 Deployability

- The server must be packaged as a Docker container with no external service dependencies
- The server must bind only to the Docker internal network interface, never to `0.0.0.0`
- No ports must be published to the host by default
- The server must be configurable entirely via environment variables or a mounted config file; no runtime reconfiguration is required

### 4.5 Compatibility

- The gRPC interface must be the sole operational interface; no Redis wire protocol compatibility is provided
- Generated client stubs must be the primary integration path for consumers; raw HTTP is not supported
- The proto package must be namespaced as `henchman.v1` from the start to allow non-breaking introduction of `henchman.v2` in future

---

## 5. Architecture

### 5.1 Deployment Topology

```
┌─────────────────────────────────────────────────────┐
│                Docker Compose Network               │
│                                                     │
│  ┌─────────────────┐     ┌─────────────────┐        │
│  │  consumer-app-a │     │  consumer-app-b │        │
│  │  (Node.js)      │     │  (Java)         │        │
│  └────────┬────────┘     └────────┬────────┘        │
│           │                       │                 │
│           └──────────┬────────────┘                 │
│                      │ gRPC TCP (internal only)     │
│           ┌──────────▼────────────┐                 │
│           │       henchman        │                 │
│           │     :6474 (gRPC)      │                 │
│           │     :9090 (metrics)   │                 │
│           └───────────────────────┘                 │
│                                                     │
│  Port 6474 and 9090 NOT published to host           │
└─────────────────────────────────────────────────────┘
```

### 5.2 Layered Architecture

```
┌──────────────────────────────────────────────┐
│              Transport Layer                 │
│   gRPC server over TCP, internal network     │
│   Unary interceptor: token → instance        │
└──────────────────┬───────────────────────────┘
                   │
┌──────────────────▼───────────────────────────┐
│              Instance Registry               │
│   Registration, global budget enforcement    │
│   Token issuance, instance lifecycle         │
└──────────────────┬───────────────────────────┘
                   │
┌──────────────────▼───────────────────────────┐
│              Instance Layer                  │
│   Per-instance config, store, goroutine ctx  │
│   Metrics collection                         │
└──────────────────┬───────────────────────────┘
                   │
┌──────────────────▼───────────────────────────┐
│               Store Layer                    │
│   map[string]*Entry, RWMutex                 │
│   Size accounting, eviction trigger          │
└──────────────────┬───────────────────────────┘
                   │
┌──────────────────▼───────────────────────────┐
│           Eviction Policy Layer              │
│   EvictionPolicy interface                   │
│   LRU (v1), LFU / FIFO (v2)                 │
└──────────────────────────────────────────────┘
```

### 5.3 Key Data Structures

#### Server

```go
type Server struct {
    registry  *Registry
    budget    *GlobalBudget
    metrics   *MetricsServer
}
```

#### Global Budget

```go
type GlobalBudget struct {
    mu             sync.Mutex
    totalBytes     int64
    allocatedBytes int64
}

func (b *GlobalBudget) Reserve(bytes int64) error
func (b *GlobalBudget) Release(bytes int64)
```

#### Registry

```go
type Registry struct {
    mu        sync.RWMutex
    byToken   map[string]*Instance
    byID      map[string]*Instance
}
```

The registry mutex is held only during `Register` and `Flush` operations. It is never held on the hot path. Instance resolution on the hot path is performed once per connection via the gRPC interceptor.

#### Instance

```go
type Instance struct {
    id      string
    token   string
    config  InstanceConfig
    store   *Store
    cancel  context.CancelFunc  // cancels sweep goroutine
    metrics *InstanceMetrics
}

type InstanceConfig struct {
    MaxBytes       int64
    MaxKeys        int64
    DefaultTTL     time.Duration  // 0 = no expiry
    EvictionPolicy string         // "lru"
    SweepInterval  time.Duration
}
```

#### Store

```go
type Store struct {
    mu       sync.RWMutex
    data     map[string]*Entry
    policy   EvictionPolicy
    maxBytes int64
    maxKeys  int64
}
```

`currentBytes` and `currentKeys` are owned by the eviction policy, not the store, to avoid drift between the two sources of truth.

#### Entry

```go
type Entry struct {
    Key       string
    Value     []byte
    ExpiresAt time.Time  // zero value = no expiry
    InvalidateAfterRead bool
}

func (e *Entry) SizeBytes() int64 {
    return int64(len(e.Key)) + int64(len(e.Value)) + entryStructOverhead
}
```

`entryStructOverhead` is a package-level constant validated by a unit test that asserts it equals the measured size of an empty `Entry` struct.

#### Eviction Policy Interface

```go
type EvictionPolicy interface {
    Add(key string, sizeBytes int64)
    Touch(key string)
    Remove(key string)
    Evict() (key string, sizeBytes int64, ok bool)
    CurrentBytes() int64
    CurrentKeys() int64
}
```

`Evict()` is the single transactional call that returns the key to remove and the bytes to reclaim. The store performs one map delete and relies on the policy's own accounting — no separate `currentBytes` field on the store.

---

## 6. Wire Protocol

### 6.1 Transport

gRPC over TCP. Henchman binds to the Docker internal network interface on a configurable port (default `6474`, set via `HENCH_PORT`). TLS is not required; the network boundary provides sufficient isolation.

### 6.2 Proto Definition (v1)

```protobuf
syntax = "proto3";
package henchman.v1;

service Cache {
  rpc Register (RegisterRequest)  returns (RegisterResponse);
  rpc Get      (GetRequest)       returns (GetResponse);
  rpc Set      (SetRequest)       returns (SetResponse);
  rpc Delete   (DeleteRequest)    returns (DeleteResponse);
  rpc Query    (QueryRequest)     returns (QueryResponse);
  rpc Flush    (FlushRequest)     returns (FlushResponse);
}

message RegisterRequest {
  string         instance_id = 1;
  InstanceConfig config      = 2;
}

message InstanceConfig {
  int64  max_bytes        = 1;
  int64  max_keys         = 2;
  int64  default_ttl_ms   = 3;
  string eviction_policy  = 4;
  int64  sweep_interval_ms = 5;
}

message RegisterResponse {
  string token = 1;
}

message GetRequest {
  string key = 1;
}

message GetResponse {
  bool   found = 1;
  bytes  value = 2;
}

message SetRequest {
  string key    = 1;
  bytes  value  = 2;
  int64  ttl_ms = 3;  // 0 = use instance default
  bool   invalidate_after_read = 4;
}

message SetResponse {}

message DeleteRequest {
  string key = 1;
}

message DeleteResponse {}

message QueryRequest {
  oneof matcher {
    string exact_key   = 1;
    string key_prefix  = 2;
  }
  bool invalidate_matched = 3;
}

message QueryResponse {
  repeated KeyValue entries = 1;
}

message KeyValue {
  string key   = 1;
  bytes  value = 2;
}

message FlushRequest {}
message FlushResponse {}
```

### 6.3 Token Transport

The instance token is passed as a gRPC metadata header on all requests except `Register`. A server-side unary interceptor extracts the token, resolves the instance from the registry, and injects it into the request context. Individual RPC handlers never perform token resolution directly.

---

## 7. Operational Concerns

### 7.1 Startup Sequence

1. Parse server config (`HENCH_MAX_MEMORY`, `HENCH_PORT`, `HENCH_SWEEP_INTERVAL`, or mounted config file)
2. Initialize global budget and registry
3. Start gRPC server on `HENCH_PORT` (default `6474`)
4. Start metrics HTTP server on `HENCH_METRICS_PORT` (default `9090`)
5. Signal readiness (log line; no readiness endpoint required in v1)

### 7.2 Shutdown Sequence

1. Stop accepting new connections
2. Cancel all instance contexts (terminates sweep goroutines)
3. Flush all stores
4. Release registry
5. Exit

### 7.3 Consumer Restart Behaviour

On reconnection with an existing `instance_id`, the server flushes the existing store and issues a new token. The consumer starts with a cold cache. This is the only supported reconnect policy in v1.

### 7.4 Cache Miss on Server Failure

Consumers must treat all of the following conditions identically to a cache miss, never as a hard error:

- Connection refused
- Deadline exceeded
- Server unavailable (gRPC status `UNAVAILABLE`)

All cache calls from consumers must carry an explicit deadline. The recommended client-side deadline is 50ms for local network calls; this must be documented in consumer integration guidance.

### 7.5 Known Limitations

- Write operations acquire a write lock for the duration of synchronous eviction. Under sustained write pressure on a nearly-full instance, read latency on the same instance will increase. This is an accepted tradeoff in v1.
- Memory accounting does not include Go runtime allocator overhead or map bucket growth. Actual process memory will exceed the sum of instance caps by a small margin. The global budget should be set conservatively (e.g. 80% of available container memory) to account for this.
- No persistence. All data is lost on container restart.

---

## 8. Build Order

The following sequence minimises integration risk by ensuring each layer is testable in isolation before the next is built on top of it.

1. `Entry` struct and `SizeBytes()` method, validated by unit test against `unsafe.Sizeof`
2. `EvictionPolicy` interface and LRU implementation
3. `Store` with size accounting, synchronous eviction, and lazy TTL expiry
4. `GlobalBudget` with atomic reservation and release
5. `Registry` with instance lifecycle (register, flush, token lookup)
6. Background TTL sweep goroutine with context cancellation
7. gRPC server with token interceptor
8. Metrics HTTP server (Prometheus format)
9. Proto-generated client stubs for Node.js, C#, Java
10. LFU and FIFO eviction policies (v2)
11. Tag-based multi-match (v2)

---

## 9. Future Work (v2)

- **LFU and FIFO eviction policies** — implement `EvictionPolicy` interface; no store changes required
- **Tag-based invalidation** — reverse index (`tag → []key`) maintained on write; `QueryRequest` extended with `TagMatcher`
- **RESUME reconnect policy** — requires connection quiesce period and atomic token invalidation; explicitly deferred due to race complexity
- **Soft reservations with global ceiling** — hybrid memory model allowing idle capacity to be borrowed; deferred until hard cap limitations are observed in production
- **Push invalidation** — server-streaming RPC for consumers to receive invalidation notifications without polling

---

*End of document.*
