# Henchman — Architecture Overview

Henchman is a standalone, in-memory cache server written in Go. It runs as a sidecar container alongside consumer applications and exposes a gRPC API for key-value cache operations. Each consumer registers its own isolated instance with a dedicated memory budget, TTL sweep goroutine, and eviction policy.

---

## Repository Layout

```
henchman/
├── cmd/henchd/          # Binary entry point (main.go)
├── internal/
│   ├── budget/          # Global memory pool management
│   ├── cache/           # Entry struct, store, and eviction logic
│   ├── metrics/         # Prometheus HTTP server
│   ├── registry/        # Instance lifecycle (register, resolve, flush)
│   └── server/          # gRPC server and RPC handlers
├── proto/henchman/v1/   # Protobuf service definition
├── gen/                 # Generated client stubs (Go, Node, C#, Java)
├── docs/                # Design and integration documentation
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

---

## Configuration

All configuration is via environment variables:

| Variable            | Default       | Description                        |
|---------------------|---------------|------------------------------------|
| `HENCH_PORT`        | `6474`        | gRPC listen port                   |
| `HENCH_METRICS_PORT`| `9090`        | Prometheus metrics HTTP port       |
| `HENCH_MAX_MEMORY`  | `536870912`   | Global memory budget in bytes (512 MB) |

Per-instance configuration is sent at registration time (see [gRPC API](#grpc-api)).

---

## gRPC API

Service: `henchman.v1.Cache`  
Proto: [`proto/henchman/v1/cache.proto`](../proto/henchman/v1/cache.proto)

### Authentication

Every RPC (except `Register`) requires a `x-hench-token` metadata header containing the token returned by `Register`. The interceptor in `internal/server/grpc.go` resolves the token to an instance and injects it into the request context.

### Methods

| RPC        | Request fields                                      | Description                                              |
|------------|-----------------------------------------------------|----------------------------------------------------------|
| `Register` | `instance_id`, `config` (InstanceConfig)            | Creates an isolated cache instance; returns a token      |
| `Get`      | `key`                                               | Retrieves a value; returns `found=false` on miss/expiry  |
| `Set`      | `key`, `value`, `ttl_ms`, `invalidate_after_read`   | Stores an entry; triggers eviction if over capacity      |
| `Delete`   | `key`                                               | Removes a single key                                     |
| `Query`    | `exact_key` or `key_prefix`, `invalidate_matched`   | Prefix or exact match; optionally deletes matched keys   |
| `Flush`    | _(empty)_                                           | Destroys the instance and releases its memory budget     |

### InstanceConfig fields

| Field               | Description                                    |
|---------------------|------------------------------------------------|
| `max_bytes`         | Memory cap for this instance                   |
| `max_keys`          | Maximum number of keys                         |
| `default_ttl_ms`    | Default TTL (0 = no expiry)                    |
| `eviction_policy`   | `"lru"` (only option in v1)                    |
| `sweep_interval_ms` | Interval for background TTL sweep goroutine    |

---

## Internal Packages

### `internal/budget`

Holds a single `GlobalBudget` — a mutex-protected counter of total vs. allocated bytes. `Reserve(n)` is called during registration; `Release(n)` during flush. Registration fails with `codes.Internal` if the budget is exhausted.

### `internal/cache`

Three concerns, three files:

- **`entry.go`** — `Entry` struct: key, value ([]byte), `ExpiresAt` (zero = no expiry), `InvalidateAfterRead` flag.
- **`store.go`** — `Store`: an RWMutex-protected map. Tracks byte and key counts via the eviction policy. Expiry is checked lazily on read; proactive expiry is handled by the sweep goroutine.
- **`eviction.go`** — `EvictionPolicy` interface + `LRUEvictionPolicy`: a doubly-linked list with a hash map for O(1) promote/evict. The policy owns the running byte/key counters.

Entry size accounting: `len(key) + len(value) + entryStructOverhead` (verified via `unsafe.Sizeof` in tests).

### `internal/registry`

`Registry` keeps two maps: `byID` and `byToken`, both protected by an RWMutex. On registration it:

1. Validates the eviction policy string.
2. Calls `budget.Reserve`.
3. Generates a 32-char hex token via `crypto/rand`.
4. Spawns a per-instance background sweep goroutine (cancellable via context).

On flush it cancels the sweep goroutine, releases the budget, and removes both map entries.

### `internal/server`

`grpc.go` wires up the gRPC server with the token-validation interceptor.  
`handlers.go` implements `pb.CacheServer`: each handler calls into the registry to resolve the instance from context, then calls the appropriate `Store` method. Errors map to standard gRPC status codes (`Unauthenticated`, `InvalidArgument`, `ResourceExhausted`, `Internal`).

### `internal/metrics`

A plain `net/http` server on `HENCH_METRICS_PORT`. `GET /metrics` iterates all registered instances, snapshots their atomic counters, and writes Prometheus exposition format (v0.0.4).

**Metric families** (all labeled `instance_id`):

| Name                           | Type    | Description                    |
|--------------------------------|---------|--------------------------------|
| `henchman_hits_total`          | counter | Cache hits                     |
| `henchman_misses_total`        | counter | Cache misses                   |
| `henchman_evictions_total`     | counter | Keys evicted by policy         |
| `henchman_expired_swept_total` | counter | Keys removed by sweep goroutine|
| `henchman_bytes_used`          | gauge   | Current bytes stored           |
| `henchman_bytes_cap`           | gauge   | Instance byte capacity         |
| `henchman_keys_current`        | gauge   | Current key count              |

---

## Deployment

Henchman is designed to run as a **sidecar** — on the same Docker network as the consumer, with the gRPC port internal-only:

```yaml
# docker-compose.yml (abridged)
services:
  henchman:
    build: .
    expose:
      - "6474"      # gRPC: internal network only
    ports:
      - "9090:9090" # Metrics: published to host for scraping
```

The runtime image is `gcr.io/distroless/static-debian12` (no shell, minimal attack surface).

---

## Client Stubs

Generated via [Buf](https://buf.build) from the proto definition. Run `make buf` to regenerate.

| Language   | Location              | Plugin                                  |
|------------|-----------------------|-----------------------------------------|
| Go         | `gen/proto/`          | `buf.build/grpc/go`                     |
| TypeScript | `gen/node/`           | `buf.build/community/stephenh-ts-proto` |
| C#         | `gen/csharp/`         | `buf.build/grpc/csharp`                 |
| Java       | `gen/java/`           | `buf.build/grpc/java`                   |

---

## Development

```sh
make test        # go test ./...
make build       # produces ./henchd binary
make run         # go run ./cmd/henchd
make buf         # regenerate all client stubs
make compose-up  # docker compose up --build
```
