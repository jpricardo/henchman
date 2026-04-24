# henchman

A standalone, in-memory cache server written in Go. Runs as a sidecar container and exposes a gRPC API for key-value cache operations. Each consumer registers its own isolated instance with a dedicated memory budget, TTL sweep goroutine, and eviction policy.

## Quick start

```sh
docker compose up --build
```

gRPC on `:6474` (internal network only), Prometheus metrics on `:9090`.

## Configuration

| Variable             | Default       | Description                        |
|----------------------|---------------|------------------------------------|
| `HENCH_PORT`         | `6474`        | gRPC listen port                   |
| `HENCH_METRICS_PORT` | `9090`        | Prometheus metrics HTTP port       |
| `HENCH_MAX_MEMORY`   | `536870912`   | Global memory budget in bytes      |

## API

Service `henchman.v1.Cache` — all operations are gRPC unary RPCs.

| RPC        | Description                                                  |
|------------|--------------------------------------------------------------|
| `Register` | Create an isolated instance; returns an auth token           |
| `Get`      | Retrieve a value by key                                      |
| `Set`      | Store a value with optional TTL and invalidate-after-read    |
| `Delete`   | Remove a single key                                          |
| `Query`    | Prefix or exact-key match with optional invalidation         |
| `Flush`    | Destroy the instance and release its memory budget           |

Every RPC except `Register` requires a `x-hench-token` metadata header with the token returned by `Register`.

See [`proto/henchman/v1/cache.proto`](proto/henchman/v1/cache.proto) for the full message definitions.

## Client stubs

Pre-generated stubs are available in `gen/` for Go, TypeScript, C#, and Java. Regenerate with:

```sh
make buf
```

## Development

```sh
make test        # go test ./...
make build       # compile to ./henchd
make run         # go run ./cmd/henchd
make compose-up  # docker compose up --build
```

## Documentation

- [Architecture overview](docs/OVERVIEW.md)
- [Cache layer design](docs/cache-layer-design.md)
- [Integration guide](docs/integration.md)
