# Pulp-ext-workers

Host-side parallel task runner for Pulp cells. Cells submit tasks via host imports; the host runs each in its own goroutine. Fire-and-forget supported.

From [BananaLabs OSS](https://github.com/BananaLabs-OSS).

## Deployment

```go
import _ "github.com/BananaLabs-OSS/Pulp-ext-workers"
```

## Capability

- `workers` — currently supports `http.fetch` (outbound HTTP via net/http). Add new task types by extending the switch in `runTask`.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PULP_WORKERS_MAX_CONCURRENCY` | `32` | Global goroutine concurrency cap across all cells. |
| `PULP_WORKERS_MAX_QUEUED` | `1024` | Max tasks allowed in the submit queue at once (across all cells). |
| `PULP_WORKERS_MAX_PER_CELL` | `8` | Per-cell ceiling on concurrent tasks; prevents one cell starving the pool. |
| `PULP_WORKERS_MAX_FETCH_BYTES` | `17179869184` (16 GiB) | Max `http.fetch` response body buffered in host memory per task. |
| `HTTP_FETCH_ALLOW` | _(deny all private)_ | Comma-separated `host[:port]` or CIDR entries to allow through the SSRF egress guard (e.g. `192.168.1.0/24,internal-service`). |
