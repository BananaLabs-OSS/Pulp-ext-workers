# Pulp-ext-workers

Host-side parallel task runner for Pulp cells. Cells submit tasks via host imports; the host runs each in its own goroutine. Fire-and-forget supported.

From [BananaLabs OSS](https://github.com/BananaLabs-OSS).

## Deployment

```go
import _ "github.com/BananaLabs-OSS/Pulp-ext-workers"
```

## Capability

- `workers` — currently supports `http.fetch` (outbound HTTP via net/http). Add new task types by extending the switch in `runTask`.
