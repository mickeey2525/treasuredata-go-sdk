# Treasure Data Go SDK and CLI (tdcli)

Go client and CLI for Treasure Data APIs: databases, tables, queries/jobs, results, permissions, CDP, and workflows. Includes optional OpenTelemetry instrumentation for traces and metrics.

## Installation

- SDK module
  ```bash
  go get github.com/mickeey2525/treasuredata-go-sdk
  ```
- CLI (choose one)
  - Homebrew (macOS)
    ```bash
    brew tap mickeey2525/tap
    brew install --cask tdcli
    # upgrade later
    brew upgrade --cask tdcli
    ```
  - Prebuilt binaries (Releases)
    - Download the asset for your OS/arch from the Releases page.
    - Verify with the accompanying `checksums.txt`.
    - Extract and place `tdcli` in your PATH, e.g. `/usr/local/bin` or `/opt/homebrew/bin`.
    - macOS note: if downloaded via browser, clear quarantine if needed: `xattr -dr com.apple.quarantine /path/to/tdcli`.
  - Go install
    ```bash
    go install github.com/mickeey2525/treasuredata-go-sdk/cmd/tdcli@latest
    ```

## Quick Start (SDK)

```go
package main

import (
    "context"
    "fmt"
    td "github.com/mickeey2525/treasuredata-go-sdk"
)

func main() {
    client, _ := td.NewClient("YOUR_API_KEY", td.WithRegion("us"))
    dbs, _ := client.Databases.List(context.Background())
    for _, d := range dbs { fmt.Println(d.Name) }
}
```

## Quick Start (CLI)

```bash
export TD_API_KEY="YOUR_API_KEY"

tdcli databases list
tdcli queries submit --database sample_datasets --query "SELECT 1"
tdcli jobs list --status running
```

More examples: `cmd/tdcli/README.md` and `examples/`.

## Configuration

- Authentication: set `TD_API_KEY` or pass `--api-key`.
- Region: `--region us|eu|tokyo|ap02` (default `us`). Programmatically use `WithRegion("us")` or `WithEndpoint(url)`.
- CLI config file: copy `tdcli.example.toml` to `~/.tdcli/.tdcli.toml` or project `.tdcli.toml`.
- TLS: CLI supports `--insecure-skip-verify`, `--cert-file`, `--key-file`, `--ca-file`. The SDK mirrors these via `WithSSLOptions`.

## OpenTelemetry

- CLI: enable by environment or flags
  ```bash
  export OTEL_ENABLED=true
  export OTEL_SERVICE_NAME=tdcli
  export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
  tdcli queries list --database sample_datasets
  ```
- SDK: pass a tracer and meter (e.g., from the provided OTEL manager)
  ```go
  cfg := otel.DefaultOTELConfig()                           // package: github.com/mickeey2525/treasuredata-go-sdk/otel
  mgr, _ := otel.NewOTELManager(cfg); _ = mgr.Initialize(context.Background())
  client, _ := td.NewClient(apiKey, td.WithOTEL(mgr.GetTracer(), mgr.GetMeter()))
  ```
See `docs/otel-integration.md` and `examples/` for vendor configs (Jaeger, Honeycomb, Datadog, Grafana Cloud).

## Common Tasks

- Databases: `client.Databases.List(ctx)`, `Create`, `Get`, `Delete`
- Tables: `client.Tables.List(ctx, db)`, `Create`, `Rename`, `Swap`, `Delete`
- Queries: `client.Queries.Issue(ctx, td.QueryTypeTrino, db, opts)` then poll via `Jobs`
- Results: `client.Results.GetResultJSON(ctx, jobID, &out)` or streaming JSONL
- Permissions: policies, groups, users via `client.Permissions`
- CDP and Workflows: see `cdp_*.go`, `workflow_*.go` and CLI subcommands

## Development

- Run tests: `go test ./... -race`
- Static checks: `go vet ./...` and `go fmt ./...`
- Release (maintainers): `goreleaser release --snapshot --clean`

## License

Apache 2.0. See `LICENSE`.
