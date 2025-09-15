# Repository Guidelines

## Project Structure & Module Organization
- Root package: Go SDK for Treasure Data (`*.go`), grouped by domain (e.g., `databases.go`, `queries.go`).
- CLI: `cmd/tdcli` (Kong-based command tree, config helpers under `cmd/tdcli/...`).
- OpenTelemetry: `otel/` (HTTP transport, helpers); integration docs in `docs/` and runnable configs under `examples/`.
- Tests: `*_test.go` colocated with sources; fixtures in `testdata/`.
- Release/packaging: `.goreleaser.yaml`, `Dockerfile`, `dist/` (artifacts).

## Build, Test, and Development Commands
- Install tools (via Aqua): `aqua i` (provides `goreleaser`, etc.).
- Compile SDK + packages: `go build ./...`
- Build CLI: `go build -o dist/tdcli ./cmd/tdcli`
- Run tests (unit + integration): `go test ./... -race -cover`
- Focus tests by package or name: `go test ./... -run TestName`
- Lint/vet (stdlib): `go vet ./...` and `go fmt ./...`
- Release dry-run: `goreleaser release --snapshot --clean`

## Coding Style & Naming Conventions
- Go formatting: run `go fmt ./...` before pushing; 4-space tabs (Go default).
- File naming mirrors services: `databases.go`, `tables.go`; tests as `xxx_test.go`.
- Exported identifiers require clear comments; avoid stutter (e.g., `Client`, `DatabasesService`).
- Context-first methods: pass `context.Context` as the first parameter for I/O.
- Errors: wrap with context using `fmt.Errorf("...: %w", err)`; prefer sentinel/typed errors where applicable.

## Testing Guidelines
- Framework: standard `testing` with table tests where helpful.
- Coverage: aim to keep/new code ≥80% where practical.
- Integration tests: OTEL/Trino tests run without external services by using in-memory exporters; when unsure, target by `-run`.
- Add tests next to sources; prefer small, deterministic HTTP handlers for API mocks.

## Commit & Pull Request Guidelines
- Commits: use Conventional Commits where possible (`feat:`, `fix:`, `chore:`); keep messages imperative and scoped.
- PRs: include a clear summary, linked issues, rationale, and test/demo output. Update `README.md`/`docs/` and examples if behavior changes.
- CI/readability: ensure `go build`, `go test -race`, and `go vet` pass locally; do not commit secrets—use `TD_API_KEY` or `~/.tdcli/.tdcli.toml` for local runs.

## Security & Configuration Tips
- Never hardcode API keys or OTEL credentials. Use env vars (e.g., `TD_API_KEY`, `OTEL_*`) or local config.
- For TLS, prefer system CAs; when adding custom CA/certs, use `SSLOptions` and document why.
