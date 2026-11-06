# CLAUDE.md

## Project Overview

**coldplane** is a Go CLI that archives cold data from relational databases (PostgreSQL, MySQL) into Parquet files on local disk, and queries those archives with DuckDB. It runs entirely on the user's machine: no account, no server, no network calls beyond the user's own database.

## Repository Structure

```
coldplane/
├── main.go                    # Entry point — calls cmd.Execute()
├── cmd/
│   ├── root.go                # Cobra commands: backup create/list/delete, query run, analyze, completion
│   └── version.go             # version command; Version/Commit/BuildDate set via ldflags
├── internal/
│   ├── analyze/               # Age analysis: find rows older than a threshold per table
│   ├── duck/                  # DuckDB engine: open, attach pg/mysql (extensions), passthrough + COPY SQL
│   ├── archive/               # Database → Parquet archiving
│   │   ├── postgres.go        # ArchivePostgres: Postgres chunk detection, planners, runner
│   │   ├── mysql.go           # ArchiveMySQL: MySQL chunk detection, planners, runner
│   │   ├── chunking.go        # chunkKind/chunkPlan, planners, runChunks, shared archiveTable (DuckDB COPY per chunk)
│   │   ├── queries.go         # Native SQL text for both engines (literal bounds; DuckDB passes it through)
│   │   ├── partition.go       # timeValue, firstTimeColumn
│   │   ├── manifest.go        # Manifest/TableEntry/FileEntry, index.json read/write, backup dir rules
│   │   ├── local.go           # ListBackups, DeleteBackup, ResolveBackupDir
│   │   ├── options.go         # ArchiveOptions (ChunkSize, Workers, OutputDir, Where)
│   │   └── utils.go           # ProgressTracker, NormalizeMySQLDSN
│   ├── dbutil/                # Shared SQL helpers: identifier quoting, time parsing, DB name from DSN
│   ├── query/
│   │   └── duck.go            # Run: DuckDB in-memory view over the backup's Parquet files
│   └── term/
│       └── color.go           # Terminal color utilities
├── integration/               # Integration tests against Docker databases (build tag: integration)
│   ├── local_test.go          # backup create/list/delete, query, analyze flows
│   └── helpers_test.go        # DSNs, seed and verify helpers
├── go.mod / go.sum            # module github.com/avantexa/coldplane, Go 1.24
├── docker-compose.yml         # PostgreSQL + MySQL for tests
├── seed_*.sql, my.cnf         # Test seed data and MySQL config
├── Dockerfile.build           # Cross-compilers for Linux/Windows builds
└── scripts/
    ├── build.sh               # Cross-platform build script
    ├── test.sh                # Test runner (unit | local | all)
    └── install.sh             # curl | sh installer pulling from GitHub Releases
```

## Build & Run

```bash
go build -o coldplane      # cgo must be enabled (DuckDB); no extra libraries needed
./coldplane <command>
```

## CLI Commands

| Command | Purpose |
|---|---|
| `analyze` | Report how much data per table is older than a threshold; suggests `backup create` commands |
| `backup create` | Archive tables to Parquet under `~/.coldplane/backups/<name>/` |
| `backup list` | List backups found on disk |
| `backup delete` | Remove a backup directory |
| `query run` | Run SQL with DuckDB against a backup (view name: `restore_input`) |
| `version`, `completion` | Build info, shell completions |

## Backup layout

```
~/.coldplane/backups/<name>/
  index.json                            # manifest
  <table>/year=YYYY/month=MM/*.parquet  # data files
```

## Key Dependencies

- **cobra** — CLI framework
- **pgx/v5** — PostgreSQL driver (archive, analyze); **lib/pq** is test-only seeding
- **go-sql-driver/mysql** — MySQL driver
- **go-duckdb** — DuckDB (cgo): extraction through the postgres/mysql extensions, Parquet writing via COPY, and querying
- **fatih/color**, **go-pretty** — Terminal output

## Code Conventions

- Standard Go formatting (`gofmt`); PascalCase exported, camelCase unexported; lowercase single-word packages.
- Wrap errors with context: `fmt.Errorf("action failed: %w", err)`.
- Extraction: native drivers detect the chunk column and plan ranges; each chunk is native SQL that DuckDB runs on the source through `postgres_query`/`mysql_query` and writes with `COPY ... (FORMAT parquet, PARTITION_BY (year, month))`. Engines differ only in detection and planners; `archiveTable` is shared. Extensions download on first use (`COLDPLANE_DUCKDB_EXTENSIONS` overrides the directory).
- Types in the archive are DuckDB's mapping of the source types. A new backup refuses to reuse an existing backup directory; a failed one is removed. The manifest records per table its columns, chunk kind and column, and row count, and per backup the `--where` used, for verify/purge/restore.
- Progress tracking via the mutex-guarded `ProgressTracker`; worker pools and channel streaming for chunked reads.
- All long-running operations accept `context.Context`.
- Keep PostgreSQL and MySQL implementations in parity when changing one.
- Schema is inferred from sample rows; decimals get precision detection; several time formats are parsed.

## Testing

```bash
./scripts/test.sh              # unit tests (go test ./...)
./scripts/test.sh local        # Docker integration tests (COLDPLANE_LOCAL_TEST=1, -tags integration)
./scripts/test.sh all          # both
```

Integration tests are behind the `integration` build tag and skip unless `COLDPLANE_LOCAL_TEST=1`. Start databases with `docker compose up -d`. The DSNs default to localhost and can be overridden with `COLDPLANE_TEST_PG_DSN` / `COLDPLANE_TEST_MYSQL_DSN`; CI runs the same suite against Postgres and MySQL service containers.

## Notes for AI Assistants

- Pure Go project, Go modules, no Node tooling.
- `internal/` packages cannot be imported externally.
- Never hardcode credentials. Test DSNs for the Docker databases live in `integration/helpers_test.go`.
- Removed on purpose (SaaS era): API client, encryption keys, S3 presigned uploads, restore/convert. Local restore is the first planned follow-up feature.
