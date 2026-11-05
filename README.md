# coldplane

`coldplane` archives cold data from PostgreSQL and MySQL tables into Parquet files on local disk, and lets you query those archives with DuckDB without restoring them.

Typical use: find the tables holding years of old rows, archive the old rows to Parquet, verify the archive with a query, then delete the rows from production.

- **Nothing leaves your machine.** Backups live under `~/.coldplane/backups/<name>/`.
- **No account, no server.** The binary talks only to your database and your disk.
- **Open format.** Parquet files partitioned by `year=YYYY/month=MM`, readable by DuckDB, Spark, pandas, or anything else.

Both `postgres://` and `pg://` connection schemes are supported for PostgreSQL.

---

## Installation

Download a binary from [GitHub Releases](https://github.com/avantexa/coldplane/releases/latest), or:

```bash
curl -fsSL https://raw.githubusercontent.com/avantexa/coldplane/main/scripts/install.sh | sh
```

Or build from source (Go 1.24+, cgo enabled for DuckDB):

```bash
git clone https://github.com/avantexa/coldplane
cd coldplane
go build -o coldplane
./coldplane version
```

---

## Commands

### `analyze` — Find old data

Connects read-only and reports how many rows in each table are older than a threshold, using an auto-detected date column.

```bash
coldplane analyze --db-url "postgres://user:pass@host:5432/mydb" --tables "orders,events"
coldplane analyze --db-url "mysql://user:pass@host:3306/mydb" --all --older-than 1y
coldplane analyze --db-url "postgres://..." --all --json
```

The report ends with ready-to-run `backup create` commands for the tables that have archivable rows.

| Flag | Short | Description |
|------|-------|-------------|
| `--db-url` | `-d` | Database connection string (required) |
| `--tables` | | Comma-separated tables to analyze |
| `--all` | | Analyze every table in the database |
| `--older-than` | | Age threshold, e.g. `18m`, `1y`, `90d` (default: `18m`) |
| `--date-column` | | Column to use for age detection (auto-detected if omitted) |
| `--json` | | Output the report as JSON |

### `backup create` — Archive cold data

Archives one or more tables to Parquet. Rows are read in chunks and partitioned by date when a timestamp column is available.

```bash
# PostgreSQL
coldplane backup create \
  --db-url "postgres://user:pass@host:5432/mydb?sslmode=require" \
  --tables "events,logs" \
  --where "created_at < '2024-01-01'"

# MySQL
coldplane backup create \
  --db-url "mysql://user:pass@host:3306/mydb" \
  --tables "events,logs"
```

| Flag | Short | Description |
|------|-------|-------------|
| `--db-url` | `-d` | Database connection string (required) |
| `--tables` | | Comma-separated list of tables to archive (required) |
| `--where` | | SQL `WHERE` clause applied to every table |
| `--name` | | Backup name (auto-generated from table and timestamp if omitted) |
| `--chunk-size` | | Rows per chunk (default: 2,500,000) |
| `--workers` | | Parallel archive workers (default: 4) |

### `backup list` — List backups

```bash
coldplane backup list
```

```
╭────────────────────────┬──────────┬──────────┬──────────────┬──────────────────────┬─────────┬──────────────────────────────────────────╮
│ Name                   │ Database │ DB Type  │ Tables       │ Created At           │ Size    │ Path                                     │
├────────────────────────┼──────────┼──────────┼──────────────┼──────────────────────┼─────────┼──────────────────────────────────────────┤
│ events_20250101_120000 │ mydb     │ postgres │ events, logs │ 2025-01-01T12:00:00Z │ 8.40 GB │ ~/.coldplane/backups/events_20250101_... │
╰────────────────────────┴──────────┴──────────┴──────────────┴──────────────────────┴─────────┴──────────────────────────────────────────╯
```

### `query run` — Query archived data without restoring

Runs SQL with DuckDB over the backup's Parquet files. The selected table is exposed as the view `restore_input`.

```bash
coldplane query run \
  --backup-id events_20250101_120000 \
  --table events \
  --sql "SELECT user_id, COUNT(*) AS total FROM restore_input WHERE type = 'error' GROUP BY user_id ORDER BY total DESC" \
  --output table
```

```bash
# JSON — pipe to jq
coldplane query run -b events_20250101_120000 -t events -o json | jq '.[] | select(.severity == "critical")'

# CSV — for spreadsheets or pipelines
coldplane query run -b events_20250101_120000 -t events -o csv > report.csv
```

| Flag | Short | Description |
|------|-------|-------------|
| `--backup-id` | `-b` | Backup name, or a path to a backup directory (required) |
| `--table` | `-t` | Table to query (defaults to all tables in the backup) |
| `--sql` | `-s` | SQL to run (default: `SELECT * FROM restore_input`) |
| `--output` | `-o` | Output format: `table` (default), `json`, `csv` |

### `backup delete` — Delete a backup

```bash
coldplane backup delete events_20250101_120000
```

---

## Backup layout

```
~/.coldplane/backups/<name>/
  index.json                            # manifest: tables, files, sizes, created_at
  <table>/year=YYYY/month=MM/*.parquet  # data files
```

Tables without a usable timestamp column are written without date partitions.

---

## Building from source

```bash
go build -o coldplane            # current platform
./scripts/build.sh               # all platforms (Linux and Windows via Docker)
./scripts/build.sh macos         # darwin arm64 + amd64
```

Output lands in `dist/` with a `checksums.txt`. Override the version with `VERSION=1.2.0 ./scripts/build.sh`.

---

## Running tests

```bash
./scripts/test.sh              # unit tests (default)
./scripts/test.sh local        # integration tests against Docker databases
./scripts/test.sh all          # both
```

Integration tests need the Docker databases up. Seed data loads on first start:

```bash
docker compose up -d                       # PostgreSQL :5432, MySQL :3306
docker compose down -v && docker compose up -d   # re-seed
```

Or directly:

```bash
go test ./...
COLDPLANE_LOCAL_TEST=1 go test -tags integration -v -timeout 10m -run TestLocal ./integration/
```

| Test | What it does |
|------|-------------|
| `TestLocalPostgresFlow` / `TestLocalMySQLFlow` | Archive, list, query, delete |
| `TestLocalPostgresAllTypes` / `TestLocalMySQLAllTypes` | Diverse column types round-trip through Parquet |
| `TestLocalMultiTable` | Multi-table backup and cross-table queries |
| `TestLocalAnalyzePostgres` / `TestLocalAnalyzeMySQL` | Age analysis report |
| `TestLocalBackupWithWhere_*` | `--where` filtering |

---

## Shell completion

```bash
source <(coldplane completion bash)                       # Bash
coldplane completion zsh > "${fpath[1]}/_coldplane"       # Zsh
coldplane completion fish | source                        # Fish
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
