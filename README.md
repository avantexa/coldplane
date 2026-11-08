# coldplane

`coldplane` archives cold data from PostgreSQL and MySQL tables into Parquet files on local disk, and lets you query those archives with DuckDB without restoring them.

Typical use: find the tables holding years of old rows, archive the old rows to Parquet, verify that the archive matches the database, then delete the rows from production.

- **Nothing leaves your machine.** Backups live under `~/.coldplane/backups/<name>/`.
- **No account, no server.** The binary talks only to your database and your disk.
- **Open format.** Parquet files partitioned by `year=YYYY/month=MM`, readable by DuckDB, Spark, pandas, or anything else.

Both `postgres://` and `pg://` connection schemes are supported for PostgreSQL.

### How it reads your database

coldplane embeds [DuckDB](https://duckdb.org) and uses its `postgres` and `mysql` extensions to pull each chunk of rows straight into Parquet, so column types survive: decimals stay `DECIMAL`, booleans `BOOLEAN`, dates `DATE`, uuids `UUID`, arrays arrays. Each chunk is a plain `SELECT ... WHERE <range>` in your database's own SQL, so `--where` is written in PostgreSQL or MySQL syntax, and the database only ever sees bounded range queries.

The two extensions are not in the binary. The first time you archive from an engine, coldplane downloads its extension (about 40 MB) into `~/.duckdb/extensions` and says so. On machines without internet access, pre-install the extensions and point `COLDPLANE_DUCKDB_EXTENSIONS` at that directory. `--verbose` logs every SQL statement sent to your database.

---

## Installation

On macOS or Linux (amd64 or arm64). The Linux binaries need glibc 2.34 and the libstdc++ of GCC 12 or later: Ubuntu 22.04+, Debian 12+, Fedora 36+. RHEL 9, Rocky 9 and Amazon Linux 2023 ship GCC 11's libstdc++ and are not supported yet.

```bash
curl -fsSL https://raw.githubusercontent.com/avantexa/coldplane/main/scripts/install.sh | sh
```

The script picks the binary for your platform, checks it against the release's `checksums.txt`, and installs it to `/usr/local/bin`, using sudo only if that directory is not writable. To install somewhere you own instead:

```bash
curl -fsSL https://raw.githubusercontent.com/avantexa/coldplane/main/scripts/install.sh | COLDPLANE_INSTALL_DIR="$HOME/.local/bin" sh
```

On Windows, download `coldplane-windows-amd64.exe` from [GitHub Releases](https://github.com/avantexa/coldplane/releases/latest); `checksums.txt` there lists every binary's SHA-256. The macOS binaries are not signed: one downloaded with a browser is quarantined, so clear that with `xattr -d com.apple.quarantine coldplane` (the install script's download is not quarantined).

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
| `--verify` | | Run `backup verify` right after archiving |
| `--older-than` | | Instead of `--where`: archive rows whose first date/timestamp column is older than this, e.g. `18m`, `2y`, `90d` |
| `--since-last` | | With `--older-than`: start where the newest verified backup of the same table stopped, so a monthly run archives only new cold rows |

A monthly job looks like this:

```bash
coldplane backup create --db-url "$DB" --tables "events,logs" --older-than 18m --since-last --verify
coldplane backup purge-source "$(coldplane backup list | ...)" --db-url "$DB" --yes
```

The first run archives everything older than 18 months. Each later run archives only the rows that have crossed the threshold since the previous verified run, so the archives never overlap and together cover every cold row. The manifest records the `cutoff` and `since` instants.

### `backup verify` — Prove the archive matches the source

Before deleting anything from production, check that the archive holds exactly the rows the database holds for the same filter.

```bash
coldplane backup verify events_20250101_120000 --db-url "postgres://user:pass@host:5432/mydb"
```

```
╭────────┬─────────────┬──────────────┬──────────┬────────╮
│ TABLE  │ SOURCE ROWS │ ARCHIVE ROWS │ CHECKSUM │ STATUS │
├────────┼─────────────┼──────────────┼──────────┼────────┤
│ events │     1842210 │      1842210 │ match    │ pass   │
│ logs   │      903311 │       903311 │ match    │ pass   │
╰────────┴─────────────┴──────────────┴──────────┴────────╯
Backup events_20250101_120000 verified.
```

For each table it counts the rows in the source, using the backup's own `--where`, and in the Parquet files, and compares an order-independent hash of every row. The result is written into the backup's `index.json`, `backup list` shows it in the Verified column, and the command exits non-zero unless every table passes. `--no-checksum` compares counts only.

| Flag | Short | Description |
|------|-------|-------------|
| `--db-url` | `-d` | Connection string of the source database (required) |
| `--no-checksum` | | Compare row counts only |

### `backup purge-source` — Remove the archived rows from the database

The step the rest exists for. It deletes from the source exactly the rows a verified backup archived, using the backup's own `--where`, in batches that each commit on their own.

```bash
coldplane backup purge-source events_20250101_120000 --db-url "postgres://..."         # dry run
coldplane backup purge-source events_20250101_120000 --db-url "postgres://..." --yes   # delete
```

```
╭────────┬────────────────╮
│ TABLE  │ ROWS TO DELETE │
├────────┼────────────────┤
│ events │        1842210 │
│ logs   │         903311 │
╰────────┴────────────────╯
2745521 rows would be deleted. Re-run with --yes to delete them.
```

Guards, all of which stop the command before any `DELETE`:

- the backup must be verified (`backup verify`) with every table passing;
- `--db-url` must point at the database the backup came from;
- each table must still hold exactly the row count that was verified. If anything changed since, verify again first.

Without `--yes` it is always a dry run. The purge is recorded in the backup's `index.json`.

| Flag | Short | Description |
|------|-------|-------------|
| `--db-url` | `-d` | Connection string of the source database (required) |
| `--yes` | | Delete; without it the command only reports |
| `--batch-size` | | Rows per `DELETE` statement (default: 5000) |

### `backup restore` — Bring the data back

Loads a backup's tables into a PostgreSQL or MySQL database, straight from the Parquet files. The target does not have to be the engine the data came from; DuckDB maps the types.

```bash
# Into a staging database, creating the tables from the archive's schema
coldplane backup restore events_20250101_120000 --to "postgres://user:pass@host:5432/staging" --create

# One table, under another name, into MySQL
coldplane backup restore events_20250101_120000 --to "mysql://user:pass@host:3306/analytics" \
  --table events --as events_2024 --create
```

Rows are appended to existing target tables. A missing table is an error that lists the columns unless `--create` is given.

| Flag | Short | Description |
|------|-------|-------------|
| `--to` | | Target connection string, `postgres://` or `mysql://` (required) |
| `--table` | | Comma-separated tables to restore (default: all) |
| `--as` | | Target table name, when restoring a single table |
| `--create` | | Create missing target tables from the archive's schema |

### `backup list` — List backups

```bash
coldplane backup list
```

```
╭────────────────────────┬──────────┬──────────┬──────────────┬──────────────────────┬─────────┬──────────┬──────────────────────────────────────────╮
│ Name                   │ Database │ DB Type  │ Tables       │ Created At           │ Size    │ Verified │ Path                                     │
├────────────────────────┼──────────┼──────────┼──────────────┼──────────────────────┼─────────┼──────────┼──────────────────────────────────────────┤
│ events_20250101_120000 │ mydb     │ postgres │ events, logs │ 2025-01-01T12:00:00Z │ 8.40 GB │ yes      │ ~/.coldplane/backups/events_20250101_... │
╰────────────────────────┴──────────┴──────────┴──────────────┴──────────────────────┴─────────┴──────────┴──────────────────────────────────────────╯
```

### `query run` — Query archived data without restoring

Runs SQL with DuckDB over the backup's Parquet files. Each archived table is a view under its own name, so the tables of one backup can be joined.

```bash
coldplane query run \
  --backup-id events_20250101_120000 \
  --sql "SELECT user_id, COUNT(*) AS total FROM events WHERE type = 'error' GROUP BY user_id ORDER BY total DESC" \
  --output table
```

```bash
# JSON — one object per line, pipe to jq
coldplane query run -b events_20250101_120000 -s "SELECT * FROM events" -o json | jq 'select(.severity == "critical")'

# CSV — for spreadsheets or pipelines
coldplane query run -b events_20250101_120000 -s "SELECT * FROM events" -o csv > report.csv

# Join two tables archived in the same backup
coldplane query run -b events_20250101_120000 -s "SELECT l.message FROM logs l JOIN events e ON e.id = l.event_id"
```

| Flag | Short | Description |
|------|-------|-------------|
| `--backup-id` | `-b` | Backup name, or a path to a backup directory (required) |
| `--sql` | `-s` | SQL to run; tables are views under their own names (required) |
| `--output` | `-o` | Output format: `table` (default), `json`, `csv` |

### `backup prune` — Remove old backups

```bash
coldplane backup prune --older-than 2y          # dry run
coldplane backup prune --older-than 2y --yes    # remove
```

Removes backups created longer ago than the threshold. Backups that were never verified are kept unless `--include-unverified` is given.

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
