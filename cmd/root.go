// cmd/root.go
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/avantexa/coldplane/internal/dbutil"
	"github.com/avantexa/coldplane/internal/duck"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/avantexa/coldplane/internal/analyze"
	"github.com/avantexa/coldplane/internal/archive"
	"github.com/avantexa/coldplane/internal/query"
	"github.com/avantexa/coldplane/internal/term"

	prettytable "github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "coldplane",
	Short: "Archive cold relational data to Parquet",
	Long: `coldplane archives cold data from PostgreSQL and MySQL tables into Parquet
files on local disk, and lets you query those archives with DuckDB.`,
}

// =============== COMPLETION ===============
var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion scripts",
	Long: `Generate autocompletion scripts for the coldplane CLI.

Bash:
  source <(coldplane completion bash)

Zsh:
  echo "autoload -U compinit; compinit" >> ~/.zshrc
  coldplane completion zsh > "${fpath[1]}/_coldplane"

Fish:
  coldplane completion fish | source

PowerShell:
  coldplane completion powershell | Out-String | Invoke-Expression
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			_ = rootCmd.GenBashCompletion(os.Stdout)
		case "zsh":
			_ = rootCmd.GenZshCompletion(os.Stdout)
		case "fish":
			_ = rootCmd.GenFishCompletion(os.Stdout, true)
		case "powershell":
			_ = rootCmd.GenPowerShellCompletionWithDesc(os.Stdout)
		}
	},
}

// =============== BACKUP GROUP ===============
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Manage backups (create, list, delete)",
}

var (
	dbURL      string
	tables     string
	backupName string
	chunkSize  int
	workers    int
	whereFlag  string
)

var backupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new backup from your database",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		fmt.Println(term.Cyan("Starting backup creation..."))

		// Detect DB type BEFORE normalization strips the protocol prefix.
		dbType := detectDBType(dbURL)
		dbURL = normalizeDSN(dbURL)

		tbls := splitList(tables)

		// Auto-generate backupName if not provided
		if backupName == "" {
			now := time.Now().UTC().Format("20060102_150405")
			dbName := dbutil.DBNameFromDSN(dbURL)
			if len(tbls) == 1 {
				backupName = fmt.Sprintf("%s_%s", tbls[0], now)
			} else if len(tbls) > 1 {
				backupName = fmt.Sprintf("multi_%s_%s", dbName, now)
			} else {
				backupName = fmt.Sprintf("%s_%s", dbName, now)
			}
			fmt.Printf("Auto-generated backup name: %s\n", backupName)
		}

		opts := archive.DefaultOptions()
		if chunkSize > 0 {
			opts.ChunkSize = chunkSize
		}
		if workers > 0 {
			opts.Workers = workers
		}
		opts.Where = whereFlag

		var err error
		switch dbType {
		case "mysql":
			fmt.Printf("[mysql] Archiving tables: %s\n", strings.Join(tbls, ", "))
			err = archive.ArchiveMySQL(ctx, dbURL, tbls, backupName, opts)
		case "postgres":
			fmt.Printf("[postgres] Archiving tables: %s\n", strings.Join(tbls, ", "))
			err = archive.ArchivePostgres(ctx, dbURL, tbls, backupName, opts)
		default:
			return fmt.Errorf("%s: expected postgres://, pg://, or mysql:// prefix", term.ColorError("unsupported database URL format"))
		}
		if err != nil || !createVerify {
			return err
		}
		return verifyBackup(ctx, archive.ResolveBackupDir(backupName), dbURL, true)
	},
}

var backupListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List existing backups",
	RunE: func(cmd *cobra.Command, args []string) error {
		return listBackups()
	},
}

func listBackups() error {
	backups, err := archive.ListBackups("")
	if err != nil {
		return err
	}
	renderBackups(os.Stdout, backups)
	return nil
}

// renderBackups prints the backups as a table, newest first.
func renderBackups(w io.Writer, backups []archive.BackupInfo) {
	if len(backups) == 0 {
		fmt.Fprintln(w, "No backups found.")
		return
	}
	t := prettytable.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(prettytable.StyleRounded)
	t.AppendHeader(prettytable.Row{"Name", "Database", "DB Type", "Tables", "Created At", "Size", "Verified", "Path"})
	for _, b := range backups {
		t.AppendRow(prettytable.Row{
			b.Name,
			b.DBName,
			b.DBType,
			strings.Join(b.Tables, ", "),
			b.CreatedAt,
			humanSizeInt64(b.SizeBytes),
			b.Verified,
			b.Path,
		})
	}
	t.Render()
}

var (
	verifyDBURL      string
	verifyNoChecksum bool
	createVerify     bool
)

var backupVerifyCmd = &cobra.Command{
	Use:   "verify <name>",
	Args:  cobra.ExactArgs(1),
	Short: "Compare a backup with its source database and record the result",
	Long: `Counts the rows of every archived table in the source (with the backup's
--where filter) and in the Parquet files, and compares an order-independent
hash of the rows. The outcome is written into the backup's index.json; a
backup is marked verified only when every table passes.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if verifyDBURL == "" {
			return fmt.Errorf("you must specify --db-url")
		}
		return verifyBackup(ctx, archive.ResolveBackupDir(args[0]), normalizeDSN(verifyDBURL), !verifyNoChecksum)
	},
}

// verifyBackup runs the verification, prints the per-table result and fails
// the command unless every table passed.
func verifyBackup(ctx context.Context, backupDir, dsn string, checksum bool) error {
	fmt.Println(term.Cyan("Verifying backup against the source database..."))
	m, err := archive.Verify(ctx, backupDir, dsn, archive.VerifyOptions{Checksum: checksum})
	if err != nil {
		return fmt.Errorf("%s: %w", term.ColorError("verification failed"), err)
	}
	renderVerification(os.Stdout, m)
	if m.VerifiedAt == "" {
		return fmt.Errorf("%s", term.ColorError("verification failed: the archive does not match the source"))
	}
	fmt.Printf("Backup %s verified.\n", m.Name)
	return nil
}

// renderVerification prints one row per table with counts, checksum outcome and status.
func renderVerification(w io.Writer, m archive.Manifest) {
	t := prettytable.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(prettytable.StyleRounded)
	t.AppendHeader(prettytable.Row{"Table", "Source Rows", "Archive Rows", "Checksum", "Status"})
	for _, tb := range m.Tables {
		v := tb.Verified
		if v == nil {
			t.AppendRow(prettytable.Row{tb.Name, "-", "-", "-", "not verified"})
			continue
		}
		checksum := "skipped"
		switch {
		case v.SourceChecksum == "" && v.ArchiveChecksum == "":
		case v.SourceChecksum == v.ArchiveChecksum:
			checksum = "match"
		default:
			checksum = "differs"
		}
		status := v.Status
		if v.Error != "" {
			status += ": " + v.Error
		}
		t.AppendRow(prettytable.Row{tb.Name, v.SourceRows, v.ArchiveRows, checksum, status})
	}
	t.Render()
}

var (
	purgeDBURL     string
	purgeBatchSize int
	purgeYes       bool
)

var backupPurgeCmd = &cobra.Command{
	Use:   "purge-source <name>",
	Args:  cobra.ExactArgs(1),
	Short: "Delete the archived rows of a verified backup from the source database",
	Long: `Removes from the source database the rows a backup archived, using the
backup's own --where filter, in batches that each commit on their own.

It refuses unless the backup is verified against this database and every
table still has exactly the row count that was verified. Without --yes it
is a dry run that only reports what would be deleted.`,
	Example: `  coldplane backup purge-source events_20250101_120000 --db-url "postgres://..."         # dry run
  coldplane backup purge-source events_20250101_120000 --db-url "postgres://..." --yes   # delete`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if purgeDBURL == "" {
			return fmt.Errorf("you must specify --db-url")
		}
		opts := archive.PurgeOptions{BatchSize: purgeBatchSize, Execute: purgeYes}
		if purgeYes {
			fmt.Println(term.Cyan("Deleting archived rows from the source database..."))
		} else {
			fmt.Println(term.Cyan("Dry run: counting the rows a purge would delete..."))
		}
		results, err := archive.Purge(ctx, archive.ResolveBackupDir(args[0]), normalizeDSN(purgeDBURL), opts)
		if err != nil {
			return fmt.Errorf("%s: %w", term.ColorError("purge refused"), err)
		}
		renderPurge(os.Stdout, results, purgeYes)
		return nil
	},
}

// renderPurge prints the per-table rows deleted (or to be deleted).
func renderPurge(w io.Writer, results []archive.PurgeResult, executed bool) {
	header := "Rows To Delete"
	if executed {
		header = "Rows Deleted"
	}
	t := prettytable.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(prettytable.StyleRounded)
	t.AppendHeader(prettytable.Row{"Table", header})
	var total int64
	for _, r := range results {
		t.AppendRow(prettytable.Row{r.Table, r.Rows})
		total += r.Rows
	}
	t.Render()
	if executed {
		fmt.Fprintf(w, "Deleted %d rows from the source.\n", total)
	} else {
		fmt.Fprintf(w, "%d rows would be deleted. Re-run with --yes to delete them.\n", total)
	}
}

var backupDeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Aliases: []string{"rm"},
	Args:    cobra.ExactArgs(1),
	Short:   "Delete a backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		return archive.DeleteBackup(args[0])
	},
}

// =============== QUERY ===============
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Run SQL queries against archived backups",
}

var (
	sqlFlag    string
	queryTable string
	backupID   string
	output     string
)

var queryRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a SQL query against an archived backup",
	Example: `
  coldplane query run \
    --backup-id orders_20250101_120000 \
    --table orders \
    --sql "SELECT COUNT(*) FROM restore_input WHERE amount > 1000" \
    --output json`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if backupID == "" {
			return fmt.Errorf("%s", term.ColorError("--backup-id is required"))
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		opts := query.QueryOptions{
			Table:  queryTable,
			SQL:    sqlFlag,
			Output: output,
		}

		fmt.Println(term.Cyan("Running query against archived backup..."))
		if err := query.Run(ctx, backupID, opts); err != nil {
			return fmt.Errorf("%s: %w", term.ColorError("query failed"), err)
		}
		return nil
	},
}

// =============== ANALYZE ===============
var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze database tables to find old data",
	Long: `Connects read-only to your database and reports how much data is older
than a given threshold. Old data is not necessarily stale — you know your data
best. No data leaves your machine.`,
	Example: `  coldplane analyze --db-url "postgres://user:pass@host:5432/mydb" --tables "orders,events"
  coldplane analyze --db-url "mysql://user:pass@host:3306/mydb" --all --older-than 1y
  coldplane analyze --db-url "postgres://..." --all --json`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		dbURL, _ := cmd.Flags().GetString("db-url")
		tables, _ := cmd.Flags().GetString("tables")
		all, _ := cmd.Flags().GetBool("all")
		if dbURL == "" {
			return fmt.Errorf("you must specify --db-url")
		}
		if tables == "" && !all {
			return fmt.Errorf("you must specify --tables or --all")
		}
		if tables != "" && all {
			return fmt.Errorf("cannot use both --tables and --all")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		dbURL, _ := cmd.Flags().GetString("db-url")
		originalDBURL := dbURL
		tablesFlag, _ := cmd.Flags().GetString("tables")
		all, _ := cmd.Flags().GetBool("all")
		olderThan, _ := cmd.Flags().GetString("older-than")
		dateColumn, _ := cmd.Flags().GetString("date-column")
		asJSON, _ := cmd.Flags().GetBool("json")

		// Detect DB type before normalization strips the prefix
		dbType := detectDBType(dbURL)
		dbName := dbutil.DBNameFromDSN(dbURL)
		dbURL = normalizeDSN(dbURL)

		duration, err := analyze.ParseDuration(olderThan)
		if err != nil {
			return fmt.Errorf("invalid --older-than value: %w", err)
		}

		fmt.Println(term.Cyan("Analyzing database..."))

		opts := analyze.Options{
			DBURL:        dbURL,
			DBType:       dbType,
			DBName:       dbName,
			All:          all,
			OlderThan:    duration,
			OlderThanRaw: olderThan,
			DateColumn:   dateColumn,
		}
		if tablesFlag != "" {
			opts.Tables = splitList(tablesFlag)
		}

		report, err := analyze.Run(ctx, opts)
		if err != nil {
			return err
		}

		if len(report.Tables) == 0 {
			fmt.Println("No tables with date/timestamp columns found to analyze.")
			return nil
		}

		if asJSON {
			raw, _ := json.MarshalIndent(report, "", "  ")
			fmt.Println(string(raw))
			return nil
		}

		renderAnalyzeReport(os.Stdout, report)
		if suggestions := archiveSuggestions(report, originalDBURL); len(suggestions) > 0 {
			fmt.Println("To archive:")
			fmt.Println(strings.Join(suggestions, "\n\n"))
		}
		return nil
	},
}

// =============== HELPERS ===============

// renderAnalyzeReport prints one row per table and the totals.
func renderAnalyzeReport(w io.Writer, report *analyze.Report) {
	t := prettytable.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(prettytable.StyleRounded)
	t.AppendHeader(prettytable.Row{
		"Table", "Total Rows", "Size", "Date Column", "Oldest Row",
		fmt.Sprintf("Rows older than %s", report.OlderThan), "Est. Archived Size",
	})

	var totalRows, totalArchivable, totalSize, totalEstArchived int64
	for _, tr := range report.Tables {
		t.AppendRow(prettytable.Row{
			tr.TableName,
			humanCount(tr.TotalRows),
			humanSizeInt64(tr.SizeBytes),
			tr.DateColumn,
			tr.OldestRow.Format("2006-01-02"),
			fmt.Sprintf("%s (%d%%)", humanCount(tr.ArchivableRows), tr.ArchivablePct),
			"~" + humanSizeInt64(tr.EstArchivedBytes),
		})
		totalRows += tr.TotalRows
		totalArchivable += tr.ArchivableRows
		totalSize += tr.SizeBytes
		totalEstArchived += tr.EstArchivedBytes
	}
	t.Render()

	totalPct := 0
	if totalRows > 0 {
		totalPct = int(totalArchivable * 100 / totalRows)
	}
	fmt.Fprintf(w, "\n%s rows (%d%%) across %d tables are older than %s.\n",
		humanCount(totalArchivable), totalPct, len(report.Tables), report.OlderThan)
	fmt.Fprintf(w, "Estimated archive size: ~%s (compressed from ~%s)\n",
		humanSizeInt64(totalEstArchived), humanSizeInt64(totalSize))
	fmt.Fprintln(w, "Note: old data is not necessarily stale. Review the tables above and decide what to archive.")
}

// archiveSuggestions returns one 'backup create' command per date column
// that has archivable rows, grouping the tables that share the column so
// the --where clause fits every table in the command.
func archiveSuggestions(report *analyze.Report, dbURL string) []string {
	type group struct {
		dateCol string
		tables  []string
	}
	var groups []*group
	byCol := map[string]*group{}
	for _, tr := range report.Tables {
		if tr.ArchivableRows == 0 {
			continue
		}
		g, ok := byCol[tr.DateColumn]
		if !ok {
			g = &group{dateCol: tr.DateColumn}
			byCol[tr.DateColumn] = g
			groups = append(groups, g)
		}
		g.tables = append(g.tables, tr.TableName)
	}
	cutoff := report.CutoffDate.Format("2006-01-02")
	var out []string
	for _, g := range groups {
		where := fmt.Sprintf("%s < '%s'", g.dateCol, cutoff)
		out = append(out, fmt.Sprintf("  coldplane backup create \\\n    --db-url %q \\\n    --tables %q \\\n    --where %q",
			dbURL, strings.Join(g.tables, ","), where))
	}
	return out
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.0fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func humanSizeInt64(sz int64) string {
	switch {
	case sz > 1<<30:
		return fmt.Sprintf("%.2f GB", float64(sz)/(1<<30))
	case sz > 1<<20:
		return fmt.Sprintf("%.2f MB", float64(sz)/(1<<20))
	case sz > 1<<10:
		return fmt.Sprintf("%.2f KB", float64(sz)/(1<<10))
	default:
		return fmt.Sprintf("%d B", sz)
	}
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// normalizeDSN normalizes pg:// → postgres:// and mysql:// via archive helper.
func normalizeDSN(dsn string) string {
	if strings.HasPrefix(dsn, "pg://") {
		return "postgres://" + strings.TrimPrefix(dsn, "pg://")
	}
	if strings.HasPrefix(dsn, "mysql://") {
		return archive.NormalizeMySQLDSN(dsn)
	}
	return dsn
}

// detectDBType returns "mysql", "postgres", or "unknown" from a DSN.
func detectDBType(dsn string) string {
	switch {
	case strings.HasPrefix(dsn, "mysql://"):
		return "mysql"
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "pg://"):
		return "postgres"
	case strings.Contains(dsn, "@tcp("):
		// Go MySQL driver format: user:pass@tcp(host:port)/dbname
		return "mysql"
	default:
		return "unknown"
	}
}

// =============== INIT ===============
var verbose bool

func init() {
	// global flags
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Log the SQL sent to the database")
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if verbose {
			duck.SetDebug(true)
		}
	}

	// backup create flags
	backupCreateCmd.Flags().StringVarP(&dbURL, "db-url", "d", "", "Database connection string (mysql://... or postgres://...)")
	backupCreateCmd.Flags().StringVar(&tables, "tables", "", "Comma-separated list of tables to back up")
	backupCreateCmd.Flags().StringVar(&backupName, "name", "", "Optional human-friendly name for the backup")
	backupCreateCmd.Flags().IntVar(&chunkSize, "chunk-size", 0, "Rows per chunk (default: 2500000)")
	backupCreateCmd.Flags().IntVar(&workers, "workers", 0, "Parallel archive workers (default: 4)")
	backupCreateCmd.Flags().StringVar(&whereFlag, "where", "", "SQL WHERE clause to filter rows (e.g. \"created_at < '2025-01-01'\")")
	backupCreateCmd.Flags().BoolVar(&createVerify, "verify", false, "Verify the backup against the source right after archiving")

	// backup verify flags
	backupVerifyCmd.Flags().StringVarP(&verifyDBURL, "db-url", "d", "", "Database connection string of the source (required)")
	backupVerifyCmd.Flags().BoolVar(&verifyNoChecksum, "no-checksum", false, "Compare row counts only")

	// backup purge-source flags
	backupPurgeCmd.Flags().StringVarP(&purgeDBURL, "db-url", "d", "", "Database connection string of the source (required)")
	backupPurgeCmd.Flags().IntVar(&purgeBatchSize, "batch-size", 5000, "Rows per DELETE statement")
	backupPurgeCmd.Flags().BoolVar(&purgeYes, "yes", false, "Actually delete; without it the command only reports")

	// validation
	backupCreateCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if dbURL == "" {
			return fmt.Errorf("you must specify --db-url")
		}
		if tables == "" {
			return fmt.Errorf("you must specify --tables")
		}
		return nil
	}

	// assemble backup group
	backupCmd.AddCommand(
		backupCreateCmd,
		backupListCmd,
		backupVerifyCmd,
		backupPurgeCmd,
		backupDeleteCmd,
	)

	// query
	queryRunCmd.Flags().StringVarP(&sqlFlag, "sql", "s", "", "SQL query to execute (default: SELECT * FROM restore_input)")
	queryRunCmd.Flags().StringVarP(&queryTable, "table", "t", "", "Specific table to query (optional)")
	queryRunCmd.Flags().StringVarP(&backupID, "backup-id", "b", "", "Backup name to query (required)")
	queryRunCmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table|json|csv")
	queryCmd.AddCommand(queryRunCmd)

	// analyze
	analyzeCmd.Flags().StringP("db-url", "d", "", "Database connection string (required)")
	analyzeCmd.Flags().String("tables", "", "Comma-separated list of tables to analyze")
	analyzeCmd.Flags().Bool("all", false, "Analyze all tables in the database")
	analyzeCmd.Flags().String("older-than", "18m", "Age threshold (e.g. 18m, 1y, 6m, 90d)")
	analyzeCmd.Flags().String("date-column", "", "Column to use for age detection (auto-detected if omitted)")
	analyzeCmd.Flags().Bool("json", false, "Output results as JSON")

	// root
	rootCmd.AddCommand(
		completionCmd,
		backupCmd,
		queryCmd,
		analyzeCmd,
	)
}

func Execute() error { return rootCmd.Execute() }
