// cmd/root.go
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
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

		// Auto-generate backupName if not provided
		if backupName == "" {
			now := time.Now().UTC().Format("20060102_150405")
			dbName := archive.ExtractDBName(dbURL)
			tbls := splitList(tables)
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

		tbls := splitList(tables)

		switch dbType {
		case "mysql":
			fmt.Printf("[mysql] Archiving tables: %s\n", strings.Join(tbls, ", "))
			return archive.ArchiveMySQL(ctx, dbURL, tbls, backupName, opts)
		case "postgres":
			fmt.Printf("[postgres] Archiving tables: %s\n", strings.Join(tbls, ", "))
			return archive.ArchivePostgres(ctx, dbURL, tbls, backupName, opts)
		default:
			return fmt.Errorf("%s: expected postgres://, pg://, or mysql:// prefix", term.ColorError("unsupported database URL format"))
		}
	},
}

var backupListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List existing backups",
	RunE: func(cmd *cobra.Command, args []string) error {
		return listLocalBackups()
	},
}

func listLocalBackups() error {
	backups, err := archive.ListLocalBackups("")
	if err != nil {
		return err
	}
	renderLocalBackups(os.Stdout, backups)
	return nil
}

// renderLocalBackups prints the backups as a table, newest first.
func renderLocalBackups(w io.Writer, backups []archive.LocalBackupInfo) {
	if len(backups) == 0 {
		fmt.Fprintln(w, "No backups found.")
		return
	}
	t := prettytable.NewWriter()
	t.SetOutputMirror(w)
	t.SetStyle(prettytable.StyleRounded)
	t.AppendHeader(prettytable.Row{"Name", "Database", "DB Type", "Tables", "Created At", "Size", "Path"})
	for _, b := range backups {
		t.AppendRow(prettytable.Row{
			b.Name,
			b.DBName,
			b.DBType,
			strings.Join(b.Tables, ", "),
			b.CreatedAt,
			humanSizeInt64(b.SizeBytes),
			b.Path,
		})
	}
	t.Render()
}

var backupDeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Aliases: []string{"rm"},
	Args:    cobra.ExactArgs(1),
	Short:   "Delete a backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		return archive.DeleteLocalBackup(args[0])
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

		opts := query.QueryOptions{
			BackupID: backupID,
			Table:    queryTable,
			SQL:      sqlFlag,
			Output:   output,
		}

		fmt.Println(term.Cyan("Running query against archived backup..."))
		if err := query.RunLocal(cmd.Context(), backupID, opts); err != nil {
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
		dbName := archive.ExtractDBName(dbURL)
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

		// Render table
		t := prettytable.NewWriter()
		t.SetOutputMirror(os.Stdout)
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

		// Summary
		totalPct := 0
		if totalRows > 0 {
			totalPct = int(totalArchivable * 100 / totalRows)
		}
		fmt.Printf("\n%s rows (%d%%) across %d tables are older than %s.\n",
			humanCount(totalArchivable), totalPct, len(report.Tables), report.OlderThan)
		fmt.Printf("Estimated archive size: ~%s (compressed from ~%s)\n",
			humanSizeInt64(totalEstArchived), humanSizeInt64(totalSize))
		fmt.Println("Note: old data is not necessarily stale. Review the tables above and decide what to archive.")

		// Suggestion — group archivable tables by date column so the --where clause is correct
		type tableGroup struct {
			dateCol string
			tables  []string
		}
		var groups []tableGroup
		groupIndex := map[string]int{}

		for _, tr := range report.Tables {
			if tr.ArchivableRows > 0 {
				idx, exists := groupIndex[tr.DateColumn]
				if !exists {
					idx = len(groups)
					groups = append(groups, tableGroup{dateCol: tr.DateColumn})
					groupIndex[tr.DateColumn] = idx
				}
				groups[idx].tables = append(groups[idx].tables, tr.TableName)
			}
		}

		if len(groups) > 0 {
			cutoffStr := report.CutoffDate.Format("2006-01-02")
			fmt.Println("To archive:")
			for _, g := range groups {
				whereClause := fmt.Sprintf("%s < '%s'", g.dateCol, cutoffStr)
				fmt.Printf("  coldplane backup create \\\n    --db-url %q \\\n    --tables %q \\\n    --where %q\n",
					originalDBURL, strings.Join(g.tables, ","), whereClause)
				if len(groups) > 1 {
					fmt.Println()
				}
			}
		}

		return nil
	},
}

// =============== HELPERS ===============

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
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Enable verbose debug output")
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if verbose {
			archive.SetDebug(true)
		}
	}

	// backup create flags
	backupCreateCmd.Flags().StringVarP(&dbURL, "db-url", "d", "", "Database connection string (mysql://... or postgres://...)")
	backupCreateCmd.Flags().StringVar(&tables, "tables", "", "Comma-separated list of tables to back up")
	backupCreateCmd.Flags().StringVar(&backupName, "name", "", "Optional human-friendly name for the backup")
	backupCreateCmd.Flags().IntVar(&chunkSize, "chunk-size", 0, "Rows per chunk (default: 2500000)")
	backupCreateCmd.Flags().IntVar(&workers, "workers", 0, "Parallel archive workers (default: 4)")
	backupCreateCmd.Flags().StringVar(&whereFlag, "where", "", "SQL WHERE clause to filter rows (e.g. \"created_at < '2025-01-01'\")")

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
