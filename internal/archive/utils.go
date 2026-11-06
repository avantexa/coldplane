package archive

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
)

// ProgressTracker tracks progress for table archiving.
type ProgressTracker struct {
	Table       string
	TotalRows   int64
	DoneRows    int64
	TotalChunks int
	DoneChunks  int
	mu          sync.Mutex
}

func (p *ProgressTracker) AddRows(n int) {
	p.mu.Lock()
	p.DoneRows += int64(n)
	fmt.Printf("[%s] Progress: %d/%s rows, %d/%d chunks\n",
		p.Table,
		p.DoneRows,
		func() string {
			if p.TotalRows == 0 {
				return "?"
			}
			return fmt.Sprintf("%d", p.TotalRows)
		}(),
		p.DoneChunks,
		p.TotalChunks,
	)
	p.mu.Unlock()
}

func (p *ProgressTracker) ChunkDone() {
	p.mu.Lock()
	p.DoneChunks++
	// Do not print rows again here — row logs are handled in AddRows
	fmt.Printf("[%s] Progress: chunks %d/%d complete\n",
		p.Table, p.DoneChunks, p.TotalChunks)
	p.mu.Unlock()
}

// NormalizeMySQLDSN transforms mysql:// URIs to Go MySQL driver format
func NormalizeMySQLDSN(input string) string {
	if !strings.HasPrefix(input, "mysql://") {
		return input
	}

	parsed, err := url.Parse(input)
	if err != nil {
		return input // fallback
	}

	user := ""
	pass := ""
	if parsed.User != nil {
		user = parsed.User.Username()
		pass, _ = parsed.User.Password()
	}

	host := parsed.Host
	db := strings.TrimPrefix(parsed.Path, "/")
	params := parsed.RawQuery

	// Compose the DSN: user:pass@tcp(host:port)/dbname?params
	auth := user
	if pass != "" {
		auth += ":" + pass
	}
	dsn := fmt.Sprintf("%s@tcp(%s)/%s", auth, host, db)
	if params != "" {
		dsn += "?" + params
	}
	return dsn
}
