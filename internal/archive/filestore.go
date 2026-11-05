package archive

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileStore abstracts where Parquet files end up after being written to a temp file.
// LocalFileStore moves them into the backup directory; other stores can be added later.
type FileStore interface {
	StoreFile(ctx context.Context, key string, localPath string) (int64, error)
}

// LocalFileStore moves files to a local directory, preserving the key as a relative path.
type LocalFileStore struct {
	BaseDir string
}

func (l *LocalFileStore) StoreFile(ctx context.Context, key string, localPath string) (int64, error) {
	dest := filepath.Join(l.BaseDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return 0, fmt.Errorf("create directory: %w", err)
	}

	// Try rename first (fast, same filesystem)
	if err := os.Rename(localPath, dest); err == nil {
		stat, err := os.Stat(dest)
		if err != nil {
			return 0, fmt.Errorf("stat after rename: %w", err)
		}
		return stat.Size(), nil
	}

	// Fallback to copy (cross-filesystem)
	src, err := os.Open(localPath)
	if err != nil {
		return 0, fmt.Errorf("open source: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(dest)
	if err != nil {
		return 0, fmt.Errorf("create dest: %w", err)
	}
	defer dst.Close()

	written, err := io.Copy(dst, src)
	if err != nil {
		return 0, fmt.Errorf("copy file: %w", err)
	}

	// Remove the original temp file
	os.Remove(localPath)
	return written, nil
}
