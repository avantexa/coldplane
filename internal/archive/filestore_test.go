package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalFileStore_StoreFile(t *testing.T) {
	// Create a temp dir as the base for the local store
	baseDir := t.TempDir()
	store := &LocalFileStore{BaseDir: baseDir}

	// Create a temp file to simulate a Parquet chunk
	tmpFile, err := os.CreateTemp("", "test-chunk-*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("fake parquet data for testing")
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	tmpPath := tmpFile.Name()

	// Store the file with a key that includes subdirectories
	key := "orders/year=2025/month=01/orders-chunk-0-part-0.parquet"
	size, err := store.StoreFile(context.Background(), key, tmpPath)
	if err != nil {
		t.Fatalf("StoreFile failed: %v", err)
	}

	if size != int64(len(content)) {
		t.Errorf("size = %d, want %d", size, len(content))
	}

	// Verify the file exists at the expected path
	destPath := filepath.Join(baseDir, filepath.FromSlash(key))
	data, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("file not found at %s: %v", destPath, err)
	}
	if string(data) != string(content) {
		t.Errorf("file content = %q, want %q", string(data), string(content))
	}

	// Verify the original temp file was removed
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("original temp file should have been removed: %s", tmpPath)
	}
}

func TestLocalFileStore_StoreFile_CreatesDirectories(t *testing.T) {
	baseDir := t.TempDir()
	store := &LocalFileStore{BaseDir: baseDir}

	tmpFile, err := os.CreateTemp("", "test-*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Write([]byte("data"))
	tmpFile.Close()

	// Use a deeply nested key
	key := "a/b/c/d/e/file.parquet"
	_, err = store.StoreFile(context.Background(), key, tmpFile.Name())
	if err != nil {
		t.Fatalf("StoreFile failed: %v", err)
	}

	destPath := filepath.Join(baseDir, filepath.FromSlash(key))
	if _, err := os.Stat(destPath); os.IsNotExist(err) {
		t.Errorf("expected file at %s", destPath)
	}
}

func TestLocalFileStore_StoreFile_EmptyFile(t *testing.T) {
	baseDir := t.TempDir()
	store := &LocalFileStore{BaseDir: baseDir}

	tmpFile, err := os.CreateTemp("", "test-empty-*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	key := "table/chunk.parquet"
	size, err := store.StoreFile(context.Background(), key, tmpFile.Name())
	if err != nil {
		t.Fatalf("StoreFile failed: %v", err)
	}
	if size != 0 {
		t.Errorf("size = %d, want 0", size)
	}
}
