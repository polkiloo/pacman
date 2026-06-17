package postgres

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDataDirArchiveForReinitMovesExistingDataDirAndRecreatesEmpty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	result, err := (DataDirArchive{
		DataDir:     dataDir,
		ArchiveName: "reinit-20260617T120000Z",
	}).ArchiveForReinit()
	if err != nil {
		t.Fatalf("archive data dir: %v", err)
	}

	if !result.Archived {
		t.Fatalf("expected existing data dir to be archived: %+v", result)
	}
	if result.DataDir != dataDir {
		t.Fatalf("data dir: got %q, want %q", result.DataDir, dataDir)
	}
	if result.ArchivePath != filepath.Join(root, defaultReinitArchiveDirName, "data-reinit-20260617T120000Z") {
		t.Fatalf("unexpected archive path: %q", result.ArchivePath)
	}

	if _, err := os.Stat(filepath.Join(result.ArchivePath, "PG_VERSION")); err != nil {
		t.Fatalf("expected marker in archive: %v", err)
	}
	if entries, err := os.ReadDir(dataDir); err != nil {
		t.Fatalf("read recreated data dir: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("expected recreated data dir to be empty, got %+v", entries)
	}
}

func TestDataDirArchiveForReinitCreatesMissingDataDir(t *testing.T) {
	t.Parallel()

	dataDir := filepath.Join(t.TempDir(), "missing", "data")
	result, err := (DataDirArchive{DataDir: dataDir}).ArchiveForReinit()
	if err != nil {
		t.Fatalf("archive missing data dir: %v", err)
	}
	if result.Archived {
		t.Fatalf("expected missing data dir to avoid archive, got %+v", result)
	}
	if info, err := os.Stat(dataDir); err != nil {
		t.Fatalf("expected data dir to be created: %v", err)
	} else if !info.IsDir() {
		t.Fatalf("expected data dir, got mode %v", info.Mode())
	}
}

func TestDataDirArchiveForReinitIsIdempotentAfterArchive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	archive := DataDirArchive{DataDir: dataDir, ArchiveName: "op-1"}
	first, err := archive.ArchiveForReinit()
	if err != nil {
		t.Fatalf("first archive: %v", err)
	}

	second, err := archive.ArchiveForReinit()
	if err != nil {
		t.Fatalf("second archive: %v", err)
	}
	if !second.Archived || second.ArchivePath != first.ArchivePath {
		t.Fatalf("unexpected retry result: first=%+v second=%+v", first, second)
	}
	if _, err := os.Stat(filepath.Join(first.ArchivePath, "PG_VERSION")); err != nil {
		t.Fatalf("expected original archive to remain intact: %v", err)
	}
}

func TestDataDirArchiveForReinitRejectsArchiveConflictWithNonEmptyDataDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	archivePath := filepath.Join(root, defaultReinitArchiveDirName, "data-op-1")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	if err := os.MkdirAll(archivePath, 0o700); err != nil {
		t.Fatalf("create archive conflict: %v", err)
	}

	_, err := (DataDirArchive{DataDir: dataDir, ArchiveName: "op-1"}).ArchiveForReinit()
	if !errors.Is(err, ErrDataDirArchiveConflict) {
		t.Fatalf("archive conflict error: got %v, want %v", err, ErrDataDirArchiveConflict)
	}
}

func TestDataDirArchiveForReinitRejectsUnsafePaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	testCases := []struct {
		name    string
		archive DataDirArchive
		wantErr error
	}{
		{
			name:    "missing data dir",
			archive: DataDirArchive{},
			wantErr: ErrDataDirRequired,
		},
		{
			name: "archive root inside data dir",
			archive: DataDirArchive{
				DataDir:     dataDir,
				ArchiveRoot: filepath.Join(dataDir, "archive"),
			},
			wantErr: ErrDataDirUnsafe,
		},
		{
			name: "data dir symlink",
			archive: DataDirArchive{
				DataDir: func() string {
					linkPath := filepath.Join(root, "data-link")
					if err := os.Symlink(dataDir, linkPath); err != nil {
						t.Fatalf("create data dir symlink: %v", err)
					}
					return linkPath
				}(),
			},
			wantErr: ErrDataDirUnsafe,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.archive.ArchiveForReinit()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("archive error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestDataDirArchiveForReinitUsesTimestampWhenNameOmitted(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	result, err := (DataDirArchive{
		DataDir: dataDir,
		Now: func() time.Time {
			return time.Date(2026, time.June, 17, 12, 0, 0, 123, time.UTC)
		},
	}).ArchiveForReinit()
	if err != nil {
		t.Fatalf("archive data dir: %v", err)
	}

	want := filepath.Join(root, defaultReinitArchiveDirName, "data-20260617T120000.000000123Z")
	if result.ArchivePath != want {
		t.Fatalf("archive path: got %q, want %q", result.ArchivePath, want)
	}
}
