package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultReinitArchiveDirName = ".pacman-reinit-archive"
const postgresDataDirMode os.FileMode = 0o700

// DataDirArchive describes a safe local data-directory archive operation before
// a destructive reinit restore.
type DataDirArchive struct {
	DataDir     string
	ArchiveRoot string
	ArchiveName string
	Now         func() time.Time
}

// DataDirArchiveResult reports the paths selected by ArchiveForReinit.
type DataDirArchiveResult struct {
	DataDir     string
	ArchivePath string
	Archived    bool
}

// ReinitReplicationVerification reports the local PostgreSQL state that must
// hold before a WAL-G reinit target is accepted as a streaming standby.
type ReinitReplicationVerification struct {
	SystemIdentifier  string
	Timeline          int64
	BackupName        string
	PrimarySlotName   string
	WALReceiverStatus string
	InRecovery        bool
}

// QueryReinitReplicationVerification verifies the local streaming state of a
// restarted reinit standby.
func QueryReinitReplicationVerification(ctx context.Context, address, backupName string) (ReinitReplicationVerification, error) {
	client, err := Connect(address)
	if err != nil {
		return ReinitReplicationVerification{}, err
	}
	defer client.Close()

	return client.QueryReinitReplicationVerification(ctx, backupName)
}

// QueryReinitReplicationVerification verifies the local streaming state of a
// restarted reinit standby through the connected PostgreSQL client.
func (client *Client) QueryReinitReplicationVerification(ctx context.Context, backupName string) (ReinitReplicationVerification, error) {
	observation, err := client.QueryObservation(ctx)
	if err != nil {
		return ReinitReplicationVerification{}, err
	}

	var primarySlotName string
	var walReceiverStatus string
	if err := client.db.QueryRowContext(ctx, queryReinitReplicationVerificationSQL).Scan(&primarySlotName, &walReceiverStatus); err != nil {
		return ReinitReplicationVerification{}, err
	}

	return ReinitReplicationVerification{
		SystemIdentifier:  strings.TrimSpace(observation.Details.SystemIdentifier),
		Timeline:          observation.Details.Timeline,
		BackupName:        strings.TrimSpace(backupName),
		PrimarySlotName:   strings.TrimSpace(primarySlotName),
		WALReceiverStatus: strings.TrimSpace(walReceiverStatus),
		InRecovery:        observation.InRecovery,
	}, nil
}

// ArchiveForReinit moves an existing PostgreSQL data directory to a sibling
// archive location and recreates an empty data directory for restore. It never
// recursively deletes the existing data directory.
func (archive DataDirArchive) ArchiveForReinit() (DataDirArchiveResult, error) {
	dataDir, err := safeDataDirPath(archive.DataDir)
	if err != nil {
		return DataDirArchiveResult{}, err
	}

	archiveRoot, err := archiveRootPath(dataDir, archive.ArchiveRoot)
	if err != nil {
		return DataDirArchiveResult{}, err
	}

	archivePath := filepath.Join(archiveRoot, archiveName(dataDir, archive.ArchiveName, archive.now()))
	result := DataDirArchiveResult{
		DataDir:     dataDir,
		ArchivePath: archivePath,
	}

	info, err := os.Lstat(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(dataDir, postgresDataDirMode); err != nil {
				return DataDirArchiveResult{}, fmt.Errorf("create empty postgres data directory %q: %w", dataDir, err)
			}
			return result, nil
		}
		return DataDirArchiveResult{}, fmt.Errorf("inspect postgres data directory %q: %w", dataDir, err)
	}

	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return DataDirArchiveResult{}, ErrDataDirUnsafe
	}
	owner := fileOwner(info)

	if archiveInfo, statErr := os.Lstat(archivePath); statErr == nil {
		if !archiveInfo.IsDir() {
			return DataDirArchiveResult{}, ErrDataDirArchiveConflict
		}
		if empty, emptyErr := directoryEmpty(dataDir); emptyErr != nil {
			return DataDirArchiveResult{}, emptyErr
		} else if !empty {
			return DataDirArchiveResult{}, ErrDataDirArchiveConflict
		}
		result.Archived = true
		return result, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return DataDirArchiveResult{}, fmt.Errorf("inspect postgres data directory archive %q: %w", archivePath, statErr)
	}

	if err := os.MkdirAll(archiveRoot, 0o700); err != nil {
		return DataDirArchiveResult{}, fmt.Errorf("create postgres data directory archive root %q: %w", archiveRoot, err)
	}

	if err := os.Rename(dataDir, archivePath); err != nil {
		return DataDirArchiveResult{}, fmt.Errorf("archive postgres data directory %q to %q: %w", dataDir, archivePath, err)
	}

	if err := os.MkdirAll(dataDir, postgresSafeDataDirMode(info.Mode().Perm())); err != nil {
		return DataDirArchiveResult{}, fmt.Errorf("recreate postgres data directory %q: %w", dataDir, err)
	}
	if err := chownIfRoot(dataDir, owner); err != nil {
		return DataDirArchiveResult{}, fmt.Errorf("restore postgres data directory owner %q: %w", dataDir, err)
	}

	result.Archived = true
	return result, nil
}

// NormalizeRestoredDataDir makes a freshly restored PGDATA acceptable to
// PostgreSQL before PACMAN attempts pg_ctl start.
func NormalizeRestoredDataDir(dataDir string) error {
	dataDir, err := safeDataDirPath(dataDir)
	if err != nil {
		return err
	}

	info, err := os.Lstat(dataDir)
	if err != nil {
		return fmt.Errorf("inspect restored postgres data directory %q: %w", dataDir, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return ErrDataDirUnsafe
	}

	if os.Geteuid() == 0 {
		parentInfo, err := os.Stat(filepath.Dir(dataDir))
		if err != nil {
			return fmt.Errorf("inspect postgres data directory parent %q: %w", filepath.Dir(dataDir), err)
		}
		parentOwner := fileOwner(parentInfo)
		if parentOwner.valid {
			if err := filepath.WalkDir(dataDir, func(path string, _ os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				return os.Lchown(path, parentOwner.uid, parentOwner.gid)
			}); err != nil {
				return fmt.Errorf("restore postgres data directory ownership %q: %w", dataDir, err)
			}
		}
	} else {
		owner := fileOwner(info)
		if owner.valid && owner.uid != os.Geteuid() {
			return fmt.Errorf("restored postgres data directory %q is owned by uid %d, but pacmand runs as uid %d", dataDir, owner.uid, os.Geteuid())
		}
	}

	if err := os.Chmod(dataDir, postgresSafeDataDirMode(info.Mode().Perm())); err != nil {
		return fmt.Errorf("restore postgres data directory mode %q: %w", dataDir, err)
	}

	return nil
}

func (archive DataDirArchive) now() time.Time {
	if archive.Now != nil {
		return archive.Now().UTC()
	}

	return time.Now().UTC()
}

func safeDataDirPath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", ErrDataDirRequired
	}

	absolute, err := filepath.Abs(trimmed)
	if err != nil {
		return "", fmt.Errorf("resolve postgres data directory %q: %w", trimmed, err)
	}

	cleaned := filepath.Clean(absolute)
	if cleaned == string(filepath.Separator) || filepath.Base(cleaned) == string(filepath.Separator) {
		return "", ErrDataDirUnsafe
	}

	return cleaned, nil
}

func archiveRootPath(dataDir, configured string) (string, error) {
	root := strings.TrimSpace(configured)
	if root == "" {
		root = filepath.Join(filepath.Dir(dataDir), defaultReinitArchiveDirName)
	}

	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve postgres data directory archive root %q: %w", root, err)
	}

	cleaned := filepath.Clean(absolute)
	if cleaned == dataDir || strings.HasPrefix(cleaned, dataDir+string(filepath.Separator)) {
		return "", ErrDataDirUnsafe
	}

	return cleaned, nil
}

func archiveName(dataDir, configured string, now time.Time) string {
	label := sanitizeArchiveName(configured)
	if label == "" {
		label = now.UTC().Format("20060102T150405.000000000Z0700")
	}

	return filepath.Base(dataDir) + "-" + label
}

func postgresSafeDataDirMode(mode os.FileMode) os.FileMode {
	switch mode.Perm() {
	case 0o700, 0o750:
		return mode.Perm()
	default:
		return postgresDataDirMode
	}
}

func sanitizeArchiveName(name string) string {
	var builder strings.Builder
	for _, r := range strings.TrimSpace(name) {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.',
			r == '_',
			r == '-':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}

	return strings.Trim(builder.String(), "._-")
}

func directoryEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("read postgres data directory %q: %w", path, err)
	}

	return len(entries) == 0, nil
}
