package postgres

import "errors"

var (
	ErrDataDirRequired         = errors.New("postgres data directory is required")
	ErrDataDirUnsafe           = errors.New("postgres data directory path is unsafe")
	ErrDataDirArchiveConflict  = errors.New("postgres data directory archive already exists")
	ErrPrimaryConnInfoRequired = errors.New("postgres primary connection info is required")
	ErrShutdownModeInvalid     = errors.New("postgres shutdown mode is invalid")
	ErrSourceServerRequired    = errors.New("postgres rewind source server is required")
)
