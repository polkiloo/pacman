package postgres

import "errors"

var (
	ErrDataDirRequired         = errors.New("postgres data directory is required")
	ErrPrimaryConnInfoRequired = errors.New("postgres primary connection info is required")
	ErrShutdownModeInvalid     = errors.New("postgres shutdown mode is invalid")
	ErrSourceServerRequired    = errors.New("postgres rewind source server is required")
)
