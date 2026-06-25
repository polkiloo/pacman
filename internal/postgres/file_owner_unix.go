//go:build unix

package postgres

import (
	"os"
	"syscall"
)

type fileOwnership struct {
	uid   int
	gid   int
	valid bool
}

func fileOwner(info os.FileInfo) fileOwnership {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileOwnership{}
	}

	return fileOwnership{
		uid:   int(stat.Uid),
		gid:   int(stat.Gid),
		valid: true,
	}
}

func chownIfRoot(path string, owner fileOwnership) error {
	if !owner.valid || os.Geteuid() != 0 {
		return nil
	}

	return os.Chown(path, owner.uid, owner.gid)
}
