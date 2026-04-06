package buildinfo

import (
	"fmt"
	"io"
)

// Info captures the build metadata emitted by process entrypoints.
type Info struct {
	Version string
	Date    string
	Commit  string
}

// Print writes a single-line build metadata banner.
func Print(writer io.Writer, info Info) {
	if writer == nil {
		return
	}

	_, _ = fmt.Fprintf(writer, "version=%s commit=%s date=%s\n", info.Version, info.Commit, info.Date)
}
