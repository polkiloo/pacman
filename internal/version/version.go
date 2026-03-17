package version

import "fmt"

// Build metadata is injected by the build system.
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

// String renders build metadata for human-readable output.
func String() string {
	return fmt.Sprintf("%s (commit=%s, date=%s)", Version, Commit, BuildDate)
}
