package buildinfo

import (
	"bytes"
	"testing"
)

func TestPrintWritesBuildBanner(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer

	Print(&output, Info{
		Version: "1.2.3",
		Date:    "2026-04-05T12:00:00Z",
		Commit:  "abc123",
	})

	if got, want := output.String(), "version=1.2.3 commit=abc123 date=2026-04-05T12:00:00Z\n"; got != want {
		t.Fatalf("unexpected build banner: got %q want %q", got, want)
	}
}

func TestPrintIgnoresNilWriter(t *testing.T) {
	t.Parallel()

	Print(nil, Info{
		Version: "1.2.3",
		Date:    "2026-04-05T12:00:00Z",
		Commit:  "abc123",
	})
}
