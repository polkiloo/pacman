package version

import "testing"

func TestString(t *testing.T) {
	t.Parallel()

	if got, want := String(), Version+" (commit="+Commit+", date="+BuildDate+")"; got != want {
		t.Fatalf("unexpected version string: got %q, want %q", got, want)
	}
}
