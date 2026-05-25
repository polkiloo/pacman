package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunDispatchesCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []string
		wantStatus int
		wantStdout []string
		wantStderr []string
	}{
		{
			name:       "no args prints usage",
			wantStatus: 0,
			wantStdout: []string{
				"usage: jepsenctl <command> [args]",
				"cases",
				"cluster",
				"version",
				"go run ./tools/jepsenctl",
			},
		},
		{
			name:       "help prints usage",
			args:       []string{"help"},
			wantStatus: 0,
			wantStdout: []string{
				"usage: jepsenctl <command> [args]",
				"cases",
				"cluster",
				"version",
			},
		},
		{
			name:       "version prints version",
			args:       []string{"version"},
			wantStatus: 0,
			wantStdout: []string{
				"jepsenctl dev",
			},
		},
		{
			name:       "unknown command fails",
			args:       []string{"missing"},
			wantStatus: 2,
			wantStderr: []string{
				`unknown command "missing" for "jepsenctl"`,
			},
		},
		{
			name:       "version rejects arguments",
			args:       []string{"version", "extra"},
			wantStatus: 2,
			wantStderr: []string{
				"version does not accept arguments",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			status := Run(test.args, &stdout, &stderr)

			if status != test.wantStatus {
				t.Fatalf("status: got %d want %d", status, test.wantStatus)
			}

			assertContainsAll(t, "stdout", normalizeOutput(stdout.String()), test.wantStdout)
			assertContainsAll(t, "stderr", normalizeOutput(stderr.String()), test.wantStderr)
		})
	}
}

func assertContainsAll(t *testing.T, streamName, output string, want []string) {
	t.Helper()

	for _, fragment := range want {
		if !strings.Contains(output, fragment) {
			t.Fatalf("%s missing %q in:\n%s", streamName, fragment, output)
		}
	}
}

func normalizeOutput(output string) string {
	return strings.ReplaceAll(output, "\r\n", "\n")
}
