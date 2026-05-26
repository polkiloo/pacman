package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseListCasesOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantCases []jepsenCase
		wantErr   string
	}{
		{
			name: "parses slug spec and description",
			input: `
# comment
append-smoke-none append-smoke:none Smoke append workload without nemesis.
append-failover-packet-kill append-failover:packet,kill Append workload under partition plus kill.
`,
			wantCases: []jepsenCase{
				{
					Slug:        "append-smoke-none",
					Spec:        "append-smoke:none",
					Description: "Smoke append workload without nemesis.",
				},
				{
					Slug:        "append-failover-packet-kill",
					Spec:        "append-failover:packet,kill",
					Description: "Append workload under partition plus kill.",
				},
			},
		},
		{
			name:    "rejects malformed line",
			input:   "append-smoke-none\n",
			wantErr: "expected slug and workload:nemesis",
		},
		{
			name:    "rejects spec without nemesis separator",
			input:   "append-smoke-none append-smoke Smoke append workload.\n",
			wantErr: `spec "append-smoke" must use workload:nemesis format`,
		},
		{
			name: "rejects duplicate slug",
			input: `
append-smoke-none append-smoke:none Smoke append workload.
append-smoke-none append-smoke:none Duplicate.
`,
			wantErr: `duplicate case slug "append-smoke-none"`,
		},
		{
			name:    "rejects empty registry",
			input:   "# no cases\n",
			wantErr: "did not contain any cases",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cases, err := parseListCasesOutput([]byte(test.input))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error: got %v want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse cases: %v", err)
			}
			if len(cases) != len(test.wantCases) {
				t.Fatalf("case count: got %d want %d", len(cases), len(test.wantCases))
			}
			for i := range cases {
				if cases[i] != test.wantCases[i] {
					t.Fatalf("case[%d]: got %+v want %+v", i, cases[i], test.wantCases[i])
				}
			}
		})
	}
}

func TestValidateCaseTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cases       []jepsenCase
		targets     map[string]struct{}
		wantMissing []string
	}{
		{
			name: "accepts case and docker targets",
			cases: []jepsenCase{
				{Slug: "append-smoke-none", Spec: "append-smoke:none"},
			},
			targets: map[string]struct{}{
				"jepsen-case-append-smoke-none":        {},
				"jepsen-docker-case-append-smoke-none": {},
			},
		},
		{
			name: "reports missing targets",
			cases: []jepsenCase{
				{Slug: "append-smoke-none", Spec: "append-smoke:none"},
				{Slug: "append-failover-kill", Spec: "append-failover:kill"},
			},
			targets: map[string]struct{}{
				"jepsen-case-append-smoke-none": {},
			},
			wantMissing: []string{
				"missing Make target for Jepsen case append-smoke-none: jepsen-docker-case-append-smoke-none",
				"missing Make target for Jepsen case append-failover-kill: jepsen-case-append-failover-kill",
				"missing Make target for Jepsen case append-failover-kill: jepsen-docker-case-append-failover-kill",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result := validateCaseTargets(test.cases, test.targets)
			if result.CaseCount != len(test.cases) {
				t.Fatalf("case count: got %d want %d", result.CaseCount, len(test.cases))
			}
			if result.TargetCount != len(test.cases)*2 {
				t.Fatalf("target count: got %d want %d", result.TargetCount, len(test.cases)*2)
			}
			assertStringSlicesEqual(t, result.MissingLines, test.wantMissing)
		})
	}
}

func TestCasesValidateCommand(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	casesFile := filepath.Join(tempDir, "cases.txt")
	makefile := filepath.Join(tempDir, "jepsen.mk")

	writeTestFile(t, casesFile, `
append-smoke-none append-smoke:none Smoke append workload.
append-failover-kill append-failover:kill Append workload.
`)
	writeTestFile(t, makefile, `
jepsen-case-append-smoke-none:
	@true
jepsen-docker-case-append-smoke-none:
	@true
jepsen-case-append-failover-kill:
	@true
jepsen-docker-case-append-failover-kill:
	@true
`)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{"cases", "validate", "--cases-file", casesFile, "--makefile", makefile}, &stdout, &stderr)

	if status != 0 {
		t.Fatalf("status: got %d want 0; stderr:\n%s", status, stderr.String())
	}
	assertContainsAll(t, "stdout", stdout.String(), []string{
		"validated 2 Jepsen cases and 4 required Make targets",
	})
}

func TestCasesValidateCommandReportsMissingTargets(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	casesFile := filepath.Join(tempDir, "cases.txt")
	makefile := filepath.Join(tempDir, "jepsen.mk")

	writeTestFile(t, casesFile, "append-smoke-none append-smoke:none Smoke append workload.\n")
	writeTestFile(t, makefile, "jepsen-case-append-smoke-none:\n\t@true\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{"cases", "validate", "--cases-file", casesFile, "--makefile", makefile}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	assertContainsAll(t, "stderr", stderr.String(), []string{
		"missing Make target for Jepsen case append-smoke-none: jepsen-docker-case-append-smoke-none",
		"missing 1 Jepsen case Make target(s)",
	})
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent directory for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(content, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("slice length: got %d want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slice[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}
