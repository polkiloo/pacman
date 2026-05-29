package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchPair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		goArch   string
		nfpmArch string
		rpmArch  string
		wantErr  bool
	}{
		{goArch: "amd64", nfpmArch: "amd64", rpmArch: "x86_64"},
		{goArch: "arm64", nfpmArch: "arm64", rpmArch: "aarch64"},
		{goArch: "386", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.goArch, func(t *testing.T) {
			t.Parallel()

			nfpmArch, rpmArch, err := archPair(test.goArch)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("arch pair: %v", err)
			}
			if nfpmArch != test.nfpmArch || rpmArch != test.rpmArch {
				t.Fatalf("arch pair: got %s/%s want %s/%s", nfpmArch, rpmArch, test.nfpmArch, test.rpmArch)
			}
		})
	}
}

func TestCopyWorkspaceSkipsBuildArtifacts(t *testing.T) {
	t.Parallel()

	src := t.TempDir()
	dst := t.TempDir()
	writeTestFile(t, filepath.Join(src, "go.mod"), "module example.test/pacman\n")
	writeTestFile(t, filepath.Join(src, "cmd", "pacmand", "main.go"), "package main\n")
	writeTestFile(t, filepath.Join(src, "bin", "old.rpm"), "skip\n")
	writeTestFile(t, filepath.Join(src, "jepsen", "store", "history.edn"), "skip\n")
	writeTestFile(t, filepath.Join(src, ".gocache", "cache"), "skip\n")

	cfg := config{
		workspaceSkipExact: map[string]struct{}{
			".gocache":     {},
			"bin":          {},
			"jepsen/store": {},
		},
		workspaceSkipPrefix: []string{
			".gocache/",
			"bin/",
			"jepsen/store/",
		},
	}

	if err := copyWorkspace(src, dst, cfg); err != nil {
		t.Fatalf("copy workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "cmd", "pacmand", "main.go")); err != nil {
		t.Fatalf("copied source missing: %v", err)
	}
	for _, path := range []string{
		filepath.Join(dst, "bin"),
		filepath.Join(dst, "jepsen", "store"),
		filepath.Join(dst, ".gocache"),
	} {
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("path should have been skipped: %s", path)
		}
	}
}

func TestLDFlags(t *testing.T) {
	t.Parallel()

	got := ldflags("1.2.3", "abc123", "2026-05-29T00:00:00Z")
	for _, want := range []string{
		"Version=1.2.3",
		"Commit=abc123",
		"BuildDate=2026-05-29T00:00:00Z",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("ldflags %q missing %q", got, want)
		}
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}
