package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunListsRepoPackagesAndSkipsGeneratedCaches(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module example.com/project\n\ngo 1.26.1\n")
	writeFile(t, root, "main.go", "package project\n")
	writeFile(t, root, "internal/app/app.go", "package app\n")
	writeFile(t, root, "test/integration/integration.go", "package integration\n")
	writeFile(t, root, "bin/rpm-gomodcache/github.com/boltdb/bolt@v1.3.1/bolt.go", "package bolt\n")
	writeFile(t, root, "jepsen/store/pacman/nightly/generated.go", "package generated\n")
	writeFile(t, root, "deploy/lab/.local/state/generated.go", "package state\n")

	var out bytes.Buffer
	if err := run(&out, root, "import"); err != nil {
		t.Fatalf("run: %v", err)
	}

	got := strings.Fields(out.String())
	want := []string{
		"example.com/project",
		"example.com/project/internal/app",
		"example.com/project/test/integration",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("package list mismatch\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRunListsPackagePatterns(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module example.com/project\n\ngo 1.26.1\n")
	writeFile(t, root, "main.go", "package project\n")
	writeFile(t, root, "internal/app/app.go", "package app\n")

	var out bytes.Buffer
	if err := run(&out, root, "pattern"); err != nil {
		t.Fatalf("run: %v", err)
	}

	got := strings.Fields(out.String())
	want := []string{".", "./internal/app"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("package list mismatch\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRunRejectsUnknownFormat(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out, t.TempDir(), "unknown"); err == nil {
		t.Fatal("expected unsupported format error")
	}
}

func TestModulePathRequiresModuleDirective(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "go.mod")
	if err := os.WriteFile(path, []byte("go 1.26.1\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	if _, err := modulePath(path); err == nil {
		t.Fatal("expected missing module directive error")
	}
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()

	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}
