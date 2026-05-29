package main

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	if err := run(os.Stdout, "."); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(w io.Writer, root string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}

	module, err := modulePath(filepath.Join(rootAbs, "go.mod"))
	if err != nil {
		return err
	}

	dirs, err := packageDirs(rootAbs)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		rel, err := filepath.Rel(rootAbs, dir)
		if err != nil {
			return err
		}

		importPath := module
		if rel != "." {
			importPath += "/" + filepath.ToSlash(rel)
		}
		if _, err := fmt.Fprintln(w, importPath); err != nil {
			return err
		}
	}

	return nil
}

func modulePath(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "module" {
			return fields[1], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", fmt.Errorf("module directive not found in %s", path)
}

func packageDirs(root string) ([]string, error) {
	packages := make(map[string]struct{})
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && shouldPrune(root, path) {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}

		packages[filepath.Dir(path)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}

	dirs := make([]string, 0, len(packages))
	for dir := range packages {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	return dirs, nil
}

func shouldPrune(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." {
		return false
	}

	rel = filepath.ToSlash(rel)
	switch rel {
	case ".git", ".github", ".gocache", "bin", "vendor":
		return true
	case "deploy/lab/.local", "jepsen/store":
		return true
	default:
		return false
	}
}
