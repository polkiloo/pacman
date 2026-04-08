package config

import (
	"path/filepath"
	"testing"
)

func TestExampleConfigsLoad(t *testing.T) {
	t.Parallel()

	examples := []string{
		"pacmand-raft-data.yaml",
		"pacmand-raft-witness.yaml",
		"pacmand-etcd-data.yaml",
	}

	for _, example := range examples {
		example := example
		t.Run(example, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join("..", "..", "docs", "examples", example)
			if _, err := Load(path); err != nil {
				t.Fatalf("load example config %q: %v", example, err)
			}
		})
	}
}
