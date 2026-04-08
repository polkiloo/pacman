package pgext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtensionExampleContainsAllDocumentedSettings(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "docs", "examples", "pacman-agent.postgresql.conf")
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read extension example: %v", err)
	}

	document := string(payload)
	requiredSettings := []string{
		"shared_preload_libraries",
		GUCNodeName,
		GUCNodeRole,
		GUCAPIAddress,
		GUCControlAddress,
		GUCHelperPath,
		GUCPostgresDataDir,
		GUCPostgresBinDir,
		GUCPostgresListenAddress,
		GUCPostgresPort,
		GUCClusterName,
		GUCInitialPrimary,
		GUCSeedAddresses,
		GUCExpectedMembers,
	}

	for _, setting := range requiredSettings {
		if !strings.Contains(document, setting) {
			t.Fatalf("expected extension example to contain %q", setting)
		}
	}
}
