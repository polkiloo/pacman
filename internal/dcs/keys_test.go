package dcs

import (
	"errors"
	"testing"
)

func TestNewKeySpaceRequiresClusterName(t *testing.T) {
	t.Parallel()

	_, err := NewKeySpace(" ")
	if !errors.Is(err, ErrClusterNameRequired) {
		t.Fatalf("unexpected error: got %v, want %v", err, ErrClusterNameRequired)
	}
}

func TestKeySpacePaths(t *testing.T) {
	t.Parallel()

	space, err := NewKeySpace(" alpha ")
	if err != nil {
		t.Fatalf("new key space: %v", err)
	}

	testCases := map[string]string{
		"root":          "/pacman/alpha",
		"config":        "/pacman/alpha/config",
		"leader":        "/pacman/alpha/leader",
		"membersPrefix": "/pacman/alpha/members/",
		"member":        "/pacman/alpha/members/alpha-1",
		"statusPrefix":  "/pacman/alpha/status/",
		"status":        "/pacman/alpha/status/alpha-1",
		"operation":     "/pacman/alpha/operation",
		"historyPrefix": "/pacman/alpha/history/",
		"history":       "/pacman/alpha/history/op-1",
		"maintenance":   "/pacman/alpha/maintenance",
		"epoch":         "/pacman/alpha/epoch",
	}

	got := map[string]string{
		"root":          space.Root(),
		"config":        space.Config(),
		"leader":        space.Leader(),
		"membersPrefix": space.MembersPrefix(),
		"member":        space.Member(" alpha-1 "),
		"statusPrefix":  space.StatusPrefix(),
		"status":        space.Status(" alpha-1 "),
		"operation":     space.Operation(),
		"historyPrefix": space.HistoryPrefix(),
		"history":       space.History(" op-1 "),
		"maintenance":   space.Maintenance(),
		"epoch":         space.Epoch(),
	}

	for name, want := range testCases {
		if got[name] != want {
			t.Fatalf("%s path mismatch: got %q, want %q", name, got[name], want)
		}
	}
}
