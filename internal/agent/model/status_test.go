package model

import "testing"

func TestNodeStatusCloneCopiesTags(t *testing.T) {
	t.Parallel()

	original := NodeStatus{
		NodeName: "alpha-1",
		Tags: map[string]any{
			"region": "dc-1",
			"lag":    12,
		},
	}

	cloned := original.Clone()
	cloned.Tags["region"] = "dc-2"
	cloned.Tags["new"] = true

	if got, want := original.Tags["region"], "dc-1"; got != want {
		t.Fatalf("unexpected original tag value: got %v want %v", got, want)
	}

	if _, ok := original.Tags["new"]; ok {
		t.Fatal("expected original tags to remain detached from clone")
	}
}

func TestNodeStatusCloneHandlesNilTags(t *testing.T) {
	t.Parallel()

	cloned := (NodeStatus{}).Clone()
	if cloned.Tags != nil {
		t.Fatalf("expected nil tags in clone, got %#v", cloned.Tags)
	}
}
