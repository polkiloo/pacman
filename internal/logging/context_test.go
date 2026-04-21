package logging

import (
	"context"
	"testing"
)

func TestAttrsFromContextReturnsStableSortedCorrelationFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = WithCluster(ctx, "alpha")
	ctx = WithPrincipalSubject(ctx, "ops@example")
	ctx = WithOperation(ctx, "op-1", "switchover")
	ctx = WithNode(ctx, "alpha-1")
	ctx = WithRequestID(ctx, "req-123")
	ctx = WithMember(ctx, "alpha-2")

	attrs := AttrsFromContext(ctx)
	if len(attrs) != 7 {
		t.Fatalf("expected 7 correlation attrs, got %d", len(attrs))
	}

	gotKeys := make([]string, len(attrs))
	gotValues := make(map[string]string, len(attrs))
	for index, attr := range attrs {
		gotKeys[index] = attr.Key
		gotValues[attr.Key] = attr.Value.String()
	}

	wantKeys := []string{
		"cluster",
		"member",
		"node",
		"operation_id",
		"operation_kind",
		"principal_subject",
		"request_id",
	}
	for index := range wantKeys {
		if gotKeys[index] != wantKeys[index] {
			t.Fatalf("attr key %d: got %q, want %q", index, gotKeys[index], wantKeys[index])
		}
	}

	for key, want := range map[string]string{
		"cluster":           "alpha",
		"member":            "alpha-2",
		"node":              "alpha-1",
		"operation_id":      "op-1",
		"operation_kind":    "switchover",
		"principal_subject": "ops@example",
		"request_id":        "req-123",
	} {
		if got := gotValues[key]; got != want {
			t.Fatalf("attr value %q: got %q, want %q", key, got, want)
		}
	}
}

func TestWithCorrelationFieldIgnoresBlankValues(t *testing.T) {
	t.Parallel()

	ctx := WithRequestID(context.Background(), "  ")
	ctx = WithPrincipalMechanism(ctx, "")

	if attrs := AttrsFromContext(ctx); len(attrs) != 0 {
		t.Fatalf("expected no attrs for blank correlation values, got %+v", attrs)
	}
}
