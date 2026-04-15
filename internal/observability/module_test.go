package observability

import (
	"testing"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/agent"
)

type resolvedAgentOptions struct {
	fx.In

	Options []agent.Option `group:"agent.option"`
}

func TestModuleProvidesPrometheusExporterAgentOption(t *testing.T) {
	t.Parallel()

	var resolved resolvedAgentOptions
	app := fx.New(
		fx.NopLogger,
		Module(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	if len(resolved.Options) != 1 {
		t.Fatalf("unexpected agent options: got %d, want 1", len(resolved.Options))
	}

	if resolved.Options[0] == nil {
		t.Fatal("expected non-nil observability agent option")
	}
}
