package fxrun

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/fx"
)

const lifecycleTimeout = 15 * time.Second

// Run starts an Fx application, waits for completion or cancellation, and then
// stops it with a bounded shutdown timeout.
func Run(ctx context.Context, app *fx.App) error {
	startContext, cancelStart := context.WithTimeout(ctx, lifecycleTimeout)
	defer cancelStart()

	if err := app.Start(startContext); err != nil {
		return err
	}

	waitCh := app.Wait()
	var signal fx.ShutdownSignal
	waited := false

	select {
	case signal = <-waitCh:
		waited = true
	case <-ctx.Done():
	}

	stopContext, cancelStop := context.WithTimeout(context.Background(), lifecycleTimeout)
	defer cancelStop()

	if err := app.Stop(stopContext); err != nil {
		return err
	}

	if waited && signal.ExitCode != 0 {
		return fmt.Errorf("application exited with code %d", signal.ExitCode)
	}

	return nil
}
