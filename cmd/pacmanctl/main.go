package main

import (
	"context"
	"log/slog"
	"os"

	"go.uber.org/dig"

	pacmanctlcmd "github.com/polkiloo/pacman/internal/app/pacmanctl"
	"github.com/polkiloo/pacman/internal/di"
	"github.com/polkiloo/pacman/internal/logging"
)

const processName = "pacmanctl"

type invokeParams struct {
	dig.In

	App  *pacmanctlcmd.App
	Args []string `name:"args"`
}

func main() {
	os.Exit(run())
}

func run() int {
	logger := logging.New(processName, os.Stderr)
	container := dig.New()

	if err := di.ProvideBase(container, processName, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		logger.Error("failed to bootstrap DI container", slog.Any("error", err))
		return 1
	}

	if err := container.Provide(pacmanctlcmd.New); err != nil {
		logger.Error("failed to register app dependencies", slog.Any("error", err))
		return 1
	}

	var runErr error

	if err := container.Invoke(func(params invokeParams) {
		runErr = params.App.Run(context.Background(), params.Args)
	}); err != nil {
		logger.Error("failed to invoke app", slog.Any("error", err))
		return 1
	}

	if runErr != nil {
		logger.Error("app run failed", slog.Any("error", runErr))
		return 1
	}

	return 0
}
