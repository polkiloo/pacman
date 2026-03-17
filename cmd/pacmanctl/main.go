package main

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/dig"

	pacmanctlcmd "github.com/polkiloo/pacman/internal/app/pacmanctl"
	"github.com/polkiloo/pacman/internal/di"
)

type invokeParams struct {
	dig.In

	App  *pacmanctlcmd.App
	Args []string `name:"args"`
}

func main() {
	os.Exit(run())
}

func run() int {
	container := dig.New()

	if err := di.ProvideBase(container, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "pacmanctl: bootstrap DI container: %v\n", err)
		return 1
	}

	if err := container.Provide(pacmanctlcmd.New); err != nil {
		fmt.Fprintf(os.Stderr, "pacmanctl: register app dependencies: %v\n", err)
		return 1
	}

	var runErr error

	if err := container.Invoke(func(params invokeParams) {
		runErr = params.App.Run(context.Background(), params.Args)
	}); err != nil {
		fmt.Fprintf(os.Stderr, "pacmanctl: invoke app: %v\n", err)
		return 1
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "pacmanctl: %v\n", runErr)
		return 1
	}

	return 0
}
