package main

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/dig"

	pacmandcmd "github.com/polkiloo/pacman/internal/app/pacmand"
	"github.com/polkiloo/pacman/internal/di"
)

type invokeParams struct {
	dig.In

	App  *pacmandcmd.App
	Args []string `name:"args"`
}

func main() {
	os.Exit(run())
}

func run() int {
	container := dig.New()

	if err := di.ProvideBase(container, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "pacmand: bootstrap DI container: %v\n", err)
		return 1
	}

	if err := container.Provide(pacmandcmd.New); err != nil {
		fmt.Fprintf(os.Stderr, "pacmand: register app dependencies: %v\n", err)
		return 1
	}

	var runErr error

	if err := container.Invoke(func(params invokeParams) {
		runErr = params.App.Run(context.Background(), params.Args)
	}); err != nil {
		fmt.Fprintf(os.Stderr, "pacmand: invoke app: %v\n", err)
		return 1
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "pacmand: %v\n", runErr)
		return 1
	}

	return 0
}
