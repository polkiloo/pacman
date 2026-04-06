package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"

	pacmanctlcmd "github.com/polkiloo/pacman/internal/app/pacmanctl"
	"github.com/polkiloo/pacman/internal/fxrun"
)

const processName = "pacmanctl"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() context.Context { return ctx }),
		pacmanctlcmd.Module(processName, os.Args[1:], os.Stdout, os.Stderr),
	)

	if err := fxrun.Run(ctx, app); err != nil {
		log.Printf("command stopped with error: %v", err)
		os.Exit(1)
	}
}
