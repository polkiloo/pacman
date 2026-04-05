package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"

	pacmanctlcmd "github.com/polkiloo/pacman/internal/app/pacmanctl"
	"github.com/polkiloo/pacman/internal/buildinfo"
	"github.com/polkiloo/pacman/internal/fxrun"
	"github.com/polkiloo/pacman/internal/version"
)

const processName = "pacmanctl"

func main() {
	buildinfo.Print(os.Stdout, buildinfo.Info{
		Version: version.Version,
		Date:    version.BuildDate,
		Commit:  version.Commit,
	})

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
