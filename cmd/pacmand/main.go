package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"

	pacmandcmd "github.com/polkiloo/pacman/internal/app/pacmand"
	"github.com/polkiloo/pacman/internal/buildinfo"
	"github.com/polkiloo/pacman/internal/fxrun"
	"github.com/polkiloo/pacman/internal/version"
)

const processName = "pacmand"

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
		pacmandcmd.Module(processName, os.Args[1:], os.Stdout, os.Stderr),
	)

	if err := fxrun.Run(ctx, app); err != nil {
		log.Printf("server stopped with error: %v", err)
		os.Exit(1)
	}
}
