package pacmand

import (
	"context"
	"flag"
	"fmt"
	"io"

	"go.uber.org/dig"

	"github.com/polkiloo/pacman/internal/version"
)

// App is the pacmand process entrypoint.
type App struct {
	stdout io.Writer
	stderr io.Writer
}

// Params defines pacmand constructor dependencies.
type Params struct {
	dig.In

	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
}

// New constructs a pacmand application.
func New(params Params) *App {
	return &App{
		stdout: params.Stdout,
		stderr: params.Stderr,
	}
}

// Run parses process flags and starts the daemon scaffold.
func (a *App) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pacmand", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	showVersion := fs.Bool("version", false, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if *showVersion {
		_, err := fmt.Fprintln(a.stdout, version.String())
		return err
	}

	_, err := fmt.Fprintln(a.stdout, "pacmand scaffold: local agent entrypoint is not implemented yet")
	return err
}
