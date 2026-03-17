package pacmanctl

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"go.uber.org/dig"

	"github.com/polkiloo/pacman/internal/version"
)

// App is the pacmanctl process entrypoint.
type App struct {
	stdout io.Writer
	stderr io.Writer
}

// Params defines pacmanctl constructor dependencies.
type Params struct {
	dig.In

	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
}

// New constructs a pacmanctl application.
func New(params Params) *App {
	return &App{
		stdout: params.Stdout,
		stderr: params.Stderr,
	}
}

// Run parses process flags and starts the CLI scaffold.
func (a *App) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pacmanctl", flag.ContinueOnError)
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

	remaining := fs.Args()
	if len(remaining) == 0 {
		_, err := fmt.Fprintln(a.stdout, "pacmanctl scaffold: CLI commands are not implemented yet")
		return err
	}

	_, err := fmt.Fprintf(
		a.stdout,
		"pacmanctl scaffold: command support is not implemented yet (%s)\n",
		strings.Join(remaining, " "),
	)
	return err
}
