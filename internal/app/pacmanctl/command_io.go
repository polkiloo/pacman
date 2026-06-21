package pacmanctl

import (
	"io"

	"go.uber.org/fx"
)

type commandIO struct {
	stdout io.Writer
	stderr io.Writer
}

type commandIOParams struct {
	fx.In

	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
}

func newCommandIO(params commandIOParams) *commandIO {
	return &commandIO{
		stdout: params.Stdout,
		stderr: params.Stderr,
	}
}
