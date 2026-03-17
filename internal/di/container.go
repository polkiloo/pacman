package di

import (
	"io"

	"go.uber.org/dig"
)

// ProvideBase registers process-scoped dependencies shared by command entrypoints.
func ProvideBase(container *dig.Container, args []string, stdout, stderr io.Writer) error {
	if err := container.Provide(func() []string {
		return args
	}, dig.Name("args")); err != nil {
		return err
	}

	if err := container.Provide(func() io.Writer {
		return stdout
	}, dig.Name("stdout")); err != nil {
		return err
	}

	if err := container.Provide(func() io.Writer {
		return stderr
	}, dig.Name("stderr")); err != nil {
		return err
	}

	return nil
}
