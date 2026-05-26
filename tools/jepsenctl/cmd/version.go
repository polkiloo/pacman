package cmd

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

func newVersionCommand(stdout io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "print jepsenctl version",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("version does not accept arguments")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(stdout, "jepsenctl %s\n", version)
		},
	}
}
