package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

const version = "dev"

func Execute() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}

func Run(args []string, stdout, stderr io.Writer) int {
	root := newRootCommand(stdout, stderr)
	if len(args) == 0 {
		if err := root.Help(); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		return 0
	}

	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	return 0
}

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "jepsenctl <command> [args]",
		Short:         "PACMAN Jepsen automation helpers",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.SetHelpCommand(&cobra.Command{
		Use:    "help [command]",
		Short:  "Help about any command",
		Hidden: true,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Root().Help()
		},
	})
	root.SetHelpTemplate(`usage: {{.UseLine}}

Commands:
{{- range .Commands}}
{{- if not .Hidden}}
  {{rpad .Name 18}}{{.Short}}
{{- end}}
{{- end}}

Run with go run ./tools/jepsenctl <command> [args].
`)

	root.AddCommand(newArtifactsCommand())
	root.AddCommand(newCasesCommand(stdout, stderr))
	root.AddCommand(newCheckersCommand())
	root.AddCommand(newClusterCommand(stdout))
	root.AddCommand(newVersionCommand(stdout))

	return root
}
