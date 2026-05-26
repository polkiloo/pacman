package cmd

import "github.com/spf13/cobra"

func newCheckersCommand() *cobra.Command {
	checkers := &cobra.Command{
		Use:   "checkers",
		Short: "run Jepsen artifact checkers",
	}

	checkers.AddCommand(newAcknowledgedWriteCheckerCommand())
	checkers.AddCommand(newDCSQuorumCheckerCommand())
	checkers.AddCommand(newSinglePrimaryCheckerCommand())

	return checkers
}
