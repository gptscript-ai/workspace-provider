package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type removeAllWithPrefix struct {
	root *workspaceProvider
}

func (r *removeAllWithPrefix) Customize(c *cobra.Command) {
	c.Args = cobra.MinimumNArgs(2)
	c.Use = "rm-with-prefix [OPTIONS] ID PREFIX..."
	c.Short = "Remove all files with a given prefix"
}

func (r *removeAllWithPrefix) Run(cmd *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		if err := r.root.client.RemoveAllWithPrefix(cmd.Context(), workspaceID, arg); err != nil {
			return err
		}

		fmt.Printf("files with prefix %s deleted from workspace %s\n", arg, args[0])
	}

	return nil
}
