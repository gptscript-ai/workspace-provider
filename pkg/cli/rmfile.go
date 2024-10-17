package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type rmFile struct {
	root *workspaceProvider
}

func (r *rmFile) Customize(c *cobra.Command) {
	c.Args = cobra.MinimumNArgs(2)
	c.Use = "rm-file [OPTIONS] ID FILE..."
	c.Short = "Remove files from a workspace"
}

func (r *rmFile) Run(cmd *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		if err := r.root.client.DeleteFile(cmd.Context(), workspaceID, arg); err != nil {
			return err
		}

		fmt.Printf("file %s deleted from workspace %s\n", arg, workspaceID)
	}

	return nil
}
