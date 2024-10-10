package cli

import (
	"fmt"

	"github.com/otto8-ai/workspace-provider/pkg/client"
	"github.com/spf13/cobra"
)

type rmDir struct {
	root *workspaceProvider

	client.RmDirOptions
}

func (r *rmDir) Customize(c *cobra.Command) {
	c.Args = cobra.MinimumNArgs(2)
	c.Use = "rm-dir [OPTIONS] ID DIR..."
	c.Short = "Remove a directory from a workspace"
}

func (r *rmDir) Run(cmd *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		if err := r.root.client.RmDir(cmd.Context(), workspaceID, arg, r.RmDirOptions); err != nil {
			return err
		}

		fmt.Printf("directory %s deleted from workspace %s\n", arg, args[0])
	}

	return nil
}
