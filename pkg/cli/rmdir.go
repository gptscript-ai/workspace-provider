package cli

import (
	"errors"
	"fmt"

	"github.com/otto8-ai/workspace-provider/pkg/client"
	"github.com/spf13/cobra"
)

type rmDir struct {
	root *workspaceProvider

	client.RmDirOptions

	IgnoreNotFound bool `usage:"Ignore not found errors"`
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
			var notFound *client.DirectoryNotFoundError
			if r.IgnoreNotFound && errors.As(err, &notFound) {
				fmt.Printf("directory %s not found in workspace %s\n", arg, workspaceID)
				continue
			}
			return err
		}

		fmt.Printf("directory %s deleted from workspace %s\n", arg, args[0])
	}

	return nil
}
