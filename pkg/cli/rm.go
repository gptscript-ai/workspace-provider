package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/spf13/cobra"
)

type rm struct {
	root *workspaceProvider
}

func newRm(root *workspaceProvider) *cobra.Command {
	c := cmd.Command(&rm{root: root})
	c.Args = cobra.MinimumNArgs(1)
	c.Use = "rm ID..."
	c.Short = "Remove a workspace"

	return c
}

func (r *rm) Run(_ *cobra.Command, args []string) error {
	for _, arg := range args {
		if err := r.root.client.Rm(arg); err != nil {
			return err
		}

		fmt.Printf("workspace %s deleted\n", arg)
	}

	return nil
}
