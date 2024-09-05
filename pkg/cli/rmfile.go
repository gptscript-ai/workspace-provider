package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/spf13/cobra"
)

type rmFile struct {
	root *workspaceProvider
}

func newRmFile(root *workspaceProvider) *cobra.Command {
	c := cmd.Command(&rmFile{root: root})
	c.Args = cobra.MinimumNArgs(2)
	c.Use = "rm-file ID FILE..."
	c.Short = "Remove files from a workspace"

	return c
}

func (r *rmFile) Run(_ *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		if err := r.root.client.DeleteFile(workspaceID, arg); err != nil {
			return err
		}

		fmt.Printf("file %s deleted from workspace %s\n", arg, workspaceID)
	}

	return nil
}
