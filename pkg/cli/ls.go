package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/spf13/cobra"
)

type ls struct {
	root *workspaceProvider
}

func newLs(root *workspaceProvider) *cobra.Command {
	c := cmd.Command(&ls{root: root})
	c.Args = cobra.MinimumNArgs(1)
	c.Use = "ls ID..."
	c.Short = "List files in a workspace"

	return c
}

func (l *ls) Run(cmd *cobra.Command, args []string) error {
	for _, arg := range args {
		fmt.Println(arg + ":")
		contents, err := l.root.client.Ls(cmd.Context(), arg)
		if err != nil {
			return err
		}

		for _, content := range contents {
			fmt.Println(content)
		}
		fmt.Printf("\n\n")
	}

	return nil
}
