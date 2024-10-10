package cli

import (
	"fmt"

	"github.com/otto8-ai/workspace-provider/pkg/client"
	"github.com/spf13/cobra"
)

type mkdir struct {
	root *workspaceProvider

	client.MkDirOptions
}

func (m *mkdir) Customize(cmd *cobra.Command) {
	cmd.Args = cobra.MinimumNArgs(2)
	cmd.Use = "mk-dir [OPTIONS] ID DIR..."
	cmd.Short = "Create a new directory in a workspace"
}

func (m *mkdir) Run(cmd *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		fmt.Println(arg)
		if err := m.root.client.MkDir(cmd.Context(), workspaceID, arg, m.MkDirOptions); err != nil {
			return err
		}
	}

	return nil
}
