package cli

import (
	"context"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

type cpFile struct {
	root *workspaceProvider
}

func (c *cpFile) Customize(cmd *cobra.Command) {
	cmd.Args = cobra.MinimumNArgs(2)
	cmd.Use = "cp-file [OPTIONS] ID FILE..."
	cmd.Short = "Copy files to a workspace"
}

func (c *cpFile) Run(cmd *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		if err := c.copyFile(cmd.Context(), workspaceID, arg); err != nil {
			return err
		}
	}

	return nil
}

func (c *cpFile) copyFile(ctx context.Context, workspaceID, src string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	return c.root.client.WriteFile(ctx, workspaceID, filepath.Base(src), source)
}
