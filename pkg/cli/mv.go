package cli

import (
	"io"
	"os"
	"path/filepath"

	"github.com/gptscript-ai/cmd"
	"github.com/spf13/cobra"
)

type cpFile struct {
	root *workspaceProvider
}

func newCpFile(root *workspaceProvider) *cobra.Command {
	c := cmd.Command(&cpFile{root: root})
	c.Args = cobra.MinimumNArgs(2)
	c.Use = "cp-file ID FILE..."
	c.Short = "Copy a file into a workspace"

	return c
}

func (c *cpFile) Run(_ *cobra.Command, args []string) error {
	workspaceID := args[0]
	for _, arg := range args[1:] {
		if err := c.copyFile(workspaceID, arg); err != nil {
			return err
		}
	}

	return nil
}

func (c *cpFile) copyFile(workspaceID, src string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	file, err := c.root.client.WriteFile(workspaceID, filepath.Base(src))
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, source)
	return err
}
