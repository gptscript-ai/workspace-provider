package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/spf13/cobra"
)

type create struct {
	root *workspaceProvider
}

func newCreate(root *workspaceProvider) *cobra.Command {
	c := cmd.Command(&create{root: root})
	c.Short = "Create a new workspace"

	return c
}

func (c *create) Run(_ *cobra.Command, args []string) error {
	workspace, err := c.root.client.Create(c.root.Provider, args...)
	if err != nil {
		return err
	}

	fmt.Println(workspace)

	return nil
}
