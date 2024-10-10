package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type create struct {
	root *workspaceProvider
}

func (c *create) Customize(cmd *cobra.Command) {
	cmd.Use = "create [ID...]"
	cmd.Short = "Create a new workspace, optionally from one or more IDs"
}

func (c *create) Run(cmd *cobra.Command, args []string) error {
	if len(args) == 1 {
		if args[0] == "" {
			args = nil
		} else {
			args = strings.Split(args[0], ",")
		}
	}

	workspace, err := c.root.client.Create(cmd.Context(), c.root.Provider, args...)
	if err != nil {
		return err
	}

	fmt.Println(workspace)
	return nil
}
