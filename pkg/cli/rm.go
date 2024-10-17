package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type rm struct {
	root *workspaceProvider
}

func (r *rm) Customize(c *cobra.Command) {
	c.Args = cobra.MinimumNArgs(1)
	c.Use = "rm [OPTIONS] ID..."
	c.Short = "Remove a workspace"
}

func (r *rm) Run(cmd *cobra.Command, args []string) error {
	if len(args) == 1 {
		if args[0] == "" {
			return fmt.Errorf("at least one argument required")
		} else {
			args = strings.Split(args[0], ",")
		}
	}
	for _, arg := range args {
		fmt.Println(arg)
		if err := r.root.client.Rm(cmd.Context(), arg); err != nil {
			return err
		}

		fmt.Printf("workspace %s deleted\n", arg)
	}

	return nil
}
