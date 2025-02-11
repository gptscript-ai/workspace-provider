package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type validateEnv struct {
	root *workspaceProvider
}

func (r *validateEnv) Customize(c *cobra.Command) {
	c.Args = cobra.NoArgs
	c.Use = "validate-env"
	c.Short = "Validate environment variables"
}

func (r *validateEnv) Run(cmd *cobra.Command, args []string) error {
	id, err := r.root.client.Create(cmd.Context(), r.root.Provider)
	if err != nil {
		return err
	}

	if err := r.root.client.Rm(cmd.Context(), id); err != nil {
		return err
	}

	fmt.Println("Environment validated")
	return nil
}
