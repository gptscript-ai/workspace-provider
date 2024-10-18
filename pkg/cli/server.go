package cli

import (
	"github.com/spf13/cobra"

	wserver "github.com/gptscript-ai/workspace-provider/pkg/server"
)

type server struct {
	root *workspaceProvider
	Port int `usage:"Port to run the server on" default:"8888" env:"PORT"`
}

func (s *server) Customize(cmd *cobra.Command) {
	cmd.Use = "server [OPTIONS]"
	cmd.Hidden = true
}

func (s *server) Run(cmd *cobra.Command, _ []string) error {
	return wserver.Run(cmd.Context(), s.root.client, s.Port)
}
