package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/spf13/cobra"
	"github.com/thedadams/workspace-provider/pkg/client"
)

type workspaceProvider struct {
	Provider string `usage:"The workspace provider to use, valid options are 'directory' and 's3'" default:"directory" env:"WORKSPACE_PROVIDER_PROVIDER"`
	DataHome string `usage:"The data home directory or bucket name" env:"XDG_DATA_HOME"`

	client *client.Client
}

func New() *cobra.Command {
	w := new(workspaceProvider)
	c := cmd.Command(w)

	c.AddCommand(
		newCreate(w),
		newLs(w),
		newRm(w),
		newCpFile(w),
		newRmFile(w),
	)
	c.CompletionOptions.HiddenDefaultCmd = true
	return c
}

func (w *workspaceProvider) Run(cmd *cobra.Command, _ []string) error {
	return cmd.Help()
}

func (w *workspaceProvider) PersistentPre(*cobra.Command, []string) error {
	switch w.Provider {
	case "directory":
	case "s3":
		if w.DataHome == "" {
			return fmt.Errorf("s3 provider requires a bucket name")
		}
	default:
		return fmt.Errorf("invalid workspace provider: %s", w.Provider)
	}

	w.client = client.New(client.Options{DirectoryDataHome: w.DataHome})

	return nil
}
