package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/otto8-ai/workspace-provider/pkg/client"
	"github.com/otto8-ai/workspace-provider/pkg/version"
	"github.com/spf13/cobra"
)

type workspaceProvider struct {
	Provider string `usage:"The workspace provider to use, valid options are 'directory' and 's3'" default:"directory" env:"WORKSPACE_PROVIDER_PROVIDER"`
	DataHome string `usage:"The data home directory or bucket name" env:"XDG_DATA_HOME"`

	client *client.Client
}

func New() *cobra.Command {
	w := new(workspaceProvider)
	c := cmd.Command(w,
		&create{root: w},
		&rm{root: w},
		&ls{root: w},
		&mkdir{root: w},
		&rmDir{root: w},
		&cpFile{root: w},
		&writeFile{root: w},
		&rmFile{root: w},
		&readFile{root: w},
	)

	c.CompletionOptions.HiddenDefaultCmd = true
	return c
}

func (w *workspaceProvider) Run(cmd *cobra.Command, _ []string) error {
	return cmd.Help()
}

func (w *workspaceProvider) Customize(cmd *cobra.Command) {
	cmd.Version = version.Get().String()
	cmd.CompletionOptions.HiddenDefaultCmd = true
	cmd.TraverseChildren = true
}

func (w *workspaceProvider) PersistentPre(*cobra.Command, []string) error {
	switch w.Provider {
	case client.DirectoryProvider:
	case client.S3Provider:
		if w.DataHome == "" {
			return fmt.Errorf("s3 provider requires a bucket name")
		}
	default:
		return fmt.Errorf("invalid workspace provider: %s", w.Provider)
	}

	w.client = client.New(client.Options{DirectoryDataHome: w.DataHome})

	return nil
}
