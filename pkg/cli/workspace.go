package cli

import (
	"fmt"

	"github.com/gptscript-ai/cmd"
	"github.com/gptscript-ai/workspace-provider/pkg/client"
	"github.com/gptscript-ai/workspace-provider/pkg/version"
	"github.com/spf13/cobra"
)

type workspaceProvider struct {
	Provider       string `usage:"The workspace provider to use, valid options are 'directory' and 's3'" default:"directory" env:"WORKSPACE_PROVIDER_PROVIDER,PROVIDER"`
	DataHome       string `usage:"The data home directory or bucket name" env:"WORKSPACE_PROVIDER_DATA_HOME"`
	S3Bucket       string `usage:"The S3 bucket name" name:"s3-bucket" env:"WORKSPACE_PROVIDER_S3_BUCKET"`
	S3BaseEndpoint string `usage:"The S3 base endpoint to use with S3 compatible providers" name:"s3-base-endpoint" env:"WORKSPACE_PROVIDER_S3_BASE_ENDPOINT"`

	client *client.Client
}

func New() *cobra.Command {
	w := new(workspaceProvider)
	c := cmd.Command(w,
		&create{root: w},
		&rm{root: w},
		&ls{root: w},
		&removeAllWithPrefix{root: w},
		&cpFile{root: w},
		&writeFile{root: w},
		&rmFile{root: w},
		&readFile{root: w},
		&server{root: w},
		&validateEnv{root: w},
		&statFile{root: w},
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

func (w *workspaceProvider) PersistentPre(cmd *cobra.Command, _ []string) error {
	switch w.Provider {
	case client.DirectoryProvider:
	case client.S3Provider:
		if w.S3Bucket == "" {
			return fmt.Errorf("s3 provider requires a bucket name")
		}
	default:
		return fmt.Errorf("invalid workspace provider: %s", w.Provider)
	}

	var err error
	w.client, err = client.New(cmd.Context(), client.Options{
		DirectoryDataHome: w.DataHome,
		S3BucketName:      w.S3Bucket,
		S3BaseEndpoint:    w.S3BaseEndpoint,
	})

	return err
}
