package cli

import (
	"strconv"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
	"github.com/spf13/cobra"
)

type statFile struct {
	root *workspaceProvider

	WithLatestRevisionID bool `usage:"Include latest revision" env:"STAT_FILE_WITH_LATEST_REVISION_ID"`
}

func (r *statFile) Customize(c *cobra.Command) {
	c.Args = cobra.ExactArgs(2)
	c.Use = "stat-file [OPTIONS] ID FILE"
	c.Short = "Get file stats from a workspace"
}

func (r *statFile) Run(cmd *cobra.Command, args []string) error {
	info, err := r.root.client.StatFile(cmd.Context(), args[0], args[1], client.StatOptions{
		WithLatestRevisionID: r.WithLatestRevisionID,
	})
	if err != nil {
		return err
	}

	writer := cmd.OutOrStdout()

	_, _ = writer.Write([]byte("workspace id: " + info.WorkspaceID + "\n"))
	_, _ = writer.Write([]byte("name: " + info.Name + "\n"))
	_, _ = writer.Write([]byte("size: " + strconv.FormatInt(info.Size, 10) + "\n"))
	_, _ = writer.Write([]byte("mod time: " + info.ModTime.String() + "\n"))
	_, _ = writer.Write([]byte("mime type: " + info.MimeType + "\n"))
	if r.WithLatestRevisionID {
		rev, err := info.GetRevisionID()
		if err != nil {
			_, _ = writer.Write([]byte("revision ID: <not available>\n"))
		} else {
			_, _ = writer.Write([]byte("revision ID: " + rev + "\n"))
		}
	}

	return nil
}
