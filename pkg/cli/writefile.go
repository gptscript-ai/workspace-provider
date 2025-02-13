package cli

import (
	"encoding/base64"
	"io"
	"os"
	"strings"

	"github.com/gptscript-ai/go-gptscript"
	"github.com/gptscript-ai/workspace-provider/pkg/client"
	"github.com/spf13/cobra"
)

type writeFile struct {
	root *workspaceProvider

	Base64EncodedInput    bool   `usage:"Encode input as base64" env:"WRITE_FILE_BASE64_ENCODED_INPUT"`
	WithoutCreateRevision bool   `usage:"Do not create a new revision" env:"WRITE_FILE_WITHOUT_CREATE_REVISION"`
	LatestRevisionID      string `usage:"Only write if this is the latest revision" env:"WRITE_FILE_LATEST_REVISION_ID"`
}

func (c *writeFile) Customize(cmd *cobra.Command) {
	cmd.Args = cobra.ExactArgs(3)
	cmd.Use = "write-file [OPTIONS] ID FILENAME CONTENTS|-..."
	cmd.Short = "Write a local file into a workspace, use '-' to read from stdin"
}

func (c *writeFile) Run(cmd *cobra.Command, args []string) error {
	var source io.Reader
	if args[2] == "-" {
		source = os.Stdin
	} else {
		source = strings.NewReader(gptscript.GetEnv("FILE_CONTENTS", args[2]))
	}

	if c.Base64EncodedInput {
		source = base64.NewDecoder(base64.StdEncoding, source)
	}

	return c.root.client.WriteFile(cmd.Context(), args[0], args[1], source, client.WriteOptions{
		LatestRevisionID: c.LatestRevisionID,
		CreateRevision:   &[]bool{!c.WithoutCreateRevision}[0],
	})
}
