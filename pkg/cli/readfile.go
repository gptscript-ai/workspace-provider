package cli

import (
	"encoding/base64"
	"io"

	"github.com/spf13/cobra"
)

type readFile struct {
	root *workspaceProvider

	Base64EncodeOutput bool `usage:"Encode output as base64" env:"READ_FILE_BASE64_ENCODE_OUTPUT"`
}

func (r *readFile) Customize(c *cobra.Command) {
	c.Args = cobra.ExactArgs(2)
	c.Use = "read-file [OPTIONS] ID FILE"
	c.Short = "Read file from a workspace"
}

func (r *readFile) Run(cmd *cobra.Command, args []string) error {
	file, err := r.root.client.OpenFile(cmd.Context(), args[0], args[1])
	if err != nil {
		return err
	}
	defer file.Close()

	writer := cmd.OutOrStdout()
	if r.Base64EncodeOutput {
		enc := base64.NewEncoder(base64.StdEncoding, cmd.OutOrStdout())
		defer enc.Close()
		writer = enc
	}

	_, err = io.Copy(writer, file)
	return err
}
