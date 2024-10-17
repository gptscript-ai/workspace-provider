package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type ls struct {
	root *workspaceProvider

	Prefix string `usage:"Only list files with this prefix" env:"LS_PREFIX"`
}

func (l *ls) Customize(c *cobra.Command) {
	c.Args = cobra.MinimumNArgs(1)
	c.Use = "ls [OPTIONS] ID..."
	c.Short = "List files in a workspace"
}

func (l *ls) Run(cmd *cobra.Command, args []string) error {
	for _, arg := range args {
		contents, err := l.root.client.Ls(cmd.Context(), arg, l.Prefix)
		if err != nil {
			return err
		}

		printContent(arg, contents)
	}

	return nil
}

func printContent(id string, content []string) {
	fmt.Printf("%s:\n", id)
	fmt.Println(strings.Join(content, "\n"))
	fmt.Print("\n\n")
}
