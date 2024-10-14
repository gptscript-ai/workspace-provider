package cli

import (
	"encoding/json"
	"fmt"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
	"github.com/spf13/cobra"
)

type ls struct {
	root *workspaceProvider

	client.LsOptions
	JSON bool `usage:"Output as JSON" env:"LS_JSON"`
}

func (l *ls) Customize(c *cobra.Command) {
	c.Args = cobra.MinimumNArgs(1)
	c.Use = "ls [OPTIONS] ID..."
	c.Short = "List files in a workspace"
}

func (l *ls) Run(cmd *cobra.Command, args []string) error {
	var workspaceContents []client.WorkspaceContent
	for _, arg := range args {
		contents, err := l.root.client.Ls(cmd.Context(), arg, l.LsOptions)
		if err != nil {
			return err
		}

		if l.JSON {
			workspaceContents = append(workspaceContents, contents)
			continue
		}

		printContent(contents)
	}

	if l.JSON {
		b, err := json.Marshal(workspaceContents)
		if err != nil {
			return fmt.Errorf("failed to marshal workspace contents: %w", err)
		}

		fmt.Println(string(b))
	}

	return nil
}

func printContent(content client.WorkspaceContent) {
	fmt.Printf("%s:\n", content.ID)
	fmt.Printf("%s:\n", content.Path)
	for _, child := range content.Children {
		if child.FileName != "" {
			fmt.Printf("%s\n", child.FileName)
		} else if child.Path != "" {
			printContent(child)
		}
	}
	fmt.Print("\n\n")
}
