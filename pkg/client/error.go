package client

import "fmt"

type WorkspaceNotFoundError struct {
	id string
}

func (e WorkspaceNotFoundError) Error() string {
	return fmt.Sprintf("workspace not found: %s", e.id)
}
