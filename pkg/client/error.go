package client

import (
	"fmt"
	"os"
)

func newWorkspaceNotFoundError(id string) error {
	if os.Getenv("WORKSPACE_PROVIDER_IGNORE_WORKSPACE_NOT_FOUND") != "" {
		return nil
	}
	return WorkspaceNotFoundError{id: id}
}

type WorkspaceNotFoundError struct {
	id string
}

func (e WorkspaceNotFoundError) Error() string {
	return fmt.Sprintf("workspace not found: %s", e.id)
}

type DirectoryAlreadyExistsError struct {
	id, dir string
}

func (e DirectoryAlreadyExistsError) Error() string {
	return fmt.Sprintf("directory already exists: %s/%s", e.id, e.dir)
}

type DirectoryNotFoundError struct {
	id, dir string
}

func (e DirectoryNotFoundError) Error() string {
	return fmt.Sprintf("directory not found: %s/%s", e.id, e.dir)
}

type DirectoryNotEmptyError struct {
	id, dir string
}

func (e DirectoryNotEmptyError) Error() string {
	return fmt.Sprintf("directory not empty: %s/%s", e.id, e.dir)
}

type FileNotFoundError struct {
	id, file string
}

func (e FileNotFoundError) Error() string {
	return fmt.Sprintf("file not found: %s/%s", e.id, e.file)
}
