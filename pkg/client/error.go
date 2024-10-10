package client

import (
	"fmt"
	"os"
)

func newWorkspaceNotFoundError(id string) error {
	if os.Getenv("WORKSPACE_PROVIDER_IGNORE_WORKSPACE_NOT_FOUND") != "" {
		return nil
	}
	return &WorkspaceNotFoundError{newNotFoundError(id, "")}
}

type WorkspaceNotFoundError struct {
	*NotFoundError
}

func (e *WorkspaceNotFoundError) Error() string {
	return fmt.Sprintf("workspace not found: %s", e.id)
}

type DirectoryAlreadyExistsError struct {
	id, dir string
}

func (e *DirectoryAlreadyExistsError) Error() string {
	return fmt.Sprintf("directory already exists: %s/%s", e.id, e.dir)
}

type DirectoryNotFoundError struct {
	*NotFoundError
}

func (e *DirectoryNotFoundError) Error() string {
	return fmt.Sprintf("directory not found: %s/%s", e.id, e.name)
}

type DirectoryNotEmptyError struct {
	id, dir string
}

func (e *DirectoryNotEmptyError) Error() string {
	return fmt.Sprintf("directory not empty: %s/%s", e.id, e.dir)
}

type FileNotFoundError struct {
	*NotFoundError
}

func (e *FileNotFoundError) Error() string {
	return fmt.Sprintf("file not found: %s/%s", e.id, e.name)
}

type NotFoundError struct {
	id   string
	name string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("not found: %s/%s", e.id, e.name)
}

func newNotFoundError(id, name string) *NotFoundError {
	return &NotFoundError{id: id, name: name}
}
