package client

import (
	"fmt"
)

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

type ConflictError struct {
	id              string
	name            string
	latestRevision  string
	currentRevision string
}

func newConflictError(id, name, latestRevision, currentRevision string) *ConflictError {
	return &ConflictError{id: id, name: name, latestRevision: latestRevision, currentRevision: currentRevision}
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict: %s/%s (latest revision: %s, current revision: %s)", e.id, e.name, e.latestRevision, e.currentRevision)
}
