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
