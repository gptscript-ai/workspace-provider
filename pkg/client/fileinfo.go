package client

import "time"

type FileInfo struct {
	WorkspaceID string
	Name        string
	Size        int64
	ModTime     time.Time
}
