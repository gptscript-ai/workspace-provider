package client

import "time"

type FileInfo struct {
	WorkspaceID string    `json:"workspaceID"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"modTime"`
}
