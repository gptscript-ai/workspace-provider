package client

import "time"

type FileInfo struct {
	WorkspaceID string    `json:"workspaceID"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"modTime"`
	MimeType    string    `json:"mimeType"`
}

type RevisionInfo struct {
	FileInfo
	RevisionID string `json:"revisionID"`
}

type revisionInfo struct {
	CurrentID int64 `json:"currentID"`
}
