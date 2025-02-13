package client

import "time"

type FileInfo struct {
	WorkspaceID string    `json:"workspaceID"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"modTime"`
	MimeType    string    `json:"mimeType"`
	RevisionID  string    `json:"revisionID"`
}

func (f *FileInfo) GetRevisionID() (string, error) {
	if f.RevisionID == "" {
		return "", ErrRevisionNotRequested
	}
	return f.RevisionID, nil
}

type RevisionInfo struct {
	FileInfo
	RevisionID string `json:"revisionID"`
}

func (r *RevisionInfo) GetRevisionID() (string, error) {
	return r.RevisionID, nil
}

type revisionInfo struct {
	CurrentID int64 `json:"currentID"`
}
