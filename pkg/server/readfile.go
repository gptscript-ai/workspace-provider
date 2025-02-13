package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
)

type readFileResponse struct {
	RevisionID string `json:"revisionID"`
	Content    []byte `json:"content"`
}

func (s *server) readFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")
	withLatestRevision := r.URL.Query().Get("withLatestRevision") == "true"

	rc, err := s.client.OpenFile(r.Context(), id, fileName, client.OpenOptions{WithLatestRevisionID: withLatestRevision})
	if err != nil {
		if fnf := (*client.NotFoundError)(nil); errors.As(err, &fnf) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	var revision string
	if withLatestRevision {
		revision, err = rc.GetRevisionID()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
	}

	b, err := json.Marshal(readFileResponse{RevisionID: revision, Content: content})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write(b)
}
