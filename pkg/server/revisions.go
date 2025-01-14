package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
)

func (s *server) listRevisions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")

	revisions, err := s.client.ListRevisions(r.Context(), id, fileName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_ = json.NewEncoder(w).Encode(revisions)
}

func (s *server) getRevision(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")
	revisionID := r.PathValue("revisionID")

	rc, err := s.client.GetRevision(r.Context(), id, fileName, revisionID)
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

	writer := base64.NewEncoder(base64.StdEncoding, w)
	defer writer.Close()

	_, _ = io.Copy(writer, rc)
}

func (s *server) deleteRevision(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")
	revisionID := r.PathValue("revisionID")

	if err := s.client.DeleteRevision(r.Context(), id, fileName, revisionID); err != nil {
		if fnf := (*client.NotFoundError)(nil); errors.As(err, &fnf) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = w.Write([]byte(err.Error()))
		return
	}
}
