package server

import (
	"errors"
	"io"
	"net/http"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
)

func (s *server) readFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")

	rc, err := s.client.OpenFile(r.Context(), id, fileName)
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

	_, _ = io.Copy(w, rc)
}
