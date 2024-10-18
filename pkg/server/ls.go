package server

import (
	"net/http"
	"strings"
)

func (s *server) ls(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	prefix := r.PathValue("prefix")

	ws, err := s.client.Ls(r.Context(), id, prefix)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(strings.Join(ws, "\n")))
}
