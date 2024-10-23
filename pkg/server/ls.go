package server

import (
	"encoding/json"
	"fmt"
	"net/http"
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

	b, err := json.Marshal(ws)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("error: %s", err.Error())))
		return
	}

	_, _ = w.Write(b)
}
