package server

import (
	"fmt"
	"net/http"
)

func (s *server) removeAllWithPrefix(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	prefix := r.PathValue("prefix")

	if err := s.client.RemoveAllWithPrefix(r.Context(), id, prefix); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(fmt.Sprintf("files with prefix %s deleted from workspace %s", prefix, id)))
}
