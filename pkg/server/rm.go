package server

import (
	"fmt"
	"net/http"
)

func (s *server) rm(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.client.Rm(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(fmt.Sprintf("workspace with ID %s has been deleted", id)))
}
