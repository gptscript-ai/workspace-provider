package server

import (
	"fmt"
	"net/http"
)

func (s *server) deleteFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")

	if err := s.client.DeleteFile(r.Context(), id, fileName); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(fmt.Sprintf("file %s has been deleted from workspace %s", fileName, id)))
}
