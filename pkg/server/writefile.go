package server

import (
	"encoding/base64"
	"fmt"
	"net/http"
)

func (s *server) writeFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")

	if err := s.client.WriteFile(r.Context(), id, fileName, base64.NewDecoder(base64.StdEncoding, r.Body)); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(fmt.Sprintf("file %s has been written to workspace %s", fileName, id)))
}
