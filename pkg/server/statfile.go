package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
)

func (s *server) statFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fileName := r.PathValue("fileName")

	info, err := s.client.StatFile(r.Context(), id, fileName)
	if err != nil {
		if fnf := (*client.NotFoundError)(nil); errors.As(err, &fnf) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_ = json.NewEncoder(w).Encode(info)
}
