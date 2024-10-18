package server

import (
	"encoding/json"
	"net/http"
)

type createRequest struct {
	Provider         string   `json:"provider"`
	FromWorkspaceIDs []string `json:"fromWorkspaceIDs"`
}

func (s *server) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id, err := s.client.Create(r.Context(), req.Provider, req.FromWorkspaceIDs...)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(id))
}
