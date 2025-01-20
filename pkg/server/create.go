package server

import (
	"encoding/json"
	"net/http"
	"strings"
)

type createRequest struct {
	Provider         string   `json:"provider"`
	FromWorkspaceIDs []string `json:"fromWorkspaceIDs"`
	// This tool accepts two different types "from these workspaces" because it is not possible to specify that a tool
	// argument is an array. So, we also support a comma-delimited string for workspace IDs.
	WorkspaceIDs string `json:"workspace_ids"`
}

func (s *server) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	if req.WorkspaceIDs != "" {
		req.FromWorkspaceIDs = append(req.FromWorkspaceIDs, strings.Split(req.WorkspaceIDs, ",")...)
	}

	id, err := s.client.Create(r.Context(), req.Provider, req.FromWorkspaceIDs...)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	_, _ = w.Write([]byte(id))
}
