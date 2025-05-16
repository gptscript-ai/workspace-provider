package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gptscript-ai/workspace-provider/pkg/client"
)

func Run(ctx context.Context, client *client.Client, port int) error {
	mux := http.NewServeMux()
	s := &server{
		client: client,
		httpServer: &http.Server{
			Addr:    fmt.Sprintf("127.0.0.1:%d", port),
			Handler: mux,
		},
	}

	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("POST /create", s.create)
	mux.HandleFunc("POST /rm/{id}", s.rm)
	mux.HandleFunc("POST /ls/{id}/{prefix...}", s.ls)
	mux.HandleFunc("POST /read-file/{id}/{fileName}", s.readFile)
	mux.HandleFunc("POST /read-file-with-revision/{id}/{fileName}", s.readFileWithRevision)
	mux.HandleFunc("POST /write-file/{id}/{fileName}", s.writeFile)
	mux.HandleFunc("POST /rm-file/{id}/{fileName}", s.deleteFile)
	mux.HandleFunc("POST /stat-file/{id}/{fileName}", s.statFile)
	mux.HandleFunc("POST /rm-with-prefix/{id}/{prefix}", s.removeAllWithPrefix)
	mux.HandleFunc("POST /list-revisions/{id}/{fileName}", s.listRevisions)
	mux.HandleFunc("POST /get-revision/{id}/{fileName}/{revisionID}", s.getRevision)
	mux.HandleFunc("POST /delete-revision/{id}/{fileName}/{revisionID}", s.deleteRevision)

	context.AfterFunc(ctx, func() {
		if err := s.httpServer.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	})

	if err := s.httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

type server struct {
	client     *client.Client
	httpServer *http.Server
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
