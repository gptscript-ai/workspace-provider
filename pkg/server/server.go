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
	mux.HandleFunc("/create", s.create)
	mux.HandleFunc("/rm/{id}", s.rm)
	mux.HandleFunc("/ls/{id}/{prefix...}", s.ls)
	mux.HandleFunc("/read-file/{id}/{fileName}", s.readFile)
	mux.HandleFunc("/read-file-with-revision/{id}/{fileName}", s.readFileWithRevision)
	mux.HandleFunc("/write-file/{id}/{fileName}", s.writeFile)
	mux.HandleFunc("/rm-file/{id}/{fileName}", s.deleteFile)
	mux.HandleFunc("/stat-file/{id}/{fileName}", s.statFile)
	mux.HandleFunc("/rm-with-prefix/{id}/{prefix}", s.removeAllWithPrefix)
	mux.HandleFunc("/list-revisions/{id}/{fileName}", s.listRevisions)
	mux.HandleFunc("/get-revision/{id}/{fileName}/{revisionID}", s.getRevision)
	mux.HandleFunc("/delete-revision/{id}/{fileName}/{revisionID}", s.deleteRevision)

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
