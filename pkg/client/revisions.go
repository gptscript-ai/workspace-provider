package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const revisionsDir = "revisions"

func getRevisionInfo(ctx context.Context, client workspaceClient, fileName string) (revisionInfo, error) {
	var info revisionInfo
	f, err := client.OpenFile(ctx, fileName+".json")
	if err != nil {
		if nfe := (*NotFoundError)(nil); errors.As(err, &nfe) {
			info.CurrentID = -1
			return info, nil
		}
		return info, err
	}
	defer f.Close()

	return info, json.NewDecoder(f).Decode(&info)
}

func writeRevision(ctx context.Context, rClient, wClient workspaceClient, fileName string, info revisionInfo) error {
	f, err := wClient.OpenFile(ctx, fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	return rClient.WriteFile(ctx, fmt.Sprintf("%s.%d", fileName, info.CurrentID), f, WriteOptions{})
}

func deleteRevisionInfo(ctx context.Context, client workspaceClient, fileName string) error {
	return client.DeleteFile(ctx, fileName+".json")
}

func writeRevisionInfo(ctx context.Context, client workspaceClient, fileName string, info revisionInfo) error {
	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal revision info: %w", err)
	}

	return client.WriteFile(ctx, fileName+".json", bytes.NewReader(b), WriteOptions{})
}

func listRevisions(ctx context.Context, client workspaceClient, workspaceID, fileName string) ([]RevisionInfo, error) {
	info, err := getRevisionInfo(ctx, client, fileName)
	if err != nil || info.CurrentID == -1 {
		return nil, err
	}

	revisions := make([]RevisionInfo, 0, info.CurrentID)
	for i := int64(1); i <= info.CurrentID; i++ {
		id := fmt.Sprintf("%d", i)
		f, err := client.StatFile(ctx, fmt.Sprintf("%s.%s", fileName, id))
		if err != nil {
			if nfe := (*NotFoundError)(nil); errors.As(err, &nfe) {
				continue
			}

			return nil, err
		}

		f.WorkspaceID = workspaceID
		f.Name = fileName
		revisions = append(revisions, RevisionInfo{
			RevisionID: id,
			FileInfo:   f,
		})
	}

	return revisions, nil
}

func deleteRevision(ctx context.Context, client workspaceClient, fileName string, revisionID string) error {
	if err := client.DeleteFile(ctx, fmt.Sprintf("%s.%s", fileName, revisionID)); err != nil {
		return err
	}

	info, err := getRevisionInfo(ctx, client, fileName)
	if err != nil {
		return err
	}

	if fmt.Sprintf("%d", info.CurrentID) != revisionID {
		return nil
	}

	info.CurrentID--
	return writeRevisionInfo(ctx, client, fileName, info)
}

func getRevision(ctx context.Context, client workspaceClient, fileName string, revisionID string) (io.ReadCloser, error) {
	return client.OpenFile(ctx, fmt.Sprintf("%s.%s", fileName, revisionID))
}
