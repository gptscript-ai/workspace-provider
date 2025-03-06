package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/gabriel-vasile/mimetype"
	"github.com/google/uuid"
)

func newAzure(containerName, connectionString string) (workspaceFactory, error) {
	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return nil, err
	}

	return &azureProvider{
		containerName: containerName,
		client:        client,
		revisionsProvider: &azureProvider{
			containerName: containerName,
			dir:           revisionsDir,
			client:        client,
		},
	}, nil
}

type azureProvider struct {
	containerName, dir string
	client             *azblob.Client
	revisionsProvider  *azureProvider
}

func (a *azureProvider) New(id string) (workspaceClient, error) {
	container, dir, _ := strings.Cut(strings.TrimPrefix(id, AzureProvider+"://"), "/")
	if dir == revisionsDir {
		return nil, errors.New("cannot create a workspace client for the revisions directory")
	}

	return &azureProvider{
		containerName: container,
		dir:           dir,
		client:        a.client,
		revisionsProvider: &azureProvider{
			containerName: container,
			dir:           fmt.Sprintf("%s/%s", revisionsDir, dir),
			client:        a.client,
		},
	}, nil
}

func (a *azureProvider) Create() string {
	return AzureProvider + "://" + filepath.Join(a.containerName, uuid.NewString())
}

func (a *azureProvider) Rm(ctx context.Context, id string) error {
	container, dir, _ := strings.Cut(strings.TrimPrefix(id, AzureProvider+"://"), "/")

	newA := &azureProvider{
		containerName: container,
		dir:           dir,
		client:        a.client,
		revisionsProvider: &azureProvider{
			containerName: container,
			dir:           fmt.Sprintf("%s/%s", revisionsDir, dir),
			client:        a.client,
		},
	}

	// Best effort
	_ = newA.revisionsProvider.RemoveAllWithPrefix(ctx, "")

	return newA.RemoveAllWithPrefix(ctx, "")
}

func (a *azureProvider) Ls(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" {
		prefix = fmt.Sprintf("%s/%s/", a.dir, strings.TrimSuffix(prefix, "/"))
	} else {
		prefix = fmt.Sprintf("%s/", a.dir)
	}

	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	var files []string
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, blob := range resp.Segment.BlobItems {
			files = append(files, strings.TrimPrefix(*blob.Name, a.dir+"/"))
		}
	}

	return files, nil
}

func (a *azureProvider) DeleteFile(ctx context.Context, filePath string) error {
	blobClient := a.client.ServiceClient().NewContainerClient(a.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", a.dir, filePath))
	_, err := blobClient.Delete(ctx, nil)
	if err != nil {
		var storageErr *azcore.ResponseError
		if errors.As(err, &storageErr) && storageErr.StatusCode == 404 {
			return nil
		}
		return err
	}

	if a.revisionsProvider == nil {
		return nil
	}

	info, err := getRevisionInfo(ctx, a.revisionsProvider, filePath)
	if err != nil {
		return err
	}

	for i := info.CurrentID; i > 0; i-- {
		// Best effort
		_ = deleteRevision(ctx, a.revisionsProvider, filePath, fmt.Sprintf("%d", i))
	}

	// Best effort
	_ = deleteRevisionInfo(ctx, a.revisionsProvider, filePath)

	return nil
}

func (a *azureProvider) OpenFile(ctx context.Context, filePath string, opt OpenOptions) (*File, error) {
	blobClient := a.client.ServiceClient().NewContainerClient(a.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", a.dir, filePath))

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		var storageErr *azcore.ResponseError
		if errors.As(err, &storageErr) && storageErr.StatusCode == 404 {
			return nil, newNotFoundError(fmt.Sprintf("%s://%s/%s", AzureProvider, a.containerName, a.dir), filePath)
		}
		return nil, err
	}

	var revision string
	if opt.WithLatestRevisionID {
		rev, err := getRevisionInfo(ctx, a.revisionsProvider, filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to get revision info: %w", err)
		}
		revision = fmt.Sprintf("%d", rev.CurrentID)
	}

	return &File{
		ReadCloser: resp.Body,
		RevisionID: revision,
	}, nil
}

func (a *azureProvider) WriteFile(ctx context.Context, fileName string, reader io.Reader, opt WriteOptions) error {
	if a.revisionsProvider != nil && (opt.CreateRevision == nil || *opt.CreateRevision) {
		info, err := getRevisionInfo(ctx, a.revisionsProvider, fileName)
		if err != nil {
			if nfe := (*NotFoundError)(nil); !errors.As(err, &nfe) {
				return err
			}
		}

		if opt.LatestRevisionID != "" {
			requiredLatestRevision, err := strconv.ParseInt(opt.LatestRevisionID, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse latest revision for write: %w", err)
			}

			if requiredLatestRevision != info.CurrentID {
				return newConflictError(AzureProvider+"://"+a.containerName, fileName, opt.LatestRevisionID, fmt.Sprintf("%d", info.CurrentID))
			}
		}

		info.CurrentID++
		if err = writeRevision(ctx, a.revisionsProvider, a, fileName, info); err != nil {
			if nfe := (*NotFoundError)(nil); !errors.As(err, &nfe) {
				return fmt.Errorf("failed to write revision: %w", err)
			}
		}

		if err = writeRevisionInfo(ctx, a.revisionsProvider, fileName, info); err != nil {
			return fmt.Errorf("failed to write revision info: %w", err)
		}
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	blobClient := a.client.ServiceClient().NewContainerClient(a.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", a.dir, fileName))
	_, err = blobClient.UploadStream(ctx, bytes.NewReader(data), nil)
	return err
}

func (a *azureProvider) StatFile(ctx context.Context, fileName string, opt StatOptions) (FileInfo, error) {
	blobClient := a.client.ServiceClient().NewContainerClient(a.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", a.dir, fileName))

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		var storageErr *azcore.ResponseError
		if errors.As(err, &storageErr) && storageErr.StatusCode == 404 {
			return FileInfo{}, newNotFoundError(fmt.Sprintf("%s://%s/%s", AzureProvider, a.containerName, a.dir), fileName)
		}
		return FileInfo{}, err
	}

	var mime string
	if props.ContentType != nil {
		mime = *props.ContentType
	}

	// Get the first 3072 bytes of the blob to detect the mimetype
	downloadOpts := &azblob.DownloadStreamOptions{}
	downloadOpts.Range.Offset = 0
	downloadOpts.Range.Count = 3072
	resp, err := blobClient.DownloadStream(ctx, downloadOpts)
	if err == nil {
		defer resp.Body.Close()
		mt, err := mimetype.DetectReader(resp.Body)
		if err == nil {
			mime = strings.Split(mt.String(), ";")[0]
		}
	}

	var modTime time.Time
	if props.LastModified != nil {
		modTime = *props.LastModified
	}

	var revision string
	if opt.WithLatestRevisionID {
		rev, err := getRevisionInfo(ctx, a.revisionsProvider, fileName)
		if err != nil {
			return FileInfo{}, err
		}
		revision = fmt.Sprintf("%d", rev.CurrentID)
	}

	return FileInfo{
		WorkspaceID: fmt.Sprintf("%s://%s/%s", AzureProvider, a.containerName, a.dir),
		Name:        strings.TrimPrefix(fileName, a.dir+"/"),
		Size:        *props.ContentLength,
		ModTime:     modTime,
		MimeType:    mime,
		RevisionID:  revision,
	}, nil
}

func (a *azureProvider) RemoveAllWithPrefix(ctx context.Context, prefix string) error {
	if prefix != "" {
		prefix = fmt.Sprintf("%s/%s/", a.dir, strings.TrimSuffix(prefix, "/"))
	} else {
		prefix = fmt.Sprintf("%s/", a.dir)
	}

	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, blob := range resp.Segment.BlobItems {
			blobClient := containerClient.NewBlockBlobClient(*blob.Name)
			if _, err := blobClient.Delete(ctx, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *azureProvider) ListRevisions(ctx context.Context, fileName string) ([]RevisionInfo, error) {
	return listRevisions(ctx, a.revisionsProvider, fmt.Sprintf("%s://%s/%s", AzureProvider, a.containerName, a.dir), fileName)
}

func (a *azureProvider) GetRevision(ctx context.Context, fileName, revisionID string) (*File, error) {
	return getRevision(ctx, a.revisionsProvider, fileName, revisionID)
}

func (a *azureProvider) DeleteRevision(ctx context.Context, fileName, revisionID string) error {
	return deleteRevision(ctx, a.revisionsProvider, fileName, revisionID)
}

func (a *azureProvider) RevisionClient() workspaceClient {
	return a.revisionsProvider
}
