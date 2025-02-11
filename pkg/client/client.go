package client

import (
	"context"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/adrg/xdg"
)

const (
	DirectoryProvider = "directory"
	S3Provider        = "s3"
)

type workspaceFactory interface {
	New(string) (workspaceClient, error)
	Create() (string, error)
	Rm(context.Context, string) error
}

type workspaceClient interface {
	Ls(context.Context, string) ([]string, error)
	OpenFile(context.Context, string) (io.ReadCloser, error)
	WriteFile(context.Context, string, io.Reader, WriteOptions) error
	DeleteFile(context.Context, string) error
	StatFile(context.Context, string) (FileInfo, error)
	RemoveAllWithPrefix(context.Context, string) error
	ListRevisions(context.Context, string) ([]RevisionInfo, error)
	GetRevision(context.Context, string, string) (io.ReadCloser, error)
	DeleteRevision(context.Context, string, string) error
	RevisionClient() workspaceClient
}

type Options struct {
	DirectoryDataHome string
	S3BucketName      string
	S3BaseEndpoint    string
}

func complete(opts ...Options) Options {
	var opt Options

	for _, o := range opts {
		if o.DirectoryDataHome != "" {
			opt.DirectoryDataHome = o.DirectoryDataHome
		}
		if o.S3BucketName != "" {
			opt.S3BucketName = o.S3BucketName
		}
		if o.S3BaseEndpoint != "" {
			opt.S3BaseEndpoint = o.S3BaseEndpoint
		}
	}

	if opt.DirectoryDataHome == "" {
		opt.DirectoryDataHome = filepath.Join(xdg.DataHome, "workspace-provider")
	}

	return opt
}

func New(ctx context.Context, opts ...Options) (*Client, error) {
	opt := complete(opts...)

	var s3 workspaceFactory
	if opt.S3BucketName != "" {
		var err error
		s3, err = newS3(ctx, opt.S3BucketName, opt.S3BaseEndpoint)
		if err != nil {
			return nil, err
		}
	}
	return &Client{
		factories: map[string]workspaceFactory{
			DirectoryProvider: newDirectory(opt.DirectoryDataHome),
			S3Provider:        s3,
		},
	}, nil
}

type Client struct {
	factories map[string]workspaceFactory
}

func (c *Client) Providers() []string {
	return slices.Collect(maps.Keys(c.factories))
}

func (c *Client) Create(ctx context.Context, provider string, fromWorkspaces ...string) (string, error) {
	if provider == "" {
		provider = DirectoryProvider
	}

	factory, err := c.getFactory(provider)
	if err != nil {
		return "", err
	}

	id, err := factory.Create()
	if err != nil {
		return "", err
	}

	destClient, err := factory.New(id)
	if err != nil {
		return "", err
	}

	for _, fromWorkspace := range fromWorkspaces {
		sourceClient, err := c.getClient(fromWorkspace)
		if err != nil {
			return "", err
		}
		if err = cp(ctx, sourceClient, destClient); err != nil {
			return "", err
		}
		if err = cp(ctx, sourceClient.RevisionClient(), destClient.RevisionClient()); err != nil {
			return "", err
		}
	}

	return id, nil
}

func (c *Client) Rm(ctx context.Context, id string) error {
	provider, _, ok := strings.Cut(id, "://")
	if !ok {
		return fmt.Errorf("invalid workspace id: %s", id)
	}

	f, err := c.getFactory(provider)
	if err != nil {
		return err
	}

	return f.Rm(ctx, id)
}

func (c *Client) Ls(ctx context.Context, id, prefix string) ([]string, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.Ls(ctx, prefix)
}

func (c *Client) DeleteFile(ctx context.Context, id, file string) error {
	wc, err := c.getClient(id)
	if err != nil {
		return err
	}

	return wc.DeleteFile(ctx, file)
}

func (c *Client) OpenFile(ctx context.Context, id, fileName string) (io.ReadCloser, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.OpenFile(ctx, fileName)
}

type WriteOptions struct {
	CreateRevision *bool
	// If LatestRevision is set, then a conflict error will be returned if that revision is not the latest.
	LatestRevision string
}

func (c *Client) WriteFile(ctx context.Context, id, fileName string, reader io.Reader, opts ...WriteOptions) error {
	var opt WriteOptions
	for _, o := range opts {
		if o.CreateRevision != nil {
			opt.CreateRevision = o.CreateRevision
		}
		if o.LatestRevision != "" {
			opt.LatestRevision = o.LatestRevision
		}
	}
	wc, err := c.getClient(id)
	if err != nil {
		return err
	}

	return wc.WriteFile(ctx, fileName, reader, opt)
}

func (c *Client) StatFile(ctx context.Context, id, fileName string) (FileInfo, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return FileInfo{}, err
	}

	return wc.StatFile(ctx, fileName)
}

func (c *Client) RemoveAllWithPrefix(ctx context.Context, id, prefix string) error {
	wc, err := c.getClient(id)
	if err != nil {
		return err
	}

	return wc.RemoveAllWithPrefix(ctx, prefix)
}

func (c *Client) ListRevisions(ctx context.Context, id, fileName string) ([]RevisionInfo, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.ListRevisions(ctx, fileName)
}

func (c *Client) GetRevision(ctx context.Context, id, fileName, revision string) (io.ReadCloser, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.GetRevision(ctx, fileName, revision)
}

func (c *Client) DeleteRevision(ctx context.Context, id, fileName, revision string) error {
	wc, err := c.getClient(id)
	if err != nil {
		return err
	}

	return wc.DeleteRevision(ctx, fileName, revision)
}

func (c *Client) getClient(id string) (workspaceClient, error) {
	provider, _, ok := strings.Cut(id, "://")
	if !ok {
		return nil, fmt.Errorf("invalid workspace id: %s", id)
	}

	f, err := c.getFactory(provider)
	if err != nil {
		return nil, err
	}

	return f.New(id)
}

func (c *Client) getFactory(provider string) (workspaceFactory, error) {
	factory, ok := c.factories[provider]
	if !ok || factory == nil {
		return nil, fmt.Errorf("invalid workspace provider: %s", provider)
	}

	return factory, nil
}

func cp(ctx context.Context, source, dest workspaceClient) error {
	if source == nil {
		return fmt.Errorf("cannot copy from nil workspace client")
	}
	if dest == nil {
		return fmt.Errorf("cannot copy to nil workspace client")
	}

	contents, err := source.Ls(ctx, "")
	if err != nil {
		return err
	}

	for _, entry := range contents {
		if entry != "" {
			if err = cpFile(ctx, entry, source, dest); err != nil {
				return err
			}
		}
	}

	return nil
}

func cpFile(ctx context.Context, entry string, source, dest workspaceClient) error {
	sourceFile, err := source.OpenFile(ctx, entry)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	return dest.WriteFile(ctx, entry, sourceFile, WriteOptions{})
}
