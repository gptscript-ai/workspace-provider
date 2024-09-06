package client

import (
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"path/filepath"
	"strings"

	"github.com/adrg/xdg"
)

const (
	DirectoryProvider = "directory"
	S3Provider        = "s3"
)

type workspaceFactory interface {
	New(context.Context, string) workspaceClient
	Create(context.Context) (string, error)
	Rm(context.Context, string) error
}

type workspaceClient interface {
	Ls(context.Context) ([]string, error)
	DeleteFile(context.Context, string) error
	OpenFile(context.Context, string) (io.ReadCloser, error)
	WriteFile(context.Context, string) (io.WriteCloser, error)
}

type Options struct {
	DirectoryDataHome string
	S3DataHome        string
}

func complete(opts ...Options) Options {
	var opt Options

	for _, o := range opts {
		if o.DirectoryDataHome != "" {
			opt.DirectoryDataHome = o.DirectoryDataHome
		}
		if o.S3DataHome != "" {
			opt.S3DataHome = o.S3DataHome
		}
	}

	if opt.DirectoryDataHome == "" {
		opt.DirectoryDataHome = filepath.Join(xdg.DataHome, "workspace-provider")
	}

	return opt
}

func New(opts ...Options) *Client {
	opt := complete(opts...)
	return &Client{
		factories: map[string]workspaceFactory{
			DirectoryProvider: newDirectory(opt.DirectoryDataHome),
			S3Provider:        newS3(opt.S3DataHome),
		},
	}
}

type Client struct {
	factories map[string]workspaceFactory
}

func (c *Client) Providers() iter.Seq[string] {
	return maps.Keys(c.factories)
}

func (c *Client) Create(ctx context.Context, provider string, fromWorkspaces ...string) (string, error) {
	factory, err := c.getFactory(provider)
	if err != nil {
		return "", err
	}

	id, err := factory.Create(ctx)
	if err != nil {
		return "", err
	}

	destClient := factory.New(ctx, id)

	for _, fromWorkspace := range fromWorkspaces {
		sourceClient, err := c.getClient(ctx, fromWorkspace)
		if err != nil {
			return "", err
		}
		if err = cp(ctx, sourceClient, destClient); err != nil {
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

func (c *Client) Ls(ctx context.Context, id string) ([]string, error) {
	wc, err := c.getClient(ctx, id)
	if err != nil {
		return nil, err
	}

	return wc.Ls(ctx)
}

func (c *Client) DeleteFile(ctx context.Context, id, file string) error {
	wc, err := c.getClient(ctx, id)
	if err != nil {
		return err
	}

	return wc.DeleteFile(ctx, file)
}

func (c *Client) OpenFile(ctx context.Context, id, fileName string) (io.ReadCloser, error) {
	wc, err := c.getClient(ctx, id)
	if err != nil {
		return nil, err
	}

	return wc.OpenFile(ctx, fileName)
}

func (c *Client) WriteFile(ctx context.Context, id, fileName string) (io.WriteCloser, error) {
	wc, err := c.getClient(ctx, id)
	if err != nil {
		return nil, err
	}

	return wc.WriteFile(ctx, fileName)
}

func (c *Client) getClient(ctx context.Context, id string) (workspaceClient, error) {
	provider, _, ok := strings.Cut(id, "://")
	if !ok {
		return nil, fmt.Errorf("invalid workspace id: %s", id)
	}

	f, err := c.getFactory(provider)
	if err != nil {
		return nil, err
	}

	return f.New(ctx, id), nil
}

func (c *Client) getFactory(provider string) (workspaceFactory, error) {
	factory, ok := c.factories[provider]
	if !ok {
		return nil, fmt.Errorf("invalid workspace provider: %s", provider)
	}

	return factory, nil
}

func cp(ctx context.Context, source, dest workspaceClient) error {
	contents, err := source.Ls(ctx)
	if err != nil {
		return err
	}

	for _, entry := range contents {
		if err = cpFile(ctx, entry, source, dest); err != nil {
			return err
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

	destFile, err := dest.WriteFile(ctx, entry)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return nil
}
