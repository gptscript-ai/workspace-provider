package client

import (
	"fmt"
	"io"
	"iter"
	"maps"
	"net/url"
	"path/filepath"

	"github.com/adrg/xdg"
)

const (
	DirectoryProvider = "directory"
	S3Provider        = "s3"
)

type workspaceFactory interface {
	New(string) workspaceClient
	Create() (string, error)
	Rm(string) error
}

type workspaceClient interface {
	Ls() ([]string, error)
	DeleteFile(string) error
	OpenFile(string) (io.ReadCloser, error)
	WriteFile(string) (io.WriteCloser, error)
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

func (c *Client) Create(provider string, fromWorkspaces ...string) (string, error) {
	factory, err := c.getFactory(provider)
	if err != nil {
		return "", err
	}

	id, err := factory.Create()
	if err != nil {
		return "", err
	}

	destClient := factory.New(id)

	for _, fromWorkspace := range fromWorkspaces {
		sourceClient, err := c.getClient(fromWorkspace)
		if err != nil {
			return "", err
		}
		if err = cp(sourceClient, destClient); err != nil {
			return "", err
		}
	}

	return id, nil
}

func (c *Client) Rm(id string) error {
	u, err := url.Parse(id)
	if err != nil {
		return fmt.Errorf("invalid workspace id: %s", id)
	}

	f, err := c.getFactory(u.Scheme)
	if err != nil {
		return err
	}

	return f.Rm(id)
}

func (c *Client) Ls(id string) ([]string, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.Ls()
}

func (c *Client) DeleteFile(id, file string) error {
	wc, err := c.getClient(id)
	if err != nil {
		return err
	}

	return wc.DeleteFile(file)
}

func (c *Client) OpenFile(id, fileName string) (io.ReadCloser, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.OpenFile(fileName)
}

func (c *Client) WriteFile(id, fileName string) (io.WriteCloser, error) {
	wc, err := c.getClient(id)
	if err != nil {
		return nil, err
	}

	return wc.WriteFile(fileName)
}

func (c *Client) getClient(id string) (workspaceClient, error) {
	u, err := url.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid workspace id: %s", id)
	}

	f, err := c.getFactory(u.Scheme)
	if err != nil {
		return nil, err
	}

	return f.New(id), nil
}

func (c *Client) getFactory(provider string) (workspaceFactory, error) {
	factory, ok := c.factories[provider]
	if !ok {
		return nil, fmt.Errorf("invalid workspace provider: %s", provider)
	}

	return factory, nil
}

func cp(source, dest workspaceClient) error {
	contents, err := source.Ls()
	if err != nil {
		return err
	}

	for _, entry := range contents {
		if err = cpFile(entry, source, dest); err != nil {
			return err
		}
	}

	return nil
}

func cpFile(entry string, source, dest workspaceClient) error {
	sourceFile, err := source.OpenFile(entry)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := dest.WriteFile(entry)
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
