package client

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/adrg/xdg"
	"github.com/google/uuid"
)

func newDirectory(dataHome string) workspaceFactory {
	if dataHome == "" {
		dataHome = filepath.Join(xdg.DataHome, "workspace-provider")
	}
	return &directory{
		dataHome: dataHome,
	}
}

type directory struct {
	dataHome string
}

func (d *directory) New(_ context.Context, id string) workspaceClient {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}
	return &directory{
		dataHome: id,
	}
}

func (d *directory) Create(context.Context) (string, error) {
	dir := filepath.Join(d.dataHome, uuid.NewString())
	return DirectoryProvider + "://" + dir, os.MkdirAll(dir, 0o755)
}

func (d *directory) Rm(_ context.Context, id string) error {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}

	err := os.RemoveAll(id)
	if os.IsNotExist(err) {
		return WorkspaceNotFoundError{id: id}
	}

	return err
}

func (d *directory) DeleteFile(_ context.Context, file string) error {
	return os.Remove(filepath.Join(d.dataHome, file))
}

func (d *directory) OpenFile(_ context.Context, file string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(d.dataHome, file))
}

func (d *directory) WriteFile(_ context.Context, fileName string, opt WriteOptions) (io.WriteCloser, error) {
	fullFilePath := filepath.Join(d.dataHome, fileName)
	if opt.CreateDirs {
		if err := os.MkdirAll(path.Dir(fullFilePath), 0o755); err != nil {
			return nil, err
		}
	}

	flags := os.O_WRONLY | os.O_CREATE
	if opt.WithoutCreate {
		flags ^= os.O_CREATE
	}
	if opt.MustNotExist {
		flags |= os.O_CREATE | os.O_EXCL
	}

	file, err := os.OpenFile(fullFilePath, flags, 0o644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (d *directory) Ls(ctx context.Context, opt LsOptions) ([]string, error) {
	return d.ls(ctx, opt, "")
}

func (d *directory) ls(ctx context.Context, opt LsOptions, prefix string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(d.dataHome, opt.SubDir, prefix))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &WorkspaceNotFoundError{id: DirectoryProvider + "://" + d.dataHome}
		}
		return nil, err
	}

	contents := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && !opt.NonRecursive {
			c, err := d.ls(ctx, opt, filepath.Join(prefix, entry.Name()))
			if err != nil {
				return nil, err
			}

			contents = append(contents, c...)
		} else if !entry.IsDir() && (!opt.ExcludeHidden || !strings.HasPrefix(entry.Name(), ".")) {
			contents = append(contents, filepath.Join(prefix, entry.Name()))
		}
	}

	return contents, nil
}
