package client

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/adrg/xdg"
	"github.com/google/uuid"
)

func newDirectory(dataHome string) workspaceFactory {
	if dataHome == "" {
		dataHome = filepath.Join(xdg.DataHome, "workspace-provider")
	}
	return &directoryProvider{
		dataHome: dataHome,
	}
}

type directoryProvider struct {
	dataHome string
}

func (d *directoryProvider) New(id string) workspaceClient {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}
	return &directoryProvider{
		dataHome: id,
	}
}

func (d *directoryProvider) Create() string {
	return DirectoryProvider + "://" + filepath.Join(d.dataHome, uuid.NewString())
}

func (d *directoryProvider) Rm(_ context.Context, id string) error {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}

	if _, err := os.Stat(id); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return os.RemoveAll(id)
}

func (d *directoryProvider) DeleteFile(_ context.Context, file string) error {
	err := os.Remove(filepath.Join(d.dataHome, file))
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func (d *directoryProvider) OpenFile(_ context.Context, file string) (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(d.dataHome, file))
	if os.IsNotExist(err) {
		return nil, newNotFoundError(DirectoryProvider+"://"+d.dataHome, file)
	}

	return f, err
}

func (d *directoryProvider) WriteFile(_ context.Context, fileName string, reader io.Reader) error {
	fullFilePath := filepath.Join(d.dataHome, fileName)
	if err := os.MkdirAll(filepath.Dir(fullFilePath), 0o755); err != nil {
		return err
	}

	file, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	return err
}

func (d *directoryProvider) Ls(ctx context.Context, prefix string) ([]string, error) {
	files, err := d.ls(ctx, prefix)
	if err != nil || len(files) == 0 {
		return nil, err
	}
	return files, nil
}

func (d *directoryProvider) ls(ctx context.Context, prefix string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(d.dataHome, prefix))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			subFiles, err := d.ls(ctx, filepath.Join(prefix, entry.Name()))
			if err != nil {
				return nil, err
			}

			files = append(files, subFiles...)
		} else if !entry.IsDir() {
			files = append(files, filepath.Join(prefix, entry.Name()))
		}
	}

	return files, nil
}

func (d *directoryProvider) RemoveAllWithPrefix(_ context.Context, dirName string) error {
	fullDirName := filepath.Join(d.dataHome, dirName)
	_, err := os.Stat(fullDirName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return os.RemoveAll(fullDirName)
}
