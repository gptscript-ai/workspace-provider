package client

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

func newDirectory(dataHome string) workspaceFactory {
	return &directory{
		dataHome: dataHome,
	}
}

type directory struct {
	dataHome string
}

func (d *directory) New(id string) workspaceClient {
	id = strings.TrimPrefix(id, "directory://")
	if !path.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}
	return &directory{
		dataHome: id,
	}
}

func (d *directory) Create() (string, error) {
	dir := filepath.Join(d.dataHome, uuid.NewString())
	return "directory://" + dir, os.MkdirAll(dir, 0o755)
}

func (d *directory) Rm(id string) error {
	id = strings.TrimPrefix(id, "directory://")
	if !path.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}
	return os.RemoveAll(id)
}

func (d *directory) DeleteFile(file string) error {
	return os.Remove(filepath.Join(d.dataHome, file))
}

func (d *directory) OpenFile(file string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(d.dataHome, file))
}

func (d *directory) WriteFile(fileName string) (io.WriteCloser, error) {
	file, err := os.OpenFile(filepath.Join(d.dataHome, fileName), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (d *directory) Ls() ([]string, error) {
	entries, err := os.ReadDir(d.dataHome)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &WorkspaceNotFoundError{id: "directory://" + d.dataHome}
		}
		return nil, err
	}

	contents := make([]string, 0, len(entries))
	for _, entry := range entries {
		contents = append(contents, entry.Name())
	}

	return contents, nil
}

func (d *directory) Cp(source workspaceClient) error {
	contents, err := source.Ls()
	if err != nil {
		return err
	}

	for _, entry := range contents {
		sourceFile, err := source.OpenFile(entry)
		if err != nil {
			return err
		}

		destFile, err := d.WriteFile(entry)
		if err != nil {
			return err
		}

		_, err = io.Copy(destFile, sourceFile)
		if err != nil {
			return err
		}
	}

	return nil
}
