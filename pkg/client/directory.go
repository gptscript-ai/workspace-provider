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

func (d *directory) Create(ctx context.Context) (string, error) {
	dir := uuid.NewString()
	return DirectoryProvider + "://" + filepath.Join(d.dataHome, dir), d.MkDir(ctx, dir, MkDirOptions{CreateDirs: true})
}

func (d *directory) Rm(_ context.Context, id string) error {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}

	err := os.RemoveAll(id)
	if os.IsNotExist(err) {
		return newWorkspaceNotFoundError(id)
	}

	return err
}

func (d *directory) DeleteFile(_ context.Context, file string) error {
	err := os.Remove(filepath.Join(d.dataHome, file))
	if os.IsNotExist(err) {
		return FileNotFoundError{id: DirectoryProvider + "://" + d.dataHome, file: file}
	}

	return err
}

func (d *directory) OpenFile(_ context.Context, file string) (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(d.dataHome, file))
	if os.IsNotExist(err) {
		return nil, FileNotFoundError{id: DirectoryProvider + "://" + d.dataHome, file: file}
	}

	return f, err
}

func (d *directory) WriteFile(_ context.Context, fileName string, opt WriteOptions) (io.WriteCloser, error) {
	fullFilePath := filepath.Join(d.dataHome, fileName)
	if opt.CreateDirs {
		if err := os.MkdirAll(filepath.Dir(fullFilePath), 0o755); err != nil {
			return nil, err
		}
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
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
			return nil, newWorkspaceNotFoundError(DirectoryProvider + "://" + d.dataHome)
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

func (d *directory) MkDir(_ context.Context, dirName string, opt MkDirOptions) error {
	fullDirName := filepath.Join(d.dataHome, dirName)
	if _, err := os.Stat(fullDirName); err == nil {
		if opt.MustNotExist {
			return DirectoryAlreadyExistsError{id: DirectoryProvider + "://" + d.dataHome, dir: dirName}
		}

		return nil
	}

	if opt.CreateDirs {
		return os.MkdirAll(fullDirName, 0o755)
	}

	return os.Mkdir(fullDirName, 0o755)
}

func (d *directory) RmDir(_ context.Context, dirName string, opt RmDirOptions) error {
	fullDirName := filepath.Join(d.dataHome, dirName)
	if opt.NonEmpty {
		entries, err := os.ReadDir(fullDirName)
		if err != nil {
			if os.IsNotExist(err) {
				return DirectoryNotFoundError{id: DirectoryProvider + "://" + d.dataHome, dir: dirName}
			}
			return err
		}
		if len(entries) > 0 {
			return DirectoryNotEmptyError{id: DirectoryProvider + "://" + d.dataHome, dir: dirName}
		}
	}

	return os.RemoveAll(fullDirName)
}
