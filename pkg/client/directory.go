package client

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/adrg/xdg"
	"github.com/google/safeopen"
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

func (d *directoryProvider) New(id string) (workspaceClient, error) {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}

	dir := strings.TrimPrefix(id, d.dataHome+string(filepath.Separator))
	base := d.dataHome
	if dirs := strings.Split(dir, string(filepath.Separator)); len(dirs) > 0 && dirs[0] != ".." {
		base = filepath.Join(base, dirs[0])
		dir = strings.TrimPrefix(dir, dirs[0]+string(filepath.Separator))
	}

	// Check that the directory is safe to open
	f, err := safeopen.OpenBeneath(base, dir)
	if errors.Is(err, fs.ErrNotExist) {
		return &directoryProvider{
			dataHome: id,
		}, nil
	} else if err != nil {
		return nil, err
	}

	if err = f.Close(); err != nil {
		return nil, err
	}

	return &directoryProvider{
		dataHome: id,
	}, nil
}

func (d *directoryProvider) Create() (string, error) {
	dir := uuid.NewString()
	return DirectoryProvider + "://" + filepath.Join(d.dataHome, dir), os.MkdirAll(filepath.Join(d.dataHome, dir), 0o755)
}

func (d *directoryProvider) Rm(_ context.Context, id string) error {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}

	// Check that the directory is safe to delete
	f, err := safeopen.OpenBeneath(d.dataHome, strings.TrimPrefix(id, d.dataHome))
	if err != nil {
		return nil
	}
	if err = f.Close(); err != nil {
		return err
	}

	return os.RemoveAll(id)
}

func (d *directoryProvider) DeleteFile(_ context.Context, file string) error {
	// Check that the file is safe to delete
	f, err := safeopen.OpenBeneath(d.dataHome, file)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return newNotFoundError(DirectoryProvider+"://"+d.dataHome, file)
	}
	if err = f.Close(); err != nil {
		return err
	}

	if err = os.Remove(filepath.Join(d.dataHome, file)); !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

func (d *directoryProvider) OpenFile(_ context.Context, file string) (io.ReadCloser, error) {
	f, err := safeopen.OpenBeneath(d.dataHome, file)
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

	file, err := safeopen.OpenFileBeneath(d.dataHome, fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	return err
}

func (d *directoryProvider) StatFile(_ context.Context, s string) (FileInfo, error) {
	f, err := safeopen.OpenBeneath(d.dataHome, s)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return FileInfo{}, newNotFoundError(DirectoryProvider+"://"+d.dataHome, s)
		}
		return FileInfo{}, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{
		WorkspaceID: DirectoryProvider + "://" + d.dataHome,
		Name:        stat.Name(),
		Size:        stat.Size(),
		ModTime:     stat.ModTime(),
	}, nil
}

func (d *directoryProvider) Ls(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" {
		// Ensure that the provided prefix is safe to open.
		file, err := safeopen.OpenBeneath(d.dataHome, prefix)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
	}

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

	// Check that the directory is safe to delete
	f, err := safeopen.OpenBeneath(d.dataHome, dirName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return nil
	}
	if err = f.Close(); err != nil {
		return err
	}

	return os.RemoveAll(fullDirName)
}
