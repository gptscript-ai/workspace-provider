package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/adrg/xdg"
	"github.com/gabriel-vasile/mimetype"
	"github.com/google/safeopen"
	"github.com/google/uuid"
)

func newDirectory(dataHome string) workspaceFactory {
	if dataHome == "" {
		dataHome = filepath.Join(xdg.DataHome, "workspace-provider")
	}
	return &directoryProvider{
		dataHome: dataHome,
		revisionsProvider: &directoryProvider{
			dataHome: filepath.Join(dataHome, revisionsDir),
		},
	}
}

type directoryProvider struct {
	dataHome          string
	revisionsProvider workspaceClient
}

func (d *directoryProvider) New(id string) (workspaceClient, error) {
	id = strings.TrimPrefix(id, DirectoryProvider+"://")
	if !filepath.IsAbs(id) {
		id = filepath.Join(d.dataHome, id)
	}

	if path.Base(id) == revisionsDir {
		return nil, errors.New("cannot create a workspace client for the revisions directory")
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
			revisionsProvider: &directoryProvider{
				dataHome: filepath.Join(d.dataHome, revisionsDir, dir),
			},
		}, nil
	} else if err != nil {
		return nil, err
	}

	if err = f.Close(); err != nil {
		return nil, err
	}

	return &directoryProvider{
		dataHome: id,
		revisionsProvider: &directoryProvider{
			dataHome: filepath.Join(d.dataHome, revisionsDir, dir),
		},
	}, nil
}

func (d *directoryProvider) Create() (string, error) {
	dir := uuid.NewString()
	return DirectoryProvider + "://" + filepath.Join(d.dataHome, dir), os.MkdirAll(filepath.Join(d.dataHome, dir), 0o755)
}

func (d *directoryProvider) Rm(ctx context.Context, id string) error {
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

	if d.revisionsProvider != nil {
		// Best effort
		_ = d.revisionsProvider.RemoveAllWithPrefix(ctx, strings.TrimPrefix(id, d.dataHome))
	}

	return os.RemoveAll(id)
}

func (d *directoryProvider) RevisionClient() workspaceClient {
	return d.revisionsProvider
}

func (d *directoryProvider) DeleteFile(ctx context.Context, file string) error {
	if err := d.deleteFile(file); err != nil {
		return err
	}

	if d.revisionsProvider == nil {
		return nil
	}

	info, err := getRevisionInfo(ctx, d.revisionsProvider, file)
	if err != nil {
		return err
	}

	for i := info.CurrentID; i > 0; i-- {
		// Best effort
		_ = deleteRevision(ctx, d.revisionsProvider, file, fmt.Sprintf("%d", i))
	}

	// Best effort
	_ = deleteRevisionInfo(ctx, d.revisionsProvider, file)

	return nil
}

func (d *directoryProvider) OpenFile(_ context.Context, file string) (io.ReadCloser, error) {
	return d.openFile(file)
}

func (d *directoryProvider) WriteFile(ctx context.Context, fileName string, reader io.Reader, opt WriteOptions) error {
	if d.revisionsProvider != nil && (opt.CreateRevision == nil || *opt.CreateRevision) {
		info, err := getRevisionInfo(ctx, d.revisionsProvider, fileName)
		if err != nil {
			if nfe := (*NotFoundError)(nil); !errors.As(err, &nfe) {
				return err
			}
		}

		info.CurrentID++
		if err = writeRevision(ctx, d.revisionsProvider, d, fileName, info); err != nil {
			if nfe := (*NotFoundError)(nil); !errors.As(err, &nfe) {
				return fmt.Errorf("failed to write revision: %w", err)
			}
		}

		if err = writeRevisionInfo(ctx, d.revisionsProvider, fileName, info); err != nil {
			return fmt.Errorf("failed to write revision info: %w", err)
		}
	}

	return d.writeFile(fileName, reader)
}

func (d *directoryProvider) StatFile(_ context.Context, s string) (FileInfo, error) {
	return d.statFile(s)
}

func (d *directoryProvider) Ls(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" {
		// Ensure that the provided prefix is safe to open.
		file, err := safeopen.OpenBeneath(d.dataHome, strings.TrimSuffix(prefix, "/"))
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

func (d *directoryProvider) ListRevisions(ctx context.Context, fileName string) ([]RevisionInfo, error) {
	return listRevisions(ctx, d.revisionsProvider, fmt.Sprintf("%s://%s", DirectoryProvider, d.dataHome), fileName)
}

func (d *directoryProvider) GetRevision(ctx context.Context, fileName, revisionID string) (io.ReadCloser, error) {
	return getRevision(ctx, d.revisionsProvider, fileName, revisionID)
}

func (d *directoryProvider) DeleteRevision(ctx context.Context, fileName, revisionID string) error {
	return deleteRevision(ctx, d.revisionsProvider, fileName, revisionID)
}

func (d *directoryProvider) openFile(fileName string) (io.ReadCloser, error) {
	f, err := safeopen.OpenBeneath(d.dataHome, fileName)
	if os.IsNotExist(err) {
		return nil, newNotFoundError(DirectoryProvider+"://"+d.dataHome, fileName)
	}

	return f, err
}

func (d *directoryProvider) writeFile(fileName string, reader io.Reader) error {
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

func (d *directoryProvider) deleteFile(fileName string) error {
	f, err := safeopen.OpenBeneath(d.dataHome, fileName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	if err = os.Remove(filepath.Join(d.dataHome, fileName)); !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

func (d *directoryProvider) statFile(s string) (FileInfo, error) {
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

	// Get Mimetype
	mt, err := mimetype.DetectReader(f)
	if err != nil {
		return FileInfo{}, err
	}
	mime := strings.Split(mt.String(), ";")[0]

	return FileInfo{
		WorkspaceID: DirectoryProvider + "://" + d.dataHome,
		Name:        stat.Name(),
		Size:        stat.Size(),
		ModTime:     stat.ModTime(),
		MimeType:    mime,
	}, nil
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
