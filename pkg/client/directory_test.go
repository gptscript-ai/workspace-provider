package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var (
	directoryFactory   workspaceFactory
	s3Factory          workspaceFactory
	directoryTestingID string
	s3TestingID        string
	dirPrv             workspaceClient
	s3Prv              *s3Provider
	skipS3Tests        = os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET") == ""
)

func TestMain(m *testing.M) {
	var err error
	directoryFactory = newDirectory("")
	directoryTestingID, err = directoryFactory.Create()
	if err != nil {
		panic(err)
	}

	dirPrv = directoryFactory.New(directoryTestingID)

	if !skipS3Tests {
		s3Factory, _ = newS3(context.Background(), os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET"), os.Getenv("WORKSPACE_PROVIDER_S3_BASE_ENDPOINT"))
		// This won't ever error because it doesn't create anything.
		s3TestingID, _ = s3Factory.Create()

		s3Prv = s3Factory.New(s3TestingID).(*s3Provider)
	}

	exitCode := m.Run()

	var errs []error
	if err := directoryFactory.Rm(context.Background(), directoryTestingID); err != nil {
		errs = append(errs, fmt.Errorf("error removing directory workspace: %v", err))
	}

	if !skipS3Tests {
		if err := s3Factory.Rm(context.Background(), s3TestingID); err != nil {
			errs = append(errs, fmt.Errorf("error removing s3 workspace: %v", err))
		}
	}

	if err := errors.Join(errs...); err != nil {
		panic(err)
	}

	os.Exit(exitCode)
}

func TestCreateAndRm(t *testing.T) {
	id, err := directoryFactory.Create()
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	if !strings.HasPrefix(id, DirectoryProvider+"://") {
		t.Errorf("unexpected id: %s", id)
	}

	// The directory should exist
	if _, err := os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); err != nil {
		t.Errorf("unexpcted error when checking if directory exists: %v", err)
	}

	if err := directoryFactory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// The directory should no longer exist
	if _, err := os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unexpected error when checking if directory exists: %v", err)
	}
}

func TestWriteAndDeleteFileInDirectory(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	if _, err := os.Stat(filepath.Join(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"), "test.txt")); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Delete the file
	if err := dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := os.Stat(filepath.Join(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"), "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestWriteAndDeleteFileInDirectoryWithSubDir(t *testing.T) {
	filePath := filepath.Join("subdir", "test.txt")
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), filePath, strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	if _, err := os.Stat(filepath.Join(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"), filePath)); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Delete the file
	if err := dirPrv.DeleteFile(context.Background(), filePath); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := os.Stat(filepath.Join(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"), filePath)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Cleanup the directory
	if err := os.Remove(filepath.Join(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"), "subdir")); err != nil {
		t.Errorf("error when removing subdir directory: %v", err)
	}
}

func TestFileRead(t *testing.T) {
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	readFile, err := dirPrv.OpenFile(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	content, err := io.ReadAll(readFile)
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	if err = readFile.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	if string(content) != "test" {
		t.Errorf("unexpected content: %s", string(content))
	}

	// Delete the file
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestLs(t *testing.T) {
	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := dirPrv.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := dirPrv.Ls(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents) != 7 {
		t.Errorf("unexpected number of files: %d", len(contents))
	}

	sort.Strings(contents)
	if !reflect.DeepEqual(
		contents,
		[]string{
			"test0.txt",
			"test1.txt",
			"test2.txt",
			"test3.txt",
			"test4.txt",
			"test5.txt",
			"test6.txt",
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsWithSubDirs(t *testing.T) {
	defer func() {
		err := dirPrv.RemoveAllWithPrefix(context.Background(), "testDir")
		if err != nil {
			t.Errorf("unexpected error when deleting file %s: %v", "testDir", err)
		}
	}()

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir%s%s", string(os.PathSeparator), fileName)
		}
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := dirPrv.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := dirPrv.Ls(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents) != 7 {
		t.Errorf("unexpected number of children: %d", len(contents))
	}

	sort.Strings(contents)
	if !reflect.DeepEqual(
		contents,
		[]string{
			"test0.txt",
			"test1.txt",
			"test2.txt",
			filepath.Join("testDir", "test3.txt"),
			filepath.Join("testDir", "test4.txt"),
			filepath.Join("testDir", "test5.txt"),
			filepath.Join("testDir", "test6.txt"),
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsWithPrefix(t *testing.T) {
	defer func() {
		err := dirPrv.RemoveAllWithPrefix(context.Background(), "testDir")
		if err != nil {
			t.Errorf("unexpected error when deleting file %s: %v", "testDir", err)
		}
	}()

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir%s%s", string(os.PathSeparator), fileName)
		}
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := dirPrv.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := dirPrv.Ls(context.Background(), "testDir")
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents) != 4 {
		t.Errorf("unexpected number of contents: %d", len(contents))
	}

	sort.Strings(contents)
	if !reflect.DeepEqual(
		contents,
		[]string{
			filepath.Join("testDir", "test3.txt"),
			filepath.Join("testDir", "test4.txt"),
			filepath.Join("testDir", "test5.txt"),
			filepath.Join("testDir", "test6.txt"),
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestRemoveAllWithPrefix(t *testing.T) {
	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir%s%s", string(os.PathSeparator), fileName)
		}
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := dirPrv.DeleteFile(context.Background(), fileName)
			if fnf := (*NotFoundError)(nil); err != nil && !errors.As(err, &fnf) {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	err := dirPrv.RemoveAllWithPrefix(context.Background(), "testDir")
	if err != nil {
		t.Errorf("unexpected error when deleting all with prefix testDir: %v", err)
	}

	contents, err := dirPrv.Ls(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents) != 3 {
		t.Errorf("unexpected number of children: %d", len(contents))
	}

	sort.Strings(contents)
	if !reflect.DeepEqual(
		contents,
		[]string{
			"test0.txt",
			"test1.txt",
			"test2.txt",
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestOpeningFileDNENoError(t *testing.T) {
	var notFoundError *NotFoundError
	if file, err := dirPrv.OpenFile(context.Background(), "test.txt"); err == nil {
		_ = file.Close()
		t.Errorf("expected error when deleting file that doesn't exist")
	} else if !errors.As(err, &notFoundError) {
		t.Errorf("expected not found error when deleting file that doesn't exist")
	}
}
