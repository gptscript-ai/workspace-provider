package client

import (
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
	testingWorkspaceID string
	directoryProvider  workspaceClient
)

func TestMain(m *testing.M) {
	var err error

	directoryFactory = newDirectory("")
	testingWorkspaceID, err = directoryFactory.Create()
	if err != nil {
		panic(err)
	}

	directoryProvider = directoryFactory.New(testingWorkspaceID)

	exitCode := m.Run()

	if err = directoryFactory.Rm(testingWorkspaceID); err != nil {
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

	// Ensure the directory actually exists
	if _, err = os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); err != nil {
		t.Errorf("error when checking if directory exists: %v", err)
	}

	if err = directoryFactory.Rm(id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// Ensure the directory actually exists
	if _, err = os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileInDirectory(t *testing.T) {
	// Copy a file into the workspace
	file, err := directoryProvider.WriteFile("test.txt")
	if err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	if _, err = file.Write([]byte("test")); err != nil {
		file.Close()
		t.Fatalf("error writing file: %v", err)
	}

	if err = file.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	// Ensure the file actually exists
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "test.txt")); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Delete the file
	if err = directoryProvider.DeleteFile("test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestFileRead(t *testing.T) {
	writeFile, err := directoryProvider.WriteFile("test.txt")
	if err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	if _, err = writeFile.Write([]byte("test")); err != nil {
		writeFile.Close()
		t.Fatalf("error writing file: %v", err)
	}

	if err = writeFile.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	readFile, err := directoryProvider.OpenFile("test.txt")
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
	if err = directoryProvider.DeleteFile("test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestLs(t *testing.T) {
	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		writeFile, err := directoryProvider.WriteFile(fileName)
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func(directoryProvider workspaceClient, s string) {
			err := directoryProvider.DeleteFile(s)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}(directoryProvider, fileName)
	}

	contents, err := directoryProvider.Ls()
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents) != 7 {
		t.Errorf("unexpected number of files: %d", len(contents))
	}

	sort.Strings(contents)
	if !reflect.DeepEqual(contents, []string{"test0.txt", "test1.txt", "test2.txt", "test3.txt", "test4.txt", "test5.txt", "test6.txt"}) {
		t.Errorf("unexpected contents: %v", contents)
	}
}
