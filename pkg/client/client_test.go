package client

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var c = New("", "")

func TestProviders(t *testing.T) {
	providers := c.Providers()

	providerCount := 0
	for p := range providers {
		if p != "directory" && p != "s3" {
			t.Errorf("invalid provider: %s", p)
		}

		providerCount++
	}

	if providerCount != 2 {
		t.Errorf("invalid provider count: %d", providerCount)
	}
}

func TestCreateAndRmDirectoryProvider(t *testing.T) {
	id, err := c.Create("directory")
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	if !strings.HasPrefix(id, "directory://") {
		t.Errorf("unexpected id: %s", id)
	}

	// Ensure the directory actually exists
	if _, err = os.Stat(strings.TrimPrefix(id, "directory://")); err != nil {
		t.Errorf("error when checking if directory exists: %v", err)
	}

	if err = c.Rm(id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// Ensure the directory actually exists
	if _, err = os.Stat(strings.TrimPrefix(id, "directory://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing workspace: %v", err)
	}
}

func TestCreateAndRmDirectoryProviderFromProvider(t *testing.T) {
	parentID, err := c.Create("directory")
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(parentID); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	file, err := c.WriteFile(parentID, "test.txt")
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

	id, err := c.Create("directory", parentID)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	// Ensure the file was copied over
	files, err := c.Ls(id)
	if err != nil {
		t.Errorf("unexpected error when listing files: %v", err)
	}

	if len(files) != 1 {
		t.Errorf("unexpected number of files: %d", len(files))
	}

	if files[0] != "test.txt" {
		t.Errorf("unexpected file: %s", files[0])
	}

	// Read the file to ensure it was copied over
	readFile, err := c.OpenFile(id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	content, err := io.ReadAll(readFile)
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	if string(content) != "test" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = readFile.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	if err = c.Rm(id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileDirectoryProvider(t *testing.T) {
	id, err := c.Create("directory")
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(id); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	file, err := c.WriteFile(id, "test.txt")
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
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(id, "directory://"), "test.txt")); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Read the file to ensure it was copied over
	readFile, err := c.OpenFile(id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	content, err := io.ReadAll(readFile)
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	if string(content) != "test" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = readFile.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	// Delete the file
	if err = c.DeleteFile(id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(id, "directory://"), "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}
