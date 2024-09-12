package client

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var c = New()

func TestProviders(t *testing.T) {
	providers := c.Providers()

	for _, p := range providers {
		if p != DirectoryProvider && p != S3Provider {
			t.Errorf("invalid provider: %s", p)
		}
	}

	if len(providers) != 2 {
		t.Errorf("unexpected number of providers: %d", len(providers))
	}
}

func TestCreateAndRmDirectoryProvider(t *testing.T) {
	id, err := c.Create(context.Background(), DirectoryProvider)
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

	if err = c.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// Ensure the directory actually exists
	if _, err = os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing workspace: %v", err)
	}
}

func TestCreateAndRmDirectoryProviderFromProvider(t *testing.T) {
	parentID, err := c.Create(context.Background(), DirectoryProvider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(context.Background(), parentID); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	file, err := c.WriteFile(context.Background(), parentID, "test.txt")
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

	id, err := c.Create(context.Background(), DirectoryProvider, parentID)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	// Ensure the file was copied over
	files, err := c.Ls(context.Background(), id)
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
	readFile, err := c.OpenFile(context.Background(), id, "test.txt")
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

	if err = c.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileDirectoryProvider(t *testing.T) {
	id, err := c.Create(context.Background(), DirectoryProvider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(context.Background(), id); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	file, err := c.WriteFile(context.Background(), id, "test.txt")
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
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(id, DirectoryProvider+"://"), "test.txt")); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Read the file to ensure it was copied over
	readFile, err := c.OpenFile(context.Background(), id, "test.txt")
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
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(id, DirectoryProvider+"://"), "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestLsOptionsCompiledCorrectly(t *testing.T) {
	tests := []struct {
		name     string
		options  []LsOptions
		expected LsOptions
	}{
		{
			name:     "nothing provided",
			expected: LsOptions{},
		},
		{
			name:     "subdir provided",
			options:  []LsOptions{{SubDir: "test"}},
			expected: LsOptions{SubDir: "test"},
		},
		{
			name:     "last subdir used",
			options:  []LsOptions{{SubDir: "test"}, {SubDir: "test2"}},
			expected: LsOptions{SubDir: "test2"},
		},
		{
			name:     "non-recursive",
			options:  []LsOptions{{NonRecursive: true}},
			expected: LsOptions{NonRecursive: true},
		},
		{
			name: "non-recursive order doesn't matter",
			options: []LsOptions{
				{NonRecursive: true},
				{NonRecursive: false},
			},
			expected: LsOptions{NonRecursive: true},
		},
		{
			name:     "exclude hidden",
			options:  []LsOptions{{ExcludeHidden: true}},
			expected: LsOptions{ExcludeHidden: true},
		},
		{
			name: "exclude hidden order doesn't matter",
			options: []LsOptions{
				{ExcludeHidden: true},
				{ExcludeHidden: false},
			},
			expected: LsOptions{ExcludeHidden: true},
		},
		{
			name: "subdir and non-recursive taken from different entries",
			options: []LsOptions{
				{SubDir: "test", NonRecursive: true},
				{SubDir: "taken", NonRecursive: false, ExcludeHidden: true},
			},
			expected: LsOptions{SubDir: "taken", NonRecursive: true, ExcludeHidden: true},
		},
	}

	for _, test := range tests {
		c.factories["fake"] = &fake{expectedLsOptions: test.expected}
		_, err := c.Ls(context.Background(), "fake://", test.options...)
		if err != nil {
			t.Errorf("unexpected error for %q test: %v", test.name, err)
		}
		delete(c.factories, "fake")
	}

}

func TestWriteOptionsCompiledCorrectly(t *testing.T) {
	tests := []struct {
		name     string
		options  []WriteOptions
		expected WriteOptions
	}{
		{
			name:     "nothing provided",
			expected: WriteOptions{},
		},
		{
			name:     "WithoutCreate provided",
			options:  []WriteOptions{{WithoutCreate: true}},
			expected: WriteOptions{WithoutCreate: true},
		},
		{
			name:     "MustNotExist provided",
			options:  []WriteOptions{{MustNotExist: true}},
			expected: WriteOptions{MustNotExist: true},
		},
		{
			name:     "CreateDirs provided",
			options:  []WriteOptions{{CreateDirs: true}},
			expected: WriteOptions{CreateDirs: true},
		},
		{
			name: "order for bools doesn't matter",
			options: []WriteOptions{
				{CreateDirs: true},
				{WithoutCreate: true},
				{MustNotExist: true},
			},
			expected: WriteOptions{CreateDirs: true, WithoutCreate: true, MustNotExist: true},
		},
	}

	for _, test := range tests {
		c.factories["fake"] = &fake{expectedWriteOptions: test.expected}
		_, err := c.WriteFile(context.Background(), "fake://", "fake.txt", test.options...)
		if err != nil {
			t.Errorf("unexpected error for %q test: %v", test.name, err)
		}
		delete(c.factories, "fake")
	}
}
