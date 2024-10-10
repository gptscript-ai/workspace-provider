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
	testingWorkspaceID string
	directoryProvider  workspaceClient
)

func TestMain(m *testing.M) {
	var err error

	directoryFactory = newDirectory("")
	testingWorkspaceID, err = directoryFactory.Create(context.Background())
	if err != nil {
		panic(err)
	}

	directoryProvider = directoryFactory.New(context.Background(), testingWorkspaceID)

	exitCode := m.Run()

	if err = directoryFactory.Rm(context.Background(), testingWorkspaceID); err != nil {
		panic(err)
	}

	os.Exit(exitCode)
}

func TestCreateAndRm(t *testing.T) {
	id, err := directoryFactory.Create(context.Background())
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

	if err = directoryFactory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// Ensure the directory actually exists
	if _, err = os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileInDirectory(t *testing.T) {
	// Copy a file into the workspace
	file, err := directoryProvider.WriteFile(context.Background(), "test.txt", WriteOptions{})
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
	if err = directoryProvider.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestWriteAndDeleteFileInDirectoryWithSubDir(t *testing.T) {
	filePath := filepath.Join("subdir", "test.txt")
	// Copy a file into the workspace
	file, err := directoryProvider.WriteFile(context.Background(), filePath, WriteOptions{CreateDirs: true})
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
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), filePath)); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Delete the file
	if err = directoryProvider.DeleteFile(context.Background(), "subdir/test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), filePath)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Cleanup the directory
	if err = os.Remove(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "subdir")); err != nil {
		t.Errorf("error when removing subdir directory: %v", err)
	}
}

func TestWriteFailsIfCreateDirsFalse(t *testing.T) {
	filePath := filepath.Join("subdir", "test.txt")
	_, err := directoryProvider.WriteFile(context.Background(), filePath, WriteOptions{})
	if err == nil {
		t.Errorf("expected error if creating dirs is false")
	}
}

func TestWriteFailsWithoutCreate(t *testing.T) {
	_, err := directoryProvider.WriteFile(context.Background(), "test.txt", WriteOptions{WithoutCreate: true})
	if err == nil {
		t.Errorf("expected error if file doesn't exist and using WithoutCreate")
	}
}

func TestWriteMustNotExist(t *testing.T) {
	// Copy a file into the workspace
	file, err := directoryProvider.WriteFile(context.Background(), "test.txt", WriteOptions{MustNotExist: true})
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

	// Copy a file into the workspace
	file, err = directoryProvider.WriteFile(context.Background(), "test.txt", WriteOptions{MustNotExist: true})
	if err == nil {
		file.Close()
		t.Errorf("expected error if file already exists and using MustNotExist")
	}

	// Delete the file
	if err = directoryProvider.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestFileRead(t *testing.T) {
	writeFile, err := directoryProvider.WriteFile(context.Background(), "test.txt", WriteOptions{})
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

	readFile, err := directoryProvider.OpenFile(context.Background(), "test.txt")
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
	if err = directoryProvider.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestLs(t *testing.T) {
	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		writeFile, err := directoryProvider.WriteFile(context.Background(), fileName, WriteOptions{})
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := directoryProvider.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := directoryProvider.Ls(context.Background(), LsOptions{})
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents.Children) != 7 {
		t.Errorf("unexpected number of files: %d", len(contents.Children))
	}

	sort.Slice(contents.Children, func(i, j int) bool {
		return contents.Children[i].FileName < contents.Children[j].FileName
	})
	if !reflect.DeepEqual(
		contents.Children,
		[]WorkspaceContent{
			{FileName: "test0.txt"},
			{FileName: "test1.txt"},
			{FileName: "test2.txt"},
			{FileName: "test3.txt"},
			{FileName: "test4.txt"},
			{FileName: "test5.txt"},
			{FileName: "test6.txt"},
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsWithSubDirs(t *testing.T) {
	defer func() {
		err := directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{})
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
		writeFile, err := directoryProvider.WriteFile(context.Background(), fileName, WriteOptions{CreateDirs: true})
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := directoryProvider.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := directoryProvider.Ls(context.Background(), LsOptions{})
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents.Children) != 4 {
		t.Errorf("unexpected number of children: %d", len(contents.Children))
	}

	sort.Slice(contents.Children, func(i, j int) bool {
		return contents.Children[i].FileName < contents.Children[j].FileName
	})
	if !reflect.DeepEqual(
		contents.Children,
		[]WorkspaceContent{
			{
				Path: "testDir",
				Children: []WorkspaceContent{
					{FileName: "test3.txt"},
					{FileName: "test4.txt"},
					{FileName: "test5.txt"},
					{FileName: "test6.txt"},
				},
			},
			{FileName: "test0.txt"},
			{FileName: "test1.txt"},
			{FileName: "test2.txt"},
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsWithSubDirsNoRecursive(t *testing.T) {
	defer func() {
		err := directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{})
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
		writeFile, err := directoryProvider.WriteFile(context.Background(), fileName, WriteOptions{CreateDirs: true})
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := directoryProvider.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := directoryProvider.Ls(context.Background(), LsOptions{NonRecursive: true})
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents.Children) != 3 {
		t.Errorf("unexpected number of files: %d", len(contents.Children))
	}

	sort.Slice(contents.Children, func(i, j int) bool {
		return contents.Children[i].FileName < contents.Children[j].FileName
	})
	if !reflect.DeepEqual(
		contents.Children,
		[]WorkspaceContent{
			{FileName: "test0.txt"},
			{FileName: "test1.txt"},
			{FileName: "test2.txt"},
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsFromSubDir(t *testing.T) {
	defer func() {
		err := directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{})
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
		writeFile, err := directoryProvider.WriteFile(context.Background(), fileName, WriteOptions{CreateDirs: true})
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := directoryProvider.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := directoryProvider.Ls(context.Background(), LsOptions{SubDir: "testDir"})
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents.Children) != 4 {
		t.Errorf("unexpected number of contents: %d", len(contents.Children))
	}

	sort.Slice(contents.Children, func(i, j int) bool {
		return contents.Children[i].FileName < contents.Children[j].FileName
	})
	if !reflect.DeepEqual(
		contents,
		WorkspaceContent{
			ID:   testingWorkspaceID,
			Path: "testDir",
			Children: []WorkspaceContent{
				{FileName: "test3.txt"},
				{FileName: "test4.txt"},
				{FileName: "test5.txt"},
				{FileName: "test6.txt"},
			},
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsWithSubDirsWithHiddenFiles(t *testing.T) {
	defer func() {
		err := directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{})
		if err != nil {
			t.Errorf("unexpected error when deleting file %s: %v", "testDir", err)
		}
	}()

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i%2 == 0 {
			fileName = "." + fileName
		}
		if i >= 3 {
			fileName = fmt.Sprintf("testDir%s%s", string(os.PathSeparator), fileName)
		}
		writeFile, err := directoryProvider.WriteFile(context.Background(), fileName, WriteOptions{CreateDirs: true})
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := directoryProvider.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := directoryProvider.Ls(context.Background(), LsOptions{})
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents.Children) != 4 {
		t.Errorf("unexpected number of contents: %d", len(contents.Children))
	}

	sort.Slice(contents.Children, func(i, j int) bool {
		return contents.Children[i].FileName < contents.Children[j].FileName
	})
	if !reflect.DeepEqual(
		contents.Children,
		[]WorkspaceContent{
			{
				Path: "testDir",
				Children: []WorkspaceContent{
					{FileName: ".test4.txt"},
					{FileName: ".test6.txt"},
					{FileName: "test3.txt"},
					{FileName: "test5.txt"},
				},
			},
			{FileName: ".test0.txt"},
			{FileName: ".test2.txt"},
			{FileName: "test1.txt"},
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestLsWithSubDirsExcludeHiddenFiles(t *testing.T) {
	defer func() {
		err := directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{})
		if err != nil {
			t.Errorf("unexpected error when deleting file %s: %v", "testDir", err)
		}
	}()

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i%2 == 0 {
			fileName = "." + fileName
		}
		if i >= 3 {
			fileName = fmt.Sprintf("testDir%s%s", string(os.PathSeparator), fileName)
		}
		writeFile, err := directoryProvider.WriteFile(context.Background(), fileName, WriteOptions{CreateDirs: true})
		if err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		if err = writeFile.Close(); err != nil {
			t.Errorf("error closing file: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := directoryProvider.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := directoryProvider.Ls(context.Background(), LsOptions{ExcludeHidden: true})
	if err != nil {
		t.Fatalf("unexpected error when listing files: %v", err)
	}

	if len(contents.Children) != 2 {
		t.Errorf("unexpected number of contents: %d", len(contents.Children))
	}

	sort.Slice(contents.Children, func(i, j int) bool {
		return contents.Children[i].FileName < contents.Children[j].FileName
	})
	if !reflect.DeepEqual(
		contents.Children,
		[]WorkspaceContent{
			{
				Path: "testDir",
				Children: []WorkspaceContent{
					{FileName: "test3.txt"},
					{FileName: "test5.txt"},
				},
			},
			{FileName: "test1.txt"},
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestMkDirRmDir(t *testing.T) {
	err := directoryProvider.MkDir(context.Background(), "testDir", MkDirOptions{})
	if err != nil {
		t.Fatalf("unexpected error when creating directory: %v", err)
	}

	// Ensure the directory is actually created
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "testDir"))
	if err != nil {
		t.Errorf("error when checking if directory exists: %v", err)
	}

	// Creating the directory with MustNotExist should fail
	err = directoryProvider.MkDir(context.Background(), "testDir", MkDirOptions{MustNotExist: true})
	var dae *DirectoryAlreadyExistsError
	if !errors.As(err, &dae) {
		t.Fatalf("unexpected error when creating directory: %v", err)
	}

	writeFile, err := directoryProvider.WriteFile(context.Background(), filepath.Join("testDir", "test.txt"), WriteOptions{})
	if err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	if err = writeFile.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	err = directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{NonEmpty: true})
	var dne *DirectoryNotEmptyError
	if !errors.As(err, &dne) {
		t.Errorf("unexpected error when removing directory: %v", err)
	}

	err = directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{})
	if err != nil {
		t.Errorf("unexpected error when removing directory: %v", err)
	}

	// Ensure the directory is actually deleted
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "testDir"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing directory: %v", err)
	}
}

func TestMkDirRmDirWhenEmpty(t *testing.T) {
	err := directoryProvider.MkDir(context.Background(), "testDir", MkDirOptions{})
	if err != nil {
		t.Fatalf("unexpected error when creating directory: %v", err)
	}

	// Ensure the directory is actually created
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "testDir"))
	if err != nil {
		t.Errorf("error when checking if directory exists: %v", err)
	}

	err = directoryProvider.RmDir(context.Background(), "testDir", RmDirOptions{NonEmpty: true})
	if err != nil {
		t.Errorf("unexpected error when removing directory: %v", err)
	}

	// Ensure the directory is actually deleted
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "testDir"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing directory: %v", err)
	}
}

func TestMkDirCreateDirs(t *testing.T) {
	testDir := filepath.Join("test", "testDir")
	err := directoryProvider.MkDir(context.Background(), testDir, MkDirOptions{})
	if err == nil {
		t.Fatalf("expected error when creating nested directories")
	}

	// Ensure the directory is actually created
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "testDir"))
	if err == nil {
		t.Errorf("directory should not exist after creating nested directories")
	}

	err = directoryProvider.MkDir(context.Background(), testDir, MkDirOptions{CreateDirs: true})
	if err != nil {
		t.Fatalf("unexpected error when creating nested directories: %v", err)
	}

	// Ensure the directory is actually created
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), testDir))
	if err != nil {
		t.Errorf("unexpected error when checking nested directories: %v", err)
	}

	err = directoryProvider.RmDir(context.Background(), testDir, RmDirOptions{})
	if err != nil {
		t.Errorf("unexpected error when removing directory: %v", err)
	}

	// Ensure the directory is actually deleted
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), testDir))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing directory: %v", err)
	}

	err = directoryProvider.RmDir(context.Background(), "test", RmDirOptions{})
	if err != nil {
		t.Errorf("unexpected error when removing directory: %v", err)
	}

	// Ensure the directory is actually deleted
	_, err = os.Stat(filepath.Join(strings.TrimPrefix(testingWorkspaceID, DirectoryProvider+"://"), "test"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("directory should not exist after removing directory: %v", err)
	}
}
