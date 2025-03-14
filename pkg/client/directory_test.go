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

type s3TestSetup struct {
	name      string
	factory   workspaceFactory
	testingID string
	provider  *s3Provider
}

var (
	directoryFactory   workspaceFactory
	directoryTestingID string
	dirPrv             workspaceClient
	s3TestSetups       []s3TestSetup
	skipS3Tests        = os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET") == ""
	azureFactory       workspaceFactory
	azureTestingID     string
	azurePrv           *azureProvider
	skipAzureTests     = os.Getenv("WORKSPACE_PROVIDER_AZURE_CONNECTION_STRING") == "" || os.Getenv("WORKSPACE_PROVIDER_AZURE_CONTAINER") == ""
)

func TestMain(m *testing.M) {
	directoryFactory = newDirectory("")
	directoryTestingID = directoryFactory.Create()
	dirPrv, _ = directoryFactory.New(directoryTestingID)

	if !skipS3Tests {
		if os.Getenv("WORKSPACE_PROVIDER_S3_USE_PATH_STYLE") != "true" {
			s3Factory, _ := newS3(context.Background(), os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET"), os.Getenv("WORKSPACE_PROVIDER_S3_BASE_ENDPOINT"), false)
			// This won't ever error because it doesn't create anything.
			s3TestingID := s3Factory.Create()

			s3Client, _ := s3Factory.New(s3TestingID)
			s3Prv := s3Client.(*s3Provider)
			s3TestSetups = append(s3TestSetups, s3TestSetup{
				name:      "default",
				factory:   s3Factory,
				testingID: s3TestingID,
				provider:  s3Prv,
			})
		}

		s3PathStyleFactory, _ := newS3(context.Background(), os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET"), os.Getenv("WORKSPACE_PROVIDER_S3_BASE_ENDPOINT"), true)
		s3PathStyleTestingID := s3PathStyleFactory.Create()
		s3PathStyleClient, _ := s3PathStyleFactory.New(s3PathStyleTestingID)
		s3TestSetups = append(s3TestSetups, s3TestSetup{
			name:      "use-path-style",
			factory:   s3PathStyleFactory,
			testingID: s3PathStyleTestingID,
			provider:  s3PathStyleClient.(*s3Provider),
		})
	}

	if !skipAzureTests {
		azureFactory, _ = newAzure(os.Getenv("WORKSPACE_PROVIDER_AZURE_CONTAINER"), os.Getenv("WORKSPACE_PROVIDER_AZURE_CONNECTION_STRING"))
		// This won't ever error because it doesn't create anything.
		azureTestingID = azureFactory.Create()

		azureClient, _ := azureFactory.New(azureTestingID)
		azurePrv = azureClient.(*azureProvider)
	}

	exitCode := m.Run()

	var errs []error
	if err := directoryFactory.Rm(context.Background(), directoryTestingID); err != nil {
		errs = append(errs, fmt.Errorf("error removing directory workspace: %v", err))
	}

	if !skipS3Tests {
		for _, s3TS := range s3TestSetups {
			if err := s3TS.factory.Rm(context.Background(), s3TS.testingID); err != nil {
				errs = append(errs, fmt.Errorf("error removing s3 workspace: %v", err))
			}
		}
	}

	if !skipAzureTests {
		if err := azureFactory.Rm(context.Background(), azureTestingID); err != nil {
			errs = append(errs, fmt.Errorf("error removing azure workspace: %v", err))
		}
	}

	if err := errors.Join(errs...); err != nil {
		panic(err)
	}

	os.Exit(exitCode)
}

func TestCreateAndRm(t *testing.T) {
	id := directoryFactory.Create()
	if !strings.HasPrefix(id, DirectoryProvider+"://") {
		t.Errorf("unexpected id: %s", id)
	}

	// The directory should not exist yet
	if _, err := os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unexpected error when checking if directory exists: %v", err)
	}

	if err := directoryFactory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// The directory should no longer exist
	if _, err := os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unexpected error when checking if directory exists: %v", err)
	}
}

func TestWriteFileWorkspaceDNE(t *testing.T) {
	id := directoryFactory.Create()

	// Delete the directory
	if err := os.RemoveAll(strings.TrimPrefix(id, DirectoryProvider+"://")); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	dne, err := directoryFactory.New(id)
	if err != nil {
		t.Fatalf("error creating workspace: %v", err)
	}

	if err = dne.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Errorf("unexpected error when writing file: %v", err)
	}

	if err = directoryFactory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestEnsureCannotCreateUnsafeWorkspace(t *testing.T) {
	id := directoryFactory.Create()

	_, err := directoryFactory.New(id + "/..")
	if err == nil {
		t.Fatalf("expected error when creating directory outside of workspace")
	}

	if err = directoryFactory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestEnsureCannotWriteReadUnsafeFile(t *testing.T) {
	var pathErr *os.PathError
	if err := dirPrv.WriteFile(context.Background(), "../test.txt", strings.NewReader("test"), WriteOptions{}); err == nil || !errors.As(err, &pathErr) || pathErr.Op != "OpenBeneath" {
		t.Errorf("unexpected error getting file to write: %v", err)
	}

	pathErr = nil
	if _, err := dirPrv.OpenFile(context.Background(), "../test.txt", OpenOptions{}); err == nil || !errors.As(err, &pathErr) || pathErr.Op != "OpenBeneath" {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestWriteAndDeleteFileInDirectory(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	info, err := os.Stat(filepath.Join(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"), "test.txt"))
	if err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Stat the file and compare with the original
	providerStat, err := dirPrv.StatFile(context.Background(), "test.txt", StatOptions{})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if providerStat.WorkspaceID != directoryTestingID {
		t.Errorf("unexpected workspace id: %s", providerStat.WorkspaceID)
	}
	if providerStat.Size != info.Size() {
		t.Errorf("unexpected file size: %d", providerStat.Size)
	}
	if providerStat.Name != info.Name() {
		t.Errorf("unexpected file name: %s", providerStat.Name)
	}
	if providerStat.ModTime.Compare(info.ModTime()) != 0 {
		t.Errorf("unexpected file mod time: %s", providerStat.ModTime)
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
	if err := dirPrv.WriteFile(context.Background(), filePath, strings.NewReader("test"), WriteOptions{}); err != nil {
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
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	readFile, err := dirPrv.OpenFile(context.Background(), "test.txt", OpenOptions{})
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

	// Deleting the file again should not throw an error
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestLs(t *testing.T) {
	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
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
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
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
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
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

	for _, input := range []string{
		"testDir",
		"testDir/",
	} {
		contents, err := dirPrv.Ls(context.Background(), input)
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
}

func TestRemoveAllWithPrefix(t *testing.T) {
	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir%s%s", string(os.PathSeparator), fileName)
		}
		if err := dirPrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
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
	if file, err := dirPrv.OpenFile(context.Background(), "test.txt", OpenOptions{}); err == nil {
		_ = file.Close()
		t.Errorf("expected error when deleting file that doesn't exist")
	} else if !errors.As(err, &notFoundError) {
		t.Errorf("expected not found error when deleting file that doesn't exist")
	}
}

func TestWriteEnsureRevision(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	dir, base := filepath.Split(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"))

	info, err := os.Stat(filepath.Join(dir, revisionsDir, base, "test.txt.1"))
	if err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Now there should be one revision
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	} else {
		if revisions[0].WorkspaceID != directoryTestingID {
			t.Errorf("unexpected workspace id: %s", revisions[0].WorkspaceID)
		}
		if revisions[0].Size != info.Size() {
			t.Errorf("unexpected file size: %d", revisions[0].Size)
		}
		if revisions[0].Name != "test.txt" {
			t.Errorf("unexpected file name: %s", revisions[0].Name)
		}
		if revisions[0].ModTime.Compare(info.ModTime()) != 0 {
			t.Errorf("unexpected file mod time: %s", revisions[0].ModTime)
		}

		if revisions[0].RevisionID != "1" {
			t.Errorf("unexpected revision id: %s", revisions[0].RevisionID)
		}

		// Get the revision and ensure that it has the correct content.
		rev, err := dirPrv.GetRevision(context.Background(), "test.txt", revisions[0].RevisionID)
		if err != nil {
			t.Errorf("unexpected error when getting revision: %v", err)
		} else {
			defer rev.Close()
		}

		content, err := io.ReadAll(rev)
		if err != nil {
			t.Errorf("unexpected error when reading revision: %v", err)
		}

		if string(content) != "test" {
			t.Errorf("unexpected content: %s", string(content))
		}

		revisionID, err := rev.GetRevisionID()
		if err != nil {
			t.Errorf("error getting revision: %v", err)
		}
		if revisionID != "1" {
			t.Errorf("unexpected revision ID: %s", revisionID)
		}
	}

	// Delete the file
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(dir, base, "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Ensure the revision file no longer exists
	if _, err = os.Stat(filepath.Join(dir, revisionsDir, base, "test.txt.1")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Ensure the API returns no revisions for the file
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}
}

func TestWriteEnsureNoRevision(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{CreateRevision: new(bool)}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should still be no revision
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the file
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestWriteEnsureConflict(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	ce := (*ConflictError)(nil)
	// Trying to update the file with a non-zero revision ID should fail with a conflict error.
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "1"}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when first updating file non-zero revision ID: %v", err)
	}

	// Also, using -1 for the revision ID should also fail because that is the same as "only write if the file doesn't exist"
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "-1"}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when first updating file non-zero revision ID: %v", err)
	}

	// Update the file
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be one revision
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file again
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test3"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be two revisions
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	ce = (*ConflictError)(nil)
	// Trying to update the file again with the same revision ID should fail with a conflict error.
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test4"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when updating file with same revision ID: %v", err)
	}

	latestRevisionID := revisions[1].RevisionID
	// Delete the most recent revision
	if err = dirPrv.DeleteRevision(context.Background(), "test.txt", latestRevisionID); err != nil {
		t.Errorf("error deleting revision: %v", err)
	}

	// Now there should be one revision
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// We cannot update the file with this revision ID
	ce = (*ConflictError)(nil)
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test5"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when updating file with zero revision ID: %v", err)
	}

	// Ensure that we can still create a new revision
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test5"), WriteOptions{LatestRevisionID: latestRevisionID}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Delete the file
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("error removing file: %v", err)
	}
}

func TestReadFileWithRevision(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Read the file
	f, err := dirPrv.OpenFile(context.Background(), "test.txt", OpenOptions{WithLatestRevisionID: true})
	if err != nil {
		t.Errorf("error reading file: %v", err)
	}

	// Read the file contents
	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file contents: %v", err)
	}

	// Close the file
	if err := f.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	if string(data) != "test" {
		t.Errorf("unexpected file contents: %s", string(data))
	}

	// Ensure that the revision is set and correct.
	revisionID, err := f.GetRevisionID()
	if err != nil {
		t.Errorf("error getting revision: %v", err)
	}
	if revisionID != "0" {
		t.Errorf("unexpected revision ID: %s", revisionID)
	}

	// Update the file
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "0"}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Read the file
	f, err = dirPrv.OpenFile(context.Background(), "test.txt", OpenOptions{WithLatestRevisionID: true})
	if err != nil {
		t.Errorf("error reading file: %v", err)
	}

	// Read the file contents
	data, err = io.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file contents: %v", err)
	}

	// Close the file
	if err := f.Close(); err != nil {
		t.Errorf("error closing file: %v", err)
	}

	if string(data) != "test2" {
		t.Errorf("unexpected file contents: %s", string(data))
	}

	// Get the revision ID
	revisionID, err = f.GetRevisionID()
	if err != nil {
		t.Errorf("error getting revision: %v", err)
	}
	if revisionID != "1" {
		t.Errorf("unexpected revision ID: %s", revisionID)
	}

	// Delete the file
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("error removing file: %v", err)
	}
}

func TestDeleteRevision(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	dir, base := filepath.Split(strings.TrimPrefix(directoryTestingID, DirectoryProvider+"://"))

	// Now there should be one revision
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test3"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be two revisions
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the first revision
	if err = dirPrv.DeleteRevision(context.Background(), "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// Now there should be one revision
	revisions, err = dirPrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 || revisions[0].RevisionID != "2" {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Deleting the revision again should not produce an error.
	if err = dirPrv.DeleteRevision(context.Background(), "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// Delete the file
	if err = dirPrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = os.Stat(filepath.Join(dir, base, "test.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Ensure the revision file no longer exists
	if _, err = os.Stat(filepath.Join(dir, revisionsDir, base, "test.txt.2")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestNoCreateRevisionsClient(t *testing.T) {
	_, err := directoryFactory.New(fmt.Sprintf("%s://revisions", directoryTestingID))
	if err == nil {
		t.Errorf("expected error when creating client for revisions dir")
	}
}

func TestStatFile(t *testing.T) {
	// Copy a file into the workspace
	if err := dirPrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Stat the file
	providerStat, err := dirPrv.StatFile(context.Background(), "test.txt", StatOptions{})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if providerStat.WorkspaceID != directoryTestingID {
		t.Errorf("unexpected workspace id: %s", providerStat.WorkspaceID)
	}
	if providerStat.Size != 4 {
		t.Errorf("unexpected file size: %d", providerStat.Size)
	}
	if providerStat.Name != "test.txt" {
		t.Errorf("unexpected file name: %s", providerStat.Name)
	}
	if providerStat.ModTime.IsZero() {
		t.Errorf("unexpected file mod time: %s", providerStat.ModTime)
	}
	if _, err := providerStat.GetRevisionID(); !errors.Is(err, RevisionNotRequestedError) {
		t.Errorf("unexpected error when revision not requested: %v", err)
	}

	// Stat the file with revision
	providerStat, err = dirPrv.StatFile(context.Background(), "test.txt", StatOptions{WithLatestRevisionID: true})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if rev, err := providerStat.GetRevisionID(); err != nil {
		t.Errorf("unexpected error when revision not requested: %v", err)
	} else if rev != "0" {
		t.Errorf("unexpected revision id when revision requested: %s", rev)
	}
}
