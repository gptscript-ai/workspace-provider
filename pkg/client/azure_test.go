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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

func TestCreateAndRmAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	id := azureFactory.Create()
	if !strings.HasPrefix(id, AzureProvider+"://") {
		t.Errorf("unexpected id: %s", id)
	}

	container, dir, _ := strings.Cut(strings.TrimPrefix(id, AzureProvider+"://"), "/")
	testAzureProvider := &azureProvider{
		containerName: container,
		client:        azurePrv.client,
	}

	// Nothing should be created
	blobClient := testAzureProvider.client.ServiceClient().NewContainerClient(container).NewBlockBlobClient(dir)
	if _, err := blobClient.GetProperties(context.Background(), nil); err == nil {
		t.Errorf("expected error when checking if workspace exists")
	} else {
		var storageErr *azcore.ResponseError
		if !errors.As(err, &storageErr) || storageErr.StatusCode != 404 {
			t.Errorf("unexpected error when checking if workspace exists: %v", err)
		}
	}

	if err := azureFactory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileInAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	blobClient := azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", azurePrv.dir, "test.txt"))
	props, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Stat the file and compare with the original
	providerStat, err := azurePrv.StatFile(context.Background(), "test.txt", StatOptions{})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if providerStat.WorkspaceID != azureTestingID {
		t.Errorf("unexpected workspace id: %s", providerStat.WorkspaceID)
	}
	if providerStat.Size != *props.ContentLength {
		t.Errorf("unexpected file size: %d", providerStat.Size)
	}
	if providerStat.Name != "test.txt" {
		t.Errorf("unexpected file name: %s", providerStat.Name)
	}
	if providerStat.ModTime.Compare(*props.LastModified) != 0 {
		t.Errorf("unexpected file mod time: %s", providerStat.ModTime)
	}

	// Delete the file
	if err := azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := blobClient.GetProperties(context.Background(), nil); err == nil {
		t.Errorf("file should not exist after deleting")
	}
}

func TestWriteAndDeleteFileInAzureWithSubDir(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	filePath := filepath.Join("subdir", "test.txt")
	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), filePath, strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	blobClient := azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", azurePrv.dir, filePath))
	if _, err := blobClient.GetProperties(context.Background(), nil); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Delete the file
	if err := azurePrv.DeleteFile(context.Background(), filePath); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := blobClient.GetProperties(context.Background(), nil); err == nil {
		t.Errorf("file should not exist after deleting")
	}
}

func TestFileReadFromAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	readFile, err := azurePrv.OpenFile(context.Background(), "test.txt", OpenOptions{})
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
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Deleting the file again should not throw an error
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestLsAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if err := azurePrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func(name string) {
			err := azurePrv.DeleteFile(context.Background(), name)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", name, err)
			}
		}(fileName)
	}

	contents, err := azurePrv.Ls(context.Background(), "")
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

func TestLsWithSubDirsAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	defer func() {
		err := azurePrv.RemoveAllWithPrefix(context.Background(), "testDir")
		if err != nil {
			t.Errorf("unexpected error when deleting file %s: %v", "testDir", err)
		}
	}()

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir/%s", fileName)
		}
		if err := azurePrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func(name string) {
			err := azurePrv.DeleteFile(context.Background(), name)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", name, err)
			}
		}(fileName)
	}

	contents, err := azurePrv.Ls(context.Background(), "")
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

func TestLsWithPrefixAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	defer func() {
		err := azurePrv.RemoveAllWithPrefix(context.Background(), "testDir")
		if err != nil {
			t.Errorf("unexpected error when deleting file %s: %v", "testDir", err)
		}
	}()

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir/%s", fileName)
		}
		if err := azurePrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func(name string) {
			err := azurePrv.DeleteFile(context.Background(), name)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", name, err)
			}
		}(fileName)
	}

	contents, err := azurePrv.Ls(context.Background(), "testDir")
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

func TestRemoveAllWithPrefixAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir/%s", fileName)
		}
		if err := azurePrv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func(name string) {
			err := azurePrv.DeleteFile(context.Background(), name)
			if fnf := (*NotFoundError)(nil); err != nil && !errors.As(err, &fnf) {
				t.Errorf("unexpected error when deleting file %s: %v", name, err)
			}
		}(fileName)
	}

	err := azurePrv.RemoveAllWithPrefix(context.Background(), "testDir")
	if err != nil {
		t.Errorf("unexpected error when deleting all with prefix testDir: %v", err)
	}

	contents, err := azurePrv.Ls(context.Background(), "")
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

func TestOpeningFileDNENoErrorAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	var notFoundError *NotFoundError
	if file, err := azurePrv.OpenFile(context.Background(), "test.txt", OpenOptions{}); err == nil {
		_ = file.Close()
		t.Errorf("expected error when opening file that doesn't exist")
	} else if !errors.As(err, &notFoundError) {
		t.Errorf("expected not found error when opening file that doesn't exist")
	}
}

// Add revision-related tests
func TestWriteEnsureRevisionAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Ensure the revision file exists
	blobClient := azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("revisions/%s/%s.1", azurePrv.dir, "test.txt"))
	props, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		t.Errorf("error when checking if revision exists: %v", err)
	}

	// Now there should be one revision
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	} else {
		if revisions[0].WorkspaceID != azureTestingID {
			t.Errorf("unexpected workspace id: %s", revisions[0].WorkspaceID)
		}
		if revisions[0].Size != *props.ContentLength {
			t.Errorf("unexpected file size: %d", revisions[0].Size)
		}
		if revisions[0].Name != "test.txt" {
			t.Errorf("unexpected file name: %s", revisions[0].Name)
		}
		if revisions[0].ModTime.Compare(*props.LastModified) != 0 {
			t.Errorf("unexpected file mod time: %s", revisions[0].ModTime)
		}
		if revisions[0].RevisionID != "1" {
			t.Errorf("unexpected revision id: %s", revisions[0].RevisionID)
		}

		// Get the revision and ensure that it has the correct content
		rev, err := azurePrv.GetRevision(context.Background(), "test.txt", revisions[0].RevisionID)
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
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", azurePrv.dir, "test.txt")).GetProperties(context.Background(), nil); err == nil {
		t.Errorf("file should not exist after deleting")
	}

	// Ensure the revision file no longer exists
	if _, err = azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("revisions/%s/%s.1", azurePrv.dir, "test.txt")).GetProperties(context.Background(), nil); err == nil {
		t.Errorf("revision should not exist after deleting")
	}

	// Ensure the API returns no revisions for the file
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}
}

func TestWriteEnsureConflictAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	ce := (*ConflictError)(nil)
	// Trying to update the file with a non-zero revision ID should fail with a conflict error
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "1"}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when first updating file non-zero revision ID: %v", err)
	}

	// Also, using -1 for the revision ID should also fail because that is the same as "only write if the file doesn't exist"
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "-1"}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when first updating file non-zero revision ID: %v", err)
	}

	// Update the file
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be one revision
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file again
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test3"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be two revisions
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Trying to update the file again with the same revision ID should fail with a conflict error
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test4"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when updating file with same revision ID: %v", err)
	}

	latestRevisionID := revisions[1].RevisionID
	// Delete the most recent revision
	if err = azurePrv.DeleteRevision(context.Background(), "test.txt", latestRevisionID); err != nil {
		t.Errorf("error deleting revision: %v", err)
	}

	// Now there should be one revision
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// We cannot update the file with this revision ID
	ce = (*ConflictError)(nil)
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test5"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when updating file with zero revision ID: %v", err)
	}

	// Ensure that we can still create a new revision
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test5"), WriteOptions{LatestRevisionID: latestRevisionID}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Delete the file
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("error removing file: %v", err)
	}
}

func TestDeleteRevisionAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be one revision
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test3"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be two revisions
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the first revision
	if err = azurePrv.DeleteRevision(context.Background(), "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// Now there should be one revision
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 || revisions[0].RevisionID != "2" {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Deleting the revision again should not produce an error
	if err = azurePrv.DeleteRevision(context.Background(), "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// Delete the file
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("%s/%s", azurePrv.dir, "test.txt")).GetProperties(context.Background(), nil); err == nil {
		t.Errorf("file should not exist after deleting")
	}

	// Ensure the revision file no longer exists
	if _, err = azurePrv.client.ServiceClient().NewContainerClient(azurePrv.containerName).NewBlockBlobClient(fmt.Sprintf("revisions/%s/%s.2", azurePrv.dir, "test.txt")).GetProperties(context.Background(), nil); err == nil {
		t.Errorf("revision should not exist after deleting")
	}
}

func TestNoCreateRevisionsClientAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	_, err := azureFactory.New(fmt.Sprintf("%s://%s/%s", AzureProvider, os.Getenv("WORKSPACE_PROVIDER_AZURE_CONTAINER"), revisionsDir))
	if err == nil {
		t.Errorf("expected error when creating client for revisions dir")
	}
}

func TestPathValidationAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	tests := []struct {
		name     string
		path     string
		wantErr  bool
		errMsg   string
		testFunc func(string) error
	}{
		// Path traversal tests
		{"traversal parent", "../test.txt", true, "must not contain '..'", nil},
		{"traversal nested", "foo/../../test.txt", true, "must not contain '..'", nil},
		{"traversal with slash", "../test.txt/", true, "must not contain '..'", nil},

		// Absolute path tests
		{"absolute path", "/test.txt", true, "must be relative", nil},
		{"absolute nested", "/foo/test.txt", true, "must be relative", nil},

		// Azure naming rule tests
		{"trailing slash", "test/", true, "cannot end with '/'", nil},
		{"double slash", "foo//bar.txt", true, "cannot contain consecutive '/'", nil},
		{"invalid chars", "test*.txt", true, "contains invalid characters", nil},
		{"invalid chars nested", "foo/test*.txt", true, "contains invalid characters", nil},
		{"long path", strings.Repeat("a/", 1000) + "a.txt", true, "length cannot exceed 1024", nil},

		// Valid paths
		{"simple file", "test.txt", false, "", nil},
		{"nested file", "foo/bar/test.txt", false, "", nil},
		{"with numbers", "test123.txt", false, "", nil},
		{"with dash", "test-file.txt", false, "", nil},
		{"with underscore", "test_file.txt", false, "", nil},
	}

	// Create a test file to verify existence checks
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error creating test file: %v", err)
	}
	defer func() {
		if err := azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
			t.Errorf("error deleting test file: %v", err)
		}
	}()

	for _, tt := range tests {
		t.Run(fmt.Sprintf("WriteFile/%s", tt.name), func(t *testing.T) {
			err := azurePrv.WriteFile(context.Background(), tt.path, strings.NewReader("test"), WriteOptions{})
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("OpenFile/%s", tt.name), func(t *testing.T) {
			_, err := azurePrv.OpenFile(context.Background(), tt.path, OpenOptions{})
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("StatFile/%s", tt.name), func(t *testing.T) {
			_, err := azurePrv.StatFile(context.Background(), tt.path, StatOptions{})
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("DeleteFile/%s", tt.name), func(t *testing.T) {
			err := azurePrv.DeleteFile(context.Background(), tt.path)
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("Ls/%s", tt.name), func(t *testing.T) {
			_, err := azurePrv.Ls(context.Background(), tt.path)
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("RemoveAllWithPrefix/%s", tt.name), func(t *testing.T) {
			err := azurePrv.RemoveAllWithPrefix(context.Background(), tt.path)
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("ListRevisions/%s", tt.name), func(t *testing.T) {
			_, err := azurePrv.ListRevisions(context.Background(), tt.path)
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})

		t.Run(fmt.Sprintf("GetRevision/%s", tt.name), func(t *testing.T) {
			if tt.wantErr {
				_, err := azurePrv.GetRevision(context.Background(), tt.path, "1")
				assertPathError(t, err, tt.wantErr, tt.errMsg)
			}
		})

		t.Run(fmt.Sprintf("DeleteRevision/%s", tt.name), func(t *testing.T) {
			err := azurePrv.DeleteRevision(context.Background(), tt.path, "1")
			assertPathError(t, err, tt.wantErr, tt.errMsg)
		})
	}
}

// Helper function to assert path validation errors
func assertPathError(t *testing.T, err error, wantErr bool, errMsg string) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Error("expected error but got none")
			return
		}
		if !strings.Contains(err.Error(), errMsg) {
			t.Errorf("expected error containing %q, got %v", errMsg, err)
		}
	} else if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestReadFileWithRevisionAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Read the file
	f, err := azurePrv.OpenFile(context.Background(), "test.txt", OpenOptions{WithLatestRevisionID: true})
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

	// Ensure that the revision is set and correct
	revisionID, err := f.GetRevisionID()
	if err != nil {
		t.Errorf("error getting revision: %v", err)
	}
	if revisionID != "0" {
		t.Errorf("unexpected revision ID: %s", revisionID)
	}

	// Update the file
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "0"}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Read the file
	f, err = azurePrv.OpenFile(context.Background(), "test.txt", OpenOptions{WithLatestRevisionID: true})
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
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("error removing file: %v", err)
	}
}

func TestWriteEnsureNoRevisionAzure(t *testing.T) {
	if skipAzureTests {
		t.Skip("Skipping Azure tests")
	}

	createRevision := false
	// Copy a file into the workspace
	if err := azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{CreateRevision: &createRevision}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = azurePrv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{CreateRevision: &createRevision}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should still be no revision
	revisions, err = azurePrv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the file
	if err = azurePrv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}
