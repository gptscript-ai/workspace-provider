package client

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var c, _ = New(context.Background(), Options{
	S3BucketName:   os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET"),
	S3BaseEndpoint: os.Getenv("WORKSPACE_PROVIDER_S3_BASE_ENDPOINT"),
})

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

	// The directory should exist
	if _, err = os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); err != nil {
		t.Errorf("unexpected error when checking if directory exists: %v", err)
	}

	if err = c.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}

	// The directory should no longer exist
	if _, err = os.Stat(strings.TrimPrefix(id, DirectoryProvider+"://")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unexpected error when checking if directory exists: %v", err)
	}
}

func TestCreateAndRmS3Provider(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	id, err := c.Create(context.Background(), S3Provider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	if !strings.HasPrefix(id, S3Provider+"://") {
		t.Errorf("unexpected id: %s", id)
	}

	bucket, dir, _ := strings.Cut(strings.TrimPrefix(id, S3Provider+"://"), "/")
	testS3Provider := &s3Provider{
		bucket: bucket,
		client: s3Prv.client,
	}

	// Nothing should be created
	var respErr *http.ResponseError
	if _, err := testS3Provider.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &testS3Provider.bucket, Key: &dir}); err == nil {
		t.Errorf("expected error when checking if workspace exists")
	} else if !errors.As(err, &respErr) || respErr.Response.StatusCode != 404 {
		t.Errorf("unexpected error when checking if workspace exists: %v", err)
	}

	if err = c.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
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
	if err = c.WriteFile(context.Background(), parentID, "test.txt", strings.NewReader("test-temp")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Write to file again to create a revision
	if err = c.WriteFile(context.Background(), parentID, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	id, err := c.Create(context.Background(), DirectoryProvider, parentID)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	// Ensure the file was copied over
	workspaceContent, err := c.Ls(context.Background(), id, "")
	if err != nil {
		t.Errorf("unexpected error when listing workspaceContent: %v", err)
	}

	if len(workspaceContent) != 1 {
		t.Errorf("unexpected number of workspaceContent: %d", len(workspaceContent))
	}

	if workspaceContent[0] != "test.txt" {
		t.Errorf("unexpected file: %s", workspaceContent[0])
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

	// Ensure the new workspace has the revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	rev, err := c.GetRevision(context.Background(), id, "test.txt", "1")
	if err != nil {
		t.Errorf("unexpected error when getting revision: %v", err)
	}

	content, err = io.ReadAll(rev)
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	if string(content) != "test-temp" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = c.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestCreateAndRmS3ProviderFromProvider(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	parentID, err := c.Create(context.Background(), S3Provider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(context.Background(), parentID); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	if err = c.WriteFile(context.Background(), parentID, "test.txt", strings.NewReader("test-temp")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Write to the file again to create a revision
	if err = c.WriteFile(context.Background(), parentID, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	id, err := c.Create(context.Background(), S3Provider, parentID)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	// Ensure the file was copied over
	workspaceContent, err := c.Ls(context.Background(), id, "")
	if err != nil {
		t.Errorf("unexpected error when listing workspaceContent: %v", err)
	}

	if len(workspaceContent) != 1 {
		t.Errorf("unexpected number of workspaceContent: %d", len(workspaceContent))
	}

	if workspaceContent[0] != "test.txt" {
		t.Errorf("unexpected file: %s", workspaceContent[0])
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

	// Ensure the new workspace has the revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	rev, err := c.GetRevision(context.Background(), id, "test.txt", "1")
	if err != nil {
		t.Errorf("unexpected error when getting revision: %v", err)
	}

	content, err = io.ReadAll(rev)
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	if string(content) != "test-temp" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = c.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestCreateAndRmS3ProviderFromDirectoryProvider(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

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
	if err = c.WriteFile(context.Background(), parentID, "test.txt", strings.NewReader("test-temp")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Write to the file again to create a revision
	if err = c.WriteFile(context.Background(), parentID, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	id, err := c.Create(context.Background(), S3Provider, parentID)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	// Ensure the file was copied over
	workspaceContent, err := c.Ls(context.Background(), id, "")
	if err != nil {
		t.Errorf("unexpected error when listing workspaceContent: %v", err)
	}

	if len(workspaceContent) != 1 {
		t.Errorf("unexpected number of workspaceContent: %d", len(workspaceContent))
	}

	if workspaceContent[0] != "test.txt" {
		t.Errorf("unexpected file: %s", workspaceContent[0])
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

	// Ensure the new workspace has the revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	rev, err := c.GetRevision(context.Background(), id, "test.txt", "1")
	if err != nil {
		t.Errorf("unexpected error when getting revision: %v", err)
	}

	content, err = io.ReadAll(rev)
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	if string(content) != "test-temp" {
		t.Errorf("unexpected content: %s", string(content))
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
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
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

	// Stat the file
	fileInfo, err := c.StatFile(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if fileInfo.Size != 4 {
		t.Errorf("unexpected size: %d", fileInfo.Size)
	}

	if fileInfo.Name != "test.txt" {
		t.Errorf("unexpected name: %s", fileInfo.Name)
	}

	if fileInfo.ModTime.IsZero() {
		t.Errorf("unexpected mod time: %s", fileInfo.ModTime)
	}

	if fileInfo.WorkspaceID != id {
		t.Errorf("unexpected workspace id: %s", fileInfo.WorkspaceID)
	}

	// Delete the file
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestWriteAndDeleteFileS3Provider(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	id, err := c.Create(context.Background(), S3Provider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(context.Background(), id); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
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

	// Stat the file
	fileInfo, err := c.StatFile(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if fileInfo.Size != 4 {
		t.Errorf("unexpected size: %d", fileInfo.Size)
	}

	if fileInfo.Name != "test.txt" {
		t.Errorf("unexpected name: %s", fileInfo.Name)
	}

	if fileInfo.ModTime.IsZero() {
		t.Errorf("unexpected mod time: %s", fileInfo.ModTime)
	}

	if fileInfo.WorkspaceID != id {
		t.Errorf("unexpected workspace id: %s", fileInfo.WorkspaceID)
	}

	// Delete the file
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestListGetDeleteRevisionDirectoryProvider(t *testing.T) {
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
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Update the file to create a revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test2")); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	} else {
		if revisions[0].Size != 4 {
			t.Errorf("unexpected size: %d", revisions[0].Size)
		}

		if revisions[0].Name != "test.txt" {
			t.Errorf("unexpected name: %s", revisions[0].Name)
		}

		if revisions[0].ModTime.IsZero() {
			t.Errorf("unexpected mod time: %s", revisions[0].ModTime)
		}

		if revisions[0].WorkspaceID != id {
			t.Errorf("unexpected workspace id: %s", revisions[0].WorkspaceID)
		}

		if revisions[0].RevisionID != "1" {
			t.Errorf("unexpected workspace id: %s", revisions[0].RevisionID)
		}
	}

	// Read the file to ensure it was copied over
	readRev, err := c.GetRevision(context.Background(), id, "test.txt", "1")
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	content, err := io.ReadAll(readRev)
	if err != nil {
		t.Errorf("unexpected error when reading revision file: %v", err)
	}

	if string(content) != "test" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = readRev.Close(); err != nil {
		t.Errorf("error closing revision revision file: %v", err)
	}

	// Update the file to create another revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test3")); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err = c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the first revision
	if err = c.DeleteRevision(context.Background(), id, "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// List revisions
	revisions, err = c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Read the file to ensure it was copied over
	readRev, err = c.GetRevision(context.Background(), id, "test.txt", "2")
	if err != nil {
		t.Errorf("unexpected error when reading revision file: %v", err)
	}

	content, err = io.ReadAll(readRev)
	if err != nil {
		t.Errorf("unexpected error when reading revision file: %v", err)
	}

	if string(content) != "test2" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = readRev.Close(); err != nil {
		t.Errorf("error closing revision file: %v", err)
	}

	readRev, err = c.GetRevision(context.Background(), id, "test.txt", "1")
	if err == nil {
		readRev.Close()
		t.Errorf("expected error when non-existent revision file")
	}

	// Delete the file
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestNoRevisionDirectoryProvider(t *testing.T) {
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
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Update the file to create a revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test2"), WriteOptions{CreateRevision: new(bool)}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file to create another revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test3")); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err = c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the file
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestListGetDeleteRevisionS3Provider(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	id, err := c.Create(context.Background(), S3Provider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(context.Background(), id); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Update the file to create a revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test2")); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	} else {
		if revisions[0].Size != 4 {
			t.Errorf("unexpected size: %d", revisions[0].Size)
		}

		if revisions[0].Name != "test.txt" {
			t.Errorf("unexpected name: %s", revisions[0].Name)
		}

		if revisions[0].ModTime.IsZero() {
			t.Errorf("unexpected mod time: %s", revisions[0].ModTime)
		}

		if revisions[0].WorkspaceID != id {
			t.Errorf("unexpected workspace id: %s", revisions[0].WorkspaceID)
		}

		if revisions[0].RevisionID != "1" {
			t.Errorf("unexpected workspace id: %s", revisions[0].RevisionID)
		}
	}

	// Read the file to ensure it was copied over
	readRev, err := c.GetRevision(context.Background(), id, "test.txt", "1")
	if err != nil {
		t.Errorf("unexpected error when reading file: %v", err)
	}

	content, err := io.ReadAll(readRev)
	if err != nil {
		t.Errorf("unexpected error when reading revision file: %v", err)
	}

	if string(content) != "test" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = readRev.Close(); err != nil {
		t.Errorf("error closing revision revision file: %v", err)
	}

	// Update the file to create another revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test3")); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err = c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the first revision
	if err = c.DeleteRevision(context.Background(), id, "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// List revisions
	revisions, err = c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Read the file to ensure it was copied over
	readRev, err = c.GetRevision(context.Background(), id, "test.txt", "2")
	if err != nil {
		t.Errorf("unexpected error when reading revision file: %v", err)
	}

	content, err = io.ReadAll(readRev)
	if err != nil {
		t.Errorf("unexpected error when reading revision file: %v", err)
	}

	if string(content) != "test2" {
		t.Errorf("unexpected content: %s", string(content))
	}

	if err = readRev.Close(); err != nil {
		t.Errorf("error closing revision file: %v", err)
	}

	readRev, err = c.GetRevision(context.Background(), id, "test.txt", "1")
	if err == nil {
		readRev.Close()
		t.Errorf("expected error when non-existent revision file")
	}

	// Delete the file
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestNoRevisionS3Provider(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	id, err := c.Create(context.Background(), S3Provider)
	if err != nil {
		t.Errorf("error creating workspace: %v", err)
	}

	t.Cleanup(func() {
		if err := c.Rm(context.Background(), id); err != nil {
			t.Errorf("unexpected error when removing parent workspace: %v", err)
		}
	})

	// Put a file in the parent workspace
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Update the file to create a revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test2"), WriteOptions{CreateRevision: new(bool)}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err := c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file to create another revision
	if err = c.WriteFile(context.Background(), id, "test.txt", strings.NewReader("test3")); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// List revisions
	revisions, err = c.ListRevisions(context.Background(), id, "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}

	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the file
	if err = c.DeleteFile(context.Background(), id, "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}
