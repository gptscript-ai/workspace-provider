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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestCreateAndRmS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	id := s3Factory.Create()
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

	if err := s3Factory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileInS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	obj, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, "test.txt"))})
	if err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}
	defer obj.Body.Close()

	// Stat the file and compare with the original
	providerStat, err := s3Prv.StatFile(context.Background(), "test.txt", StatOptions{})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if providerStat.WorkspaceID != s3TestingID {
		t.Errorf("unexpected workspace id: %s", providerStat.WorkspaceID)
	}
	if providerStat.Size != aws.ToInt64(obj.ContentLength) {
		t.Errorf("unexpected file size: %d", providerStat.Size)
	}
	if providerStat.Name != "test.txt" {
		t.Errorf("unexpected file name: %s", providerStat.Name)
	}
	if providerStat.ModTime.Compare(aws.ToTime(obj.LastModified)) != 0 {
		t.Errorf("unexpected file mod time: %s", providerStat.ModTime)
	}

	// Delete the file
	if err := s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, "test.txt"))}); err == nil {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestWriteAndDeleteFileInS3WithSubDir(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	filePath := filepath.Join("subdir", "test.txt")
	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), filePath, strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	if _, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, filePath))}); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}

	// Delete the file
	if err := s3Prv.DeleteFile(context.Background(), filePath); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, filePath))}); err == nil {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestFileReadFromS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	readFile, err := s3Prv.OpenFile(context.Background(), "test.txt", OpenOptions{})
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
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Deleting the file again should not throw an error
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestLsS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := s3Prv.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := s3Prv.Ls(context.Background(), "")
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

func TestLsWithSubDirsS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	defer func() {
		err := s3Prv.RemoveAllWithPrefix(context.Background(), "testDir")
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
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := s3Prv.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := s3Prv.Ls(context.Background(), "")
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

func TestLsWithPrefixS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	defer func() {
		err := s3Prv.RemoveAllWithPrefix(context.Background(), "testDir")
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
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := s3Prv.DeleteFile(context.Background(), fileName)
			if err != nil {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	contents, err := s3Prv.Ls(context.Background(), "testDir")
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
			"testDir/test3.txt",
			"testDir/test4.txt",
			"testDir/test5.txt",
			"testDir/test6.txt",
		},
	) {
		t.Errorf("unexpected contents: %v", contents)
	}
}

func TestRemoveAllWithPrefixS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Write a bunch of files to the directory. They can be blank
	for i := range 7 {
		fileName := fmt.Sprintf("test%d.txt", i)
		if i >= 3 {
			fileName = fmt.Sprintf("testDir/%s", fileName)
		}
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test"), WriteOptions{}); err != nil {
			t.Fatalf("error getting file to write: %v", err)
		}

		// deferring here is fine because these files shouldn't be deleted until the end of the test
		defer func() {
			err := s3Prv.DeleteFile(context.Background(), fileName)
			if fnf := (*NotFoundError)(nil); err != nil && !errors.As(err, &fnf) {
				t.Errorf("unexpected error when deleting file %s: %v", fileName, err)
			}
		}()
	}

	err := s3Prv.RemoveAllWithPrefix(context.Background(), "testDir")
	if err != nil {
		t.Errorf("unexpected error when deleting all with prefix testDir: %v", err)
	}

	contents, err := s3Prv.Ls(context.Background(), "")
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

func TestOpeningFileDNENoErrorS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	var notFoundError *NotFoundError
	if file, err := s3Prv.OpenFile(context.Background(), "test.txt", OpenOptions{}); err == nil {
		_ = file.Close()
		t.Errorf("expected error when deleting file that doesn't exist")
	} else if !errors.As(err, &notFoundError) {
		t.Errorf("expected not found error when deleting file that doesn't exist")
	}
}

func TestWriteEnsureRevisionS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	obj, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("revisions/%s/%s", s3Prv.dir, "test.txt.1"))})
	if err != nil {
		t.Errorf("error when checking if file exists: %v", err)
	}
	defer obj.Body.Close()

	// Now there should be one revision
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	} else {
		if revisions[0].WorkspaceID != s3TestingID {
			t.Errorf("unexpected workspace id: %s", revisions[0].WorkspaceID)
		}
		if revisions[0].Size != aws.ToInt64(obj.ContentLength) {
			t.Errorf("unexpected file size: %d", revisions[0].Size)
		}
		if revisions[0].Name != "test.txt" {
			t.Errorf("unexpected file name: %s", revisions[0].Name)
		}
		if revisions[0].ModTime.Compare(aws.ToTime(obj.LastModified)) != 0 {
			t.Errorf("unexpected file mod time: %s", revisions[0].ModTime)
		}

		if revisions[0].RevisionID != "1" {
			t.Errorf("unexpected revision id: %s", revisions[0].RevisionID)
		}

		// Get the revision and ensure that it has the correct content.
		rev, err := s3Prv.GetRevision(context.Background(), "test.txt", revisions[0].RevisionID)
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
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err = s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, "test.txt"))}); err == nil {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Ensure the revision file no longer exists
	if _, err = s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("revisions/%s/%s", s3Prv.dir, "test.txt.1"))}); err == nil {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Ensure the API returns no revisions for the file
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}
}

func TestWriteEnsureNoRevisionS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{CreateRevision: new(bool)}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should still be no revision
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the file
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}
}

func TestWriteEnsureConflictS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	ce := (*ConflictError)(nil)
	// Trying to update the file with a non-zero revision ID should fail with a conflict error.
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "1"}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when first updating file non-zero revision ID: %v", err)
	}

	// Also, using -1 for the revision ID should also fail because that is the same as "only write if the file doesn't exist"
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "-1"}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when first updating file non-zero revision ID: %v", err)
	}

	// Update the file
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be one revision
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file again
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test3"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be two revisions
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Trying to update the file again with the same revision ID should fail with a conflict error.
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test4"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when updating file with same revision ID: %v", err)
	}

	latestRevisionID := revisions[1].RevisionID
	// Delete the most recent revision
	if err = s3Prv.DeleteRevision(context.Background(), "test.txt", latestRevisionID); err != nil {
		t.Errorf("error deleting revision: %v", err)
	}

	// Now there should be one revision
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// We cannot update the file with this revision ID
	ce = (*ConflictError)(nil)
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test5"), WriteOptions{LatestRevisionID: revisions[0].RevisionID}); err == nil || !errors.As(err, &ce) {
		t.Errorf("expected error when updating file with zero revision ID: %v", err)
	}

	// Ensure that we can still create a new revision
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test5"), WriteOptions{LatestRevisionID: latestRevisionID}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Delete the file
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("error removing file: %v", err)
	}
}

func TestReadFileWithRevisionS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Read the file
	f, err := s3Prv.OpenFile(context.Background(), "test.txt", OpenOptions{WithLatestRevisionID: true})
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
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{LatestRevisionID: "0"}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Read the file
	f, err = s3Prv.OpenFile(context.Background(), "test.txt", OpenOptions{WithLatestRevisionID: true})
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
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("error removing file: %v", err)
	}
}

func TestDeleteRevisionS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// List revisions, there should be none
	revisions, err := s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 0 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test2"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be one revision
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Update the file
	if err = s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test3"), WriteOptions{}); err != nil {
		t.Errorf("error getting file to write: %v", err)
	}

	// Now there should be two revisions
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Delete the first revision
	if err = s3Prv.DeleteRevision(context.Background(), "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// Now there should be one revision
	revisions, err = s3Prv.ListRevisions(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("unexpected error when listing revisions: %v", err)
	}
	if len(revisions) != 1 || revisions[0].RevisionID != "2" {
		t.Errorf("unexpected number of revisions: %d", len(revisions))
	}

	// Deleting the revision again should not produce an error.
	if err = s3Prv.DeleteRevision(context.Background(), "test.txt", "1"); err != nil {
		t.Errorf("unexpected error when deleting revision: %v", err)
	}

	// Delete the file
	if err = s3Prv.DeleteFile(context.Background(), "test.txt"); err != nil {
		t.Errorf("unexpected error when deleting file: %v", err)
	}

	// Ensure the file no longer exists
	if _, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, "test.txt"))}); err == nil {
		t.Errorf("file should not exist after deleting: %v", err)
	}

	// Ensure the revision file no longer exists
	if _, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, "test.txt.2"))}); err == nil {
		t.Errorf("file should not exist after deleting: %v", err)
	}
}

func TestNoCreateRevisionsClientS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	_, err := s3Factory.New(fmt.Sprintf("%s://%s/%s", S3Provider, os.Getenv("WORKSPACE_PROVIDER_S3_BUCKET"), revisionsDir))
	if err == nil {
		t.Errorf("expected error when creating client for revisions dir")
	}
}

func TestStatFileS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test"), WriteOptions{}); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Stat the file
	providerStat, err := s3Prv.StatFile(context.Background(), "test.txt", StatOptions{})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if providerStat.WorkspaceID != s3TestingID {
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
	providerStat, err = s3Prv.StatFile(context.Background(), "test.txt", StatOptions{WithLatestRevisionID: true})
	if err != nil {
		t.Errorf("unexpected error when statting file: %v", err)
	}

	if rev, err := providerStat.GetRevisionID(); err != nil {
		t.Errorf("unexpected error when revision not requested: %v", err)
	} else if rev != "0" {
		t.Errorf("unexpected revision id when revision requested: %s", rev)
	}
}
