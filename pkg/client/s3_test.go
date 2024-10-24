package client

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	id, err := s3Factory.Create()
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

	if err := s3Factory.Rm(context.Background(), id); err != nil {
		t.Errorf("unexpected error when removing workspace: %v", err)
	}
}

func TestWriteAndDeleteFileInS3(t *testing.T) {
	if skipS3Tests {
		t.Skip("Skipping S3 tests")
	}

	// Copy a file into the workspace
	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	// Ensure the file actually exists
	if _, err := s3Prv.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: &s3Prv.bucket, Key: aws.String(fmt.Sprintf("%s/%s", s3Prv.dir, "test.txt"))}); err != nil {
		t.Errorf("error when checking if file exists: %v", err)
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
	if err := s3Prv.WriteFile(context.Background(), filePath, strings.NewReader("test")); err != nil {
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

	if err := s3Prv.WriteFile(context.Background(), "test.txt", strings.NewReader("test")); err != nil {
		t.Fatalf("error getting file to write: %v", err)
	}

	readFile, err := s3Prv.OpenFile(context.Background(), "test.txt")
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
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
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
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
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
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
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
		if err := s3Prv.WriteFile(context.Background(), fileName, strings.NewReader("test")); err != nil {
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
	if file, err := s3Prv.OpenFile(context.Background(), "test.txt"); err == nil {
		_ = file.Close()
		t.Errorf("expected error when deleting file that doesn't exist")
	} else if !errors.As(err, &notFoundError) {
		t.Errorf("expected not found error when deleting file that doesn't exist")
	}
}
