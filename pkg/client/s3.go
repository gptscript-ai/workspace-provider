package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"

	"github.com/gabriel-vasile/mimetype"
)

func newS3(ctx context.Context, bucket, baseEndpoint string) (workspaceFactory, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if baseEndpoint != "" {
			o.BaseEndpoint = aws.String(baseEndpoint)
		}
	})
	return &s3Provider{
		bucket: bucket,
		client: client,
	}, nil
}

type s3Provider struct {
	bucket, dir       string
	client            *s3.Client
	revisionsProvider workspaceClient
}

func (s *s3Provider) New(id string) (workspaceClient, error) {
	bucket, dir, _ := strings.Cut(strings.TrimPrefix(id, S3Provider+"://"), "/")
	if dir == revisionsDir {
		return nil, errors.New("cannot create a workspace client for the revisions directory")
	}

	return &s3Provider{
		bucket: bucket,
		dir:    dir,
		client: s.client,
		revisionsProvider: &s3Provider{
			bucket: bucket,
			dir:    fmt.Sprintf("%s/%s", revisionsDir, dir),
			client: s.client,
		},
	}, nil
}

func (s *s3Provider) Create() string {
	return S3Provider + "://" + filepath.Join(s.bucket, uuid.NewString())
}

func (s *s3Provider) Rm(ctx context.Context, id string) error {
	bucket, dir, _ := strings.Cut(strings.TrimPrefix(id, S3Provider+"://"), "/")

	newS := &s3Provider{
		bucket: bucket,
		dir:    dir,
		client: s.client,
		revisionsProvider: &s3Provider{
			bucket: bucket,
			dir:    fmt.Sprintf("%s/%s", revisionsDir, dir),
			client: s.client,
		},
	}

	// Best effort
	_ = newS.revisionsProvider.RemoveAllWithPrefix(ctx, s.dir)

	return newS.RemoveAllWithPrefix(ctx, "")
}

func (s *s3Provider) RevisionClient() workspaceClient {
	return s.revisionsProvider
}

func (s *s3Provider) Ls(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" {
		prefix = fmt.Sprintf("%s/%s/", s.dir, strings.TrimSuffix(prefix, "/"))
	} else {
		prefix = fmt.Sprintf("%s/", s.dir)
	}

	var (
		continuation *string
		files        []string
	)
	for {
		contents, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return nil, err
		}

		if len(contents.Contents) == 0 {
			return files, nil
		}

		files = slices.Grow(files, len(contents.Contents))
		for _, content := range contents.Contents {
			files = append(files, strings.TrimPrefix(*content.Key, s.dir+"/"))
		}

		if contents.IsTruncated == nil || !*contents.IsTruncated {
			return files, nil
		}

		continuation = contents.ContinuationToken
	}
}

func (s *s3Provider) DeleteFile(ctx context.Context, filePath string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", s.dir, filePath)),
	})
	if err != nil {
		var respErr *http.ResponseError
		if !errors.As(err, &respErr) || respErr.Response.StatusCode != 404 {
			return err
		}
	}

	if s.revisionsProvider == nil {
		return nil
	}

	info, err := getRevisionInfo(ctx, s.revisionsProvider, filePath)
	if err != nil {
		return err
	}

	for i := info.CurrentID; i > 0; i-- {
		// Best effort
		_ = deleteRevision(ctx, s.revisionsProvider, filePath, fmt.Sprintf("%d", i))
	}

	// Best effort
	_ = deleteRevisionInfo(ctx, s.revisionsProvider, filePath)

	return nil
}

func (s *s3Provider) OpenFile(ctx context.Context, filePath string, opt OpenOptions) (*File, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", s.dir, filePath)),
	})
	if err != nil {
		var respErr *http.ResponseError
		if errors.As(err, &respErr) && respErr.Response.StatusCode == 404 {
			return nil, newNotFoundError(fmt.Sprintf("%s://%s/%s", S3Provider, s.bucket, s.dir), filePath)
		}
		return nil, err
	}

	var revision string
	if opt.WithLatestRevisionID {
		rev, err := getRevisionInfo(ctx, s.revisionsProvider, filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to get revision info: %w", err)
		}
		revision = strconv.FormatInt(rev.CurrentID, 10)
	}

	return &File{
		ReadCloser: out.Body,
		RevisionID: revision,
	}, nil
}

func (s *s3Provider) WriteFile(ctx context.Context, fileName string, reader io.Reader, opt WriteOptions) error {
	if s.revisionsProvider != nil && (opt.CreateRevision == nil || *opt.CreateRevision) {
		info, err := getRevisionInfo(ctx, s.revisionsProvider, fileName)
		if err != nil {
			if nfe := (*NotFoundError)(nil); !errors.As(err, &nfe) {
				return err
			}
		}

		if opt.LatestRevisionID != "" {
			requiredLatestRevision, err := strconv.ParseInt(opt.LatestRevisionID, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse latest revision for write: %w", err)
			}

			if requiredLatestRevision != info.CurrentID {
				return newConflictError(S3Provider+"://"+s.bucket, fileName, opt.LatestRevisionID, fmt.Sprintf("%d", info.CurrentID))
			}
		}

		info.CurrentID++
		if err = writeRevision(ctx, s.revisionsProvider, s, fileName, info); err != nil {
			if nfe := (*NotFoundError)(nil); !errors.As(err, &nfe) {
				return fmt.Errorf("failed to write revision: %w", err)
			}
		}

		if err = writeRevisionInfo(ctx, s.revisionsProvider, fileName, info); err != nil {
			return fmt.Errorf("failed to write revision info: %w", err)
		}
	}

	var contentLength int64
	switch r := reader.(type) {
	case io.Seeker:
		var err error
		contentLength, err = r.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}

		_, err = r.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
	default:
		b, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		contentLength = int64(len(b))
		reader = bytes.NewReader(b)
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fmt.Sprintf("%s/%s", s.dir, fileName)),
		ContentLength: aws.Int64(contentLength),
		Body:          reader,
	})

	return err
}

func (s *s3Provider) StatFile(ctx context.Context, fileName string, opt StatOptions) (FileInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", s.dir, fileName)),
	})
	if err != nil {
		var respErr *http.ResponseError
		if errors.As(err, &respErr) && respErr.Response.StatusCode == 404 {
			return FileInfo{}, newNotFoundError(fmt.Sprintf("%s://%s/%s", S3Provider, s.bucket, s.dir), fileName)
		}
		return FileInfo{}, err
	}

	var mime string
	if out.ContentType != nil {
		mime = *out.ContentType
	}

	// get the first 3072 bytes of the file to detect the mimetype, as the S3 ContentType is not reliable if not set explicitly
	fileStart, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", s.dir, fileName)),
		Range:  aws.String("bytes=0-3072"), // 3072 is the default read limit of the mimetype package
	})
	if err != nil {
		return FileInfo{}, err
	}
	defer fileStart.Body.Close()

	mt, err := mimetype.DetectReader(fileStart.Body)
	if err == nil {
		mime = strings.Split(mt.String(), ";")[0]
	}

	var revision string
	if opt.WithLatestRevisionID {
		rev, err := getRevisionInfo(ctx, s.revisionsProvider, fileName)
		if err != nil {
			return FileInfo{}, err
		}
		revision = strconv.FormatInt(rev.CurrentID, 10)
	}

	return FileInfo{
		WorkspaceID: fmt.Sprintf("%s://%s/%s", S3Provider, s.bucket, s.dir),
		Name:        strings.TrimPrefix(fileName, s.dir+"/"),
		Size:        aws.ToInt64(out.ContentLength),
		ModTime:     aws.ToTime(out.LastModified),
		MimeType:    mime,
		RevisionID:  revision,
	}, nil
}

func (s *s3Provider) RemoveAllWithPrefix(ctx context.Context, prefix string) error {
	if prefix != "" {
		prefix = fmt.Sprintf("%s/%s/", s.dir, strings.TrimSuffix(prefix, "/"))
	} else {
		prefix = fmt.Sprintf("%s/", s.dir)
	}

	var continuation *string

	for {
		contents, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return err
		}

		if len(contents.Contents) == 0 {
			return nil
		}

		deleteObjects := make([]types.ObjectIdentifier, 0, len(contents.Contents))
		for _, item := range contents.Contents {
			deleteObjects = append(deleteObjects, types.ObjectIdentifier{
				Key: item.Key,
			})
		}

		_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: deleteObjects,
			},
		})
		if err != nil {
			return err
		}

		if contents.IsTruncated == nil || !*contents.IsTruncated {
			return nil
		}

		continuation = contents.ContinuationToken
	}
}

func (s *s3Provider) ListRevisions(ctx context.Context, fileName string) ([]RevisionInfo, error) {
	return listRevisions(ctx, s.revisionsProvider, fmt.Sprintf("%s://%s/%s", S3Provider, s.bucket, s.dir), fileName)
}

func (s *s3Provider) GetRevision(ctx context.Context, fileName, revisionID string) (*File, error) {
	return getRevision(ctx, s.revisionsProvider, fileName, revisionID)
}

func (s *s3Provider) DeleteRevision(ctx context.Context, fileName, revisionID string) error {
	return deleteRevision(ctx, s.revisionsProvider, fileName, revisionID)
}
