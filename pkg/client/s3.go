package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
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
	bucket, dir string
	client      *s3.Client
}

func (s *s3Provider) New(id string) workspaceClient {
	bucket, dir, _ := strings.Cut(strings.TrimPrefix(id, S3Provider+"://"), "/")
	return &s3Provider{
		bucket: bucket,
		dir:    dir,
		client: s.client,
	}
}

func (s *s3Provider) Create() (string, error) {
	return S3Provider + "://" + filepath.Join(s.bucket, uuid.NewString()), nil
}

func (s *s3Provider) Rm(ctx context.Context, id string) error {
	bucket, dir, _ := strings.Cut(strings.TrimPrefix(id, S3Provider+"://"), "/")

	newS := &s3Provider{
		bucket: bucket,
		dir:    dir,
		client: s.client,
	}

	return newS.RemoveAllWithPrefix(ctx, "")
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
		if errors.As(err, &respErr) && respErr.Response.StatusCode == 404 {
			return newNotFoundError(fmt.Sprintf("%s://%s/%s", S3Provider, s.bucket, s.dir), filePath)
		}
	}

	return err
}

func (s *s3Provider) OpenFile(ctx context.Context, filePath string) (io.ReadCloser, error) {
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

	return out.Body, nil
}

func (s *s3Provider) WriteFile(ctx context.Context, fileName string, reader io.Reader) error {
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
