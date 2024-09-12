package client

import (
	"context"
	"io"
)

func newS3(bucketName string) workspaceFactory {
	return &s3{
		bucketName: bucketName,
	}
}

type s3 struct {
	bucketName string
}

func (s *s3) New(_ context.Context, id string) workspaceClient {
	return &s3{
		bucketName: s.bucketName + "/" + id,
	}
}

func (s *s3) Create(context.Context) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) Rm(context.Context, string) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) Ls(context.Context, LsOptions) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) DeleteFile(context.Context, string) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) OpenFile(context.Context, string) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) WriteFile(context.Context, string, WriteOptions) (io.WriteCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) MkDir(ctx context.Context, s2 string, options MkDirOptions) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) RmDir(ctx context.Context, s2 string, options RmDirOptions) error {
	//TODO implement me
	panic("implement me")
}
