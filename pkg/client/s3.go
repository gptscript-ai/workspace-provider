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

func (s *s3) Rm(_ context.Context, id string) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) Ls(context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) DeleteFile(_ context.Context, file string) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) OpenFile(_ context.Context, file string) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) WriteFile(_ context.Context, file string) (io.WriteCloser, error) {
	//TODO implement me
	panic("implement me")
}
