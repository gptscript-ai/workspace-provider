package client

import (
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

func (s *s3) New(id string) workspaceClient {
	return &s3{
		bucketName: s.bucketName + "/" + id,
	}
}

func (s *s3) Create() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) Rm(id string) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) Cp(workspaceClient workspaceClient) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) Ls() ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) DeleteFile(file string) error {
	//TODO implement me
	panic("implement me")
}

func (s *s3) OpenFile(file string) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (s *s3) WriteFile(file string) (io.WriteCloser, error) {
	//TODO implement me
	panic("implement me")
}
