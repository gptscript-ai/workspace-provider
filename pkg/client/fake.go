package client

import (
	"context"
	"errors"
	"fmt"
	"io"
)

func newFake() *fake {
	return &fake{}
}

// fake exists only to test that the correct options are being sent from the client implementation.
type fake struct {
	id                   string
	expectedLsOptions    LsOptions
	expectedWriteOptions WriteOptions
}

func (f *fake) New(_ context.Context, id string) workspaceClient {
	return &fake{
		id:                   id,
		expectedLsOptions:    f.expectedLsOptions,
		expectedWriteOptions: f.expectedWriteOptions,
	}
}

func (f *fake) Create(context.Context) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fake) Rm(context.Context, string) error {
	//TODO implement me
	panic("implement me")
}

func (f *fake) Ls(_ context.Context, opt LsOptions) ([]string, error) {
	var errs []error
	if f.expectedLsOptions.SubDir != opt.SubDir {
		errs = append(errs, fmt.Errorf("unexpected ls subdirectory: %s", opt.SubDir))
	}
	if f.expectedLsOptions.NonRecursive != opt.NonRecursive {
		errs = append(errs, fmt.Errorf("unexpected ls non-recursive: %v", opt.NonRecursive))
	}
	return nil, errors.Join(errs...)
}

func (f *fake) DeleteFile(context.Context, string) error {
	//TODO implement me
	panic("implement me")
}

func (f *fake) OpenFile(context.Context, string) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fake) WriteFile(_ context.Context, _ string, opt WriteOptions) (io.WriteCloser, error) {
	var errs []error
	if f.expectedWriteOptions.CreateDirs != opt.CreateDirs {
		errs = append(errs, fmt.Errorf("unexpected write create dirs: %v", opt.CreateDirs))
	}
	if f.expectedWriteOptions.WithoutCreate != opt.WithoutCreate {
		errs = append(errs, fmt.Errorf("unexpected write without create: %v", opt.WithoutCreate))
	}
	if f.expectedWriteOptions.MustNotExist != opt.MustNotExist {
		errs = append(errs, fmt.Errorf("unexpected write must not exist: %v", opt.MustNotExist))
	}

	return nil, errors.Join(errs...)
}
