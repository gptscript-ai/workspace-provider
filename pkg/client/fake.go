package client

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// fake exists only to test that the correct options are being sent from the client implementation.
type fake struct {
	id                   string
	expectedLsOptions    LsOptions
	expectedWriteOptions WriteOptions
	expectedMkDirOptions MkDirOptions
	expectedRmDirOptions RmDirOptions
}

func (f *fake) New(_ context.Context, id string) workspaceClient {
	return &fake{
		id:                   id,
		expectedLsOptions:    f.expectedLsOptions,
		expectedWriteOptions: f.expectedWriteOptions,
		expectedMkDirOptions: f.expectedMkDirOptions,
		expectedRmDirOptions: f.expectedRmDirOptions,
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

func (f *fake) MkDir(_ context.Context, _ string, opt MkDirOptions) error {
	var errs []error
	if f.expectedMkDirOptions.MustNotExist != opt.MustNotExist {
		errs = append(errs, fmt.Errorf("unexpected mkdir must not exist: %v", opt.MustNotExist))
	}
	if f.expectedMkDirOptions.CreateDirs != opt.CreateDirs {
		errs = append(errs, fmt.Errorf("unexpected mkdir create dirs: %v", opt.CreateDirs))
	}

	return errors.Join(errs...)
}

func (f *fake) RmDir(_ context.Context, _ string, opt RmDirOptions) error {
	var errs []error
	if f.expectedRmDirOptions.NonEmpty != opt.NonEmpty {
		errs = append(errs, fmt.Errorf("unexpected rm dir must not exist: %v", opt.NonEmpty))
	}

	return errors.Join(errs...)
}
