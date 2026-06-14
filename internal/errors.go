package internal

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
)

var (
	ErrBusy     = errors.New("resource busy")
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid argument")
	ErrExists   = errors.New("already exists")
)

func Addf(err error, format string, args ...any) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

func StatusCode(err error) codes.Code {
	switch {
	case errors.Is(err, ErrNotFound):
		return codes.NotFound
	case errors.Is(err, ErrInvalid):
		return codes.InvalidArgument
	case errors.Is(err, ErrBusy):
		return codes.FailedPrecondition
	case errors.Is(err, ErrExists):
		return codes.AlreadyExists
	default:
		return codes.Internal
	}
}
