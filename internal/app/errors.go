package app

import "errors"

// ErrInvalidArgument marks errors caused by caller-controlled API input.
var ErrInvalidArgument = errors.New("invalid argument")
