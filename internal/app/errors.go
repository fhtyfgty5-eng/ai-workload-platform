package app

import "errors"

// ErrInvalidArgument 标识由调用方可控 API 输入引起的错误。
var ErrInvalidArgument = errors.New("invalid argument")
