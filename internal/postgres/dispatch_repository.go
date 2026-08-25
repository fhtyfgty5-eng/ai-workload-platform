package postgres

import (
	"errors"
	"time"
)

const defaultLeaseDuration = 15 * time.Second
const defaultWorkerHeartbeatInterval = 5 * time.Second

const (
	maxResultOutputBytes       = 64 * 1024
	maxResultErrorCodeBytes    = 128
	maxResultErrorMessageBytes = 8 * 1024
)

var (
	ErrInvalidResult  = errors.New("Worker result is invalid")
	ErrLeaseLost      = errors.New("Worker lease is no longer current")
	ErrResultConflict = errors.New("Worker lease already completed with a different result")
)
