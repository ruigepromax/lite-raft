package main

import "errors"

var (
	ErrStopped = errors.New("raft node stopped")
	ErrAborted = errors.New("request aborted")
)
