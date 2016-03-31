package treedb

import "errors"

var ErrCorruptedData = errors.New("treedb: corrupted data")

type NotFoundError struct {
	Path string
}

func (e *NotFoundError) Error() string {
	return "treedb: path " + e.Path + " not found"
}

type NotTreeError struct {
	Path string
}

func (e *NotTreeError) Error() string {
	return "treedb: path " + e.Path + " is not a tree"
}

type InvalidPathError struct {
	Path string
}

func (e *InvalidPathError) Error() string {
	return "treedb: invalid path: " + e.Path
}
