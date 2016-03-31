package treedb

type FieldType uint

const (
	FieldAny = iota
	FieldTree
	FieldBinary
)

const (
	CachePathNever   = -2
	CachePathDelete  = -1
	CachePathDefault = 0
)
