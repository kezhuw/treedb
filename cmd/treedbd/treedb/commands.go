package treedb

import "github.com/kezhuw/treedb/cmd/treedbd/treedb/path"

type Command interface {
	TreeDBCommand()
}

type GetCommand struct {
	Path path.Path
	Type FieldType
}

type SetCommand struct {
	Path  path.Path
	Value interface{}
}

type DeleteCommand struct {
	Path path.Path
}

type CacheCommand struct {
	Path    path.Path
	Timeout int64
}

type TouchCommand struct {
	Path path.Path
}

type closeCommand struct {
}

type snapshotCommand struct {
}

func (*GetCommand) TreeDBCommand()    {}
func (*SetCommand) TreeDBCommand()    {}
func (*DeleteCommand) TreeDBCommand() {}
func (*CacheCommand) TreeDBCommand()  {}
func (*TouchCommand) TreeDBCommand()  {}

func (*closeCommand) TreeDBCommand()    {}
func (*snapshotCommand) TreeDBCommand() {}
