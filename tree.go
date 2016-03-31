package treedb

type Tree struct {
	db   *DB
	root string
}

func (t *Tree) Get(path string, kind FieldType) (interface{}, error) {
	return t.db.get(t.root, path, kind)
}

func (t *Tree) Set(path string, value interface{}) error {
	return t.db.set(t.root, path, value)
}

func (t *Tree) Delete(path string) error {
	return t.db.delete(t.root, path)
}

func (t *Tree) Cache(path string, timeout int64) error {
	return t.db.cache(t.root, path, timeout)
}

func (t *Tree) Touch(path string) error {
	return t.db.touch(t.root, path)
}

func (t *Tree) Tree(path string) *Tree {
	switch {
	case path == "" || path == "/":
		return t
	case path[0] == '/':
		path = path[1:]
	}
	switch last := len(path) - 1; path[last] {
	case '/':
		return &Tree{db: t.db, root: t.root + path}
	default:
		return &Tree{db: t.db, root: t.root + path + "/"}
	}
}
