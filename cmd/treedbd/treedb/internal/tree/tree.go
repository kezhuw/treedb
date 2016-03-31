package tree

import (
	"sync"
	"time"
	"unsafe"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
)

const (
	TreeSize  = int(unsafe.Sizeof(Tree{}) + 24)
	FieldSize = int(unsafe.Sizeof(Field{}) + 24)
)

const (
	Separator = "/"
)

type Field struct {
	sync.RWMutex
	Touch    time.Time
	Value    interface{}
	Version  uint64
	Snapshot leveldb.Snapshot
}

type ID uint64

type Tree struct {
	sync.Mutex
	ID     ID
	Full   bool
	Fields map[string]*Field
}

func (t *Tree) EnterNewField(key string) *Field {
	f := new(Field)
	f.Lock()
	t.Fields[key] = f
	t.Unlock()
	return f
}

func NewTree(id ID) *Tree {
	return &Tree{ID: id, Fields: make(map[string]*Field)}
}
