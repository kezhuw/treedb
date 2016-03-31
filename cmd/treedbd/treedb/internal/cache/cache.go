package cache

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type Kind int

const (
	KindNone Kind = iota
	KindPassby
	KindTimeout
	KindChildren
)

type Node interface {
	Kind() Kind
	Field(key string) Node
	Duration() time.Duration
}

var _ Node = ((*Tree)(nil))
var _ Node = ((*cacheNothing)(nil))
var _ Node = ((cacheAlways)(0))
var _ Node = ((cacheDuration)(0))

type CollectedType *struct{}

var (
	CollectedObject      interface{} = ((*CollectedType)(nil))
	DefaultCachedTimeout             = time.Minute * 5
)

var (
	notCached   Node = ((*cacheNothing)(nil))
	NeverCached Node = ((*cacheNever)(nil))
)

type cacheNothing struct{}

func (*cacheNothing) Kind() Kind {
	return KindNone
}

func (*cacheNothing) Field(key string) Node {
	return notCached
}

func (*cacheNothing) Duration() time.Duration {
	return 0
}

type cacheNever struct{}

func (*cacheNever) Kind() Kind {
	return KindNone
}

func (*cacheNever) Field(key string) Node {
	return NeverCached
}

func (*cacheNever) Duration() time.Duration {
	return 0
}

type cacheAlways time.Duration

func (cacheAlways) Kind() Kind {
	return KindChildren
}

func (d cacheAlways) Field(key string) Node {
	return d
}

func (d cacheAlways) Duration() time.Duration {
	return time.Duration(d)
}

type cacheDuration time.Duration

func (cacheDuration) Kind() Kind {
	return KindTimeout
}

func (d cacheDuration) Field(key string) Node {
	return cacheAlways(time.Duration(d))
}

func (d cacheDuration) Duration() time.Duration {
	return time.Duration(d)
}

type Tree struct {
	fields map[string]Node
}

func (*Tree) Kind() Kind {
	return KindPassby
}

func (t *Tree) Field(key string) Node {
	for _, k := range []string{key, "*"} {
		if node, ok := t.fields[k]; ok {
			return node
		}
	}
	return notCached
}

func (t *Tree) Duration() time.Duration {
	return 0
}

func (t *Tree) addField(path, key string, timeout int64) error {
	if len(key) == 0 {
		return fmt.Errorf("treedb/cache: traling separator in path %q", path)
	}
	switch t.fields[key].(type) {
	case nil:
		if key != "*" {
			if _, ok := t.fields["*"].(*Tree); ok {
				return fmt.Errorf("treedb/cache: conflicted path: %s", path)
			}
		}
		switch {
		case timeout < 0:
			t.fields[key] = NeverCached
		case timeout == 0:
			if key == "*" {
				return fmt.Errorf("treedb/cache: set wildcard terminated path %s no timeout", path)
			}
			t.fields[key] = notCached
		default:
			t.fields[key] = cacheDuration(timeout)
		}
		return nil
	case *Tree:
		return fmt.Errorf("treedb/cache: conflicted path: %s", path)
	default:
		return fmt.Errorf("treedb/cache: duplicated path: %s", path)
	}
}

func (t *Tree) addPath(path string, timeout int64) error {
	parts := strings.Split(path[1:], tree.Separator)
	for _, key := range parts[:len(parts)-1] {
		if len(key) == 0 {
			return fmt.Errorf("treedb/cache: invalid path: %s", path)
		}
		switch node := t.fields[key].(type) {
		case nil:
			if key != "*" {
				switch t.fields["*"].(type) {
				case nil:
				case *Tree:
				default:
					return fmt.Errorf("treedb/cache: conflicted path: %s", path)
				}
			}
			child := &Tree{fields: make(map[string]Node)}
			t.fields[key] = child
			t = child
		case *Tree:
			t = node
		default:
			return fmt.Errorf("treedb/cache: conflicted path: %s", path)
		}
	}
	return t.addField(path, parts[len(parts)-1], timeout)
}

func SizeofKey(key string) int {
	return len(key) + int(unsafe.Sizeof(key))
}

func SizeofBytes(bytes []byte) int {
	return len(bytes) + int(unsafe.Sizeof(bytes))
}

type state struct {
	now       time.Time
	wait      sync.WaitGroup
	version   uint64
	threshold int64
	timeScale uint64
}

func (state *state) reduceMemory(n int) {
	atomic.AddInt64(&state.threshold, -int64(n))
}

func (state *state) releaseSnapshot(f *tree.Field) {
	if f.Snapshot != nil && f.Version <= state.version {
		f.Snapshot.Close()
		f.Version, f.Snapshot = 0, nil
	}
}

func (state *state) ScaleTimeout(d time.Duration) time.Duration {
	return time.Duration(uint64(d) / state.timeScale)
}

func (state *state) collectVersion(t *tree.Tree, version uint64, snapshot leveldb.Snapshot, size *int) (uint64, leveldb.Snapshot) {
	t.Lock()
	defer t.Unlock()
	for k, f := range t.Fields {
		f.Lock()
		if f.Version != 0 {
			switch {
			case f.Version > version:
				if snapshot != nil {
					snapshot.Close()
				}
				version, snapshot = f.Version, f.Snapshot
			default:
				f.Snapshot.Close()
			}
			f.Version, f.Snapshot = 0, nil
		}
		switch v := f.Value.(type) {
		case *tree.Tree:
			version, snapshot = state.collectVersion(v, version, snapshot, size)
		case []byte:
			*size += len(v)
		}
		*size += SizeofKey(k) + tree.FieldSize
		f.Unlock()
	}
	*size += tree.TreeSize
	return version, snapshot
}

func (state *state) collectField(t *tree.Tree, key string, f *tree.Field, size int) {
	switch f.Version {
	case 0:
		delete(t.Fields, key)
		if f.Value != nil {
			t.Full = false
		}
		size += SizeofKey(key) + tree.FieldSize
	default:
		f.Value = CollectedObject
		f.Unlock()
	}
	state.reduceMemory(size)
}

func (state *state) collectPath(t *tree.Tree, cache *Tree, d time.Duration, k string, f *tree.Field) {
	switch v := f.Value.(type) {
	case *tree.Tree:
		v.Lock()
		switch len(v.Fields) {
		case 0:
			// No other visitors are visiting this field and its children,
			// so we can delete it.
			delete(t.Fields, k)
			t.Full = false
			state.reduceMemory(SizeofKey(k) + tree.FieldSize + tree.TreeSize)
		default:
			f.Unlock()
			state.wait.Add(1)
			go state.collectTree(v, cache.Field(k))
		}
	default:
		switch d >= state.ScaleTimeout(DefaultCachedTimeout) {
		case true:
			var size int
			if b, ok := f.Value.([]byte); ok {
				size = SizeofBytes(b)
			}
			state.collectField(t, k, f, size)
		default:
			f.Unlock()
		}
	}
}

func (state *state) collectTree(t *tree.Tree, cache Node) {
	defer t.Unlock()
	defer state.wait.Done()
	var timeout time.Duration
	for k, f := range t.Fields {
		// After locked, no new visitors will enter this field.
		// But its children may be locked by other visitors already.
		f.Lock()
		state.releaseSnapshot(f)
		d := state.now.Sub(f.Touch)
		switch c := cache.(type) {
		case *Tree:
			state.collectPath(t, c, d, k, f)
			continue
		case *cacheNothing:
			timeout = DefaultCachedTimeout
		case cacheAlways:
			timeout = 0
		case cacheDuration:
			timeout = time.Duration(c)
		}
		switch {
		case timeout != 0 && d >= state.ScaleTimeout(timeout):
			var size int
			switch v := f.Value.(type) {
			case *tree.Tree:
				f.Version, f.Snapshot = state.collectVersion(v, f.Version, f.Snapshot, &size)
				state.releaseSnapshot(f)
			case []byte:
				size = SizeofBytes(v)
			}
			state.collectField(t, k, f, size)
		default:
			if subTree, ok := f.Value.(*tree.Tree); ok {
				state.wait.Add(1)
				subTree.Lock()
				go state.collectTree(subTree, cache.Field(k))
			}
			f.Unlock()
		}
	}
}

func (state *state) Collect(threshold int64, version uint64, root *tree.Tree, cache *Tree) int64 {
	state.now = time.Now()
	state.version = version
	state.threshold = threshold
	state.timeScale = 1
	var i int
	for i < 10 && state.threshold >= 0 {
		state.wait.Add(1)
		root.Lock()
		state.collectTree(root, cache)
		state.wait.Wait()
		i++
		state.timeScale *= 10
	}
	return threshold - state.threshold
}

func Collect(threshold int64, version uint64, root *tree.Tree, cache *Tree) int64 {
	var state state
	return state.Collect(threshold, version, root, cache)
}

func Build(paths map[string]int64) (*Tree, error) {
	root := &Tree{fields: make(map[string]Node, len(paths))}
	for path, d := range paths {
		err := root.addPath(path, d)
		if err != nil {
			return nil, err
		}
	}
	return root, nil
}
