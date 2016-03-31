package stat

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/cache"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type version struct {
	current   uint64
	updated   uint64
	threshold uint64
}

type Memory struct {
	timeout  time.Duration
	usage    int64
	limit    int64
	version  version
	root     *tree.Tree
	cache    *cache.Tree
	changed  chan struct{}
	stopped  chan struct{}
	stopping chan struct{}
}

func (m *Memory) signal() {
	select {
	case m.changed <- struct{}{}:
	default:
	}
}

func (m *Memory) Add(n int) {
	atomic.AddInt64(&m.usage, int64(n))
	m.signal()
}

func (m *Memory) Sub(n int) {
	atomic.AddInt64(&m.usage, -int64(n))
	m.signal()
}

func (m *Memory) Update() {
	atomic.AddUint64(&m.version.updated, 1)
	m.signal()
}

func (m *Memory) threshold() int64 {
	return atomic.LoadInt64(&m.usage) - atomic.LoadInt64(&m.limit)
}

func (m *Memory) collect(threshold int64, updated uint64) {
	n := cache.Collect(threshold, updated, m.root, m.getCacheTree())
	m.version.current = updated
	m.Sub(int(n))
}

func (m *Memory) Start() {
	m.changed = make(chan struct{}, 1)
	m.stopped = make(chan struct{})
	m.stopping = make(chan struct{})
	go m.monitor()
}

func (m *Memory) Stop() {
	close(m.stopping)
	<-m.stopped
}

func (m *Memory) monitor() {
	for {
		select {
		case <-time.After(m.timeout):
			m.collect(0, atomic.LoadUint64(&m.version.updated))
		case <-m.changed:
			threshold := m.threshold()
			updated := atomic.LoadUint64(&m.version.updated)
			if threshold >= 0 || updated >= m.version.current+m.version.threshold {
				m.collect(threshold, updated)
			}
		case <-m.stopping:
			close(m.stopped)
			return
		}
	}
}

func (m *Memory) getCacheTree() *cache.Tree {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&m.cache))
	return (*cache.Tree)(atomic.LoadPointer(addr))
}

func (m *Memory) ResetCacheTree(c *cache.Tree) {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&m.cache))
	atomic.StorePointer(addr, (unsafe.Pointer)(c))
}

func NewMemory(root *tree.Tree, cache *cache.Tree, timeout time.Duration, version uint64, diffver uint64) *Memory {
	m := &Memory{root: root, cache: cache, timeout: timeout}
	m.version.current = version
	m.version.updated = version
	m.version.threshold = diffver
	return m
}
