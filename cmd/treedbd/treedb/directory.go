package treedb

import (
	"errors"
	"os"
	"sync"
	"unsafe"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/filer"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
)

var (
	ErrDBExist      = errors.New("treedb: db existed")
	ErrDBMissing    = errors.New("treedb: db missing")
	ErrInvalidParam = errors.New("treedb: invalid parameters")
	ErrDBNoTemplate = errors.New("treedb: template db missing")
)

type Directory struct {
	Dir       string
	zero      sync.WaitGroup
	file      *os.File
	writable  *os.File // Nil if readonly.
	mutex     sync.Mutex
	databases map[string]*entry
}

type entry struct {
	sync.Mutex
	db *DB
}

func lock2(m0 *sync.Mutex, m1 *sync.Mutex) {
	switch uintptr(unsafe.Pointer(m0)) < uintptr(unsafe.Pointer(m1)) {
	case true:
		m0.Lock()
		m1.Lock()
	default:
		m1.Lock()
		m0.Lock()
	}
}

func Listen(dir string, readonly bool) (*Directory, error) {
	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := file.Chdir(); err != nil {
		return nil, err
	}
	var writable *os.File
	if !readonly {
		writable, err = os.OpenFile("LOCK", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file.Close()
			return nil, err
		}
		if err := filer.LockFile(writable); err != nil {
			file.Close()
			writable.Close()
			return nil, err
		}
	}
	return &Directory{
		Dir:       dir,
		file:      file,
		writable:  writable,
		databases: make(map[string]*entry),
	}, nil
}

func (d *Directory) locate(name string) *entry {
	if e, ok := d.databases[name]; ok {
		return e
	}
	e := &entry{}
	d.databases[name] = e
	return e
}

func (d *Directory) Readonly() bool {
	return d.writable == nil
}

type OpenOptions struct {
	TemplateDB      string
	CreateIfMissing bool
	ErrorIfExists   bool
	readonly        bool
	snapshot        leveldb.Snapshot
	err             error
}

func (d *Directory) openEntry(e *entry, name string, reply chan interface{}) {
	defer d.zero.Done()
	defer e.Unlock()
	db, err := openDB(name, &OpenOptions{})
	if err != nil {
		reply <- err
		return
	}
	e.db = db
	reply <- db
}

func (d *Directory) createEntry(e *entry, name string, opts *OpenOptions, reply chan interface{}) {
	db, err := openDB(name, opts)
	e.db = db
	e.Unlock()
	d.zero.Done()
	replyCreation(reply, db, err, opts.err)
}

func checkOpenOptions(opts *OpenOptions) bool {
	if !opts.CreateIfMissing {
		return opts.TemplateDB == "" && opts.ErrorIfExists == false
	}
	return true
}

func (d *Directory) openTemplate(e *entry, from string, e1 *entry, name string, opts *OpenOptions, reply chan interface{}) {
	db, err := openDB(from, &OpenOptions{})
	if err != nil {
		e.Unlock()
		e1.Unlock()
		d.zero.Done()
		reply <- err
		return
	}
	e.db = db
	e.Unlock()
	d.cloneEntry(db, db.getSnapshot(), e1, name, opts, reply)
}

func replyCreation(reply chan interface{}, db *DB, err error, err1 error) {
	switch {
	case err != nil:
		reply <- err
	case err1 != nil:
		reply <- err1
	default:
		reply <- db
	}
}

func (d *Directory) cloneEntry(db *DB, ssReply chan interface{}, e *entry, name string, opts *OpenOptions, reply chan interface{}) {
	defer d.zero.Done()
	defer e.Unlock()
	result := <-ssReply
	if err, ok := result.(error); ok {
		reply <- err
		return
	}
	opts.snapshot = result.(leveldb.Snapshot)
	defer db.releaseSnapshot(opts.snapshot)
	db, err := openDB(name, opts)
	e.db = db
	replyCreation(reply, db, err, opts.err)
}

func (d *Directory) Open(name string, options *OpenOptions, reply chan interface{}) chan interface{} {
	if !checkOpenOptions(options) || name == "" {
		reply <- ErrInvalidParam
		return reply
	}
	options.readonly = d.Readonly()
	switch {
	case options.CreateIfMissing == false:
		d.open(name, reply)
	case options.TemplateDB == "":
		d.create(name, options, reply)
	default:
		d.clone(name, options, reply)
	}
	return reply
}

func (d *Directory) open(name string, reply chan interface{}) {
	d.mutex.Lock()
	d.zero.Add(1)
	e := d.locate(name)
	d.mutex.Unlock()
	e.Lock()
	if db := e.db; db != nil {
		e.Unlock()
		d.zero.Done()
		reply <- db
		return
	}
	go d.openEntry(e, name, reply)
}

func (d *Directory) create(name string, opts *OpenOptions, reply chan interface{}) {
	d.mutex.Lock()
	d.zero.Add(1)
	e := d.locate(name)
	d.mutex.Unlock()
	e.Lock()
	if db := e.db; db != nil {
		e.Unlock()
		switch opts.ErrorIfExists {
		case true:
			reply <- ErrDBExist
		default:
			reply <- db
		}
		return
	}
	go d.createEntry(e, name, opts, reply)
}

func (d *Directory) clone(name string, opts *OpenOptions, reply chan interface{}) {
	d.mutex.Lock()
	d.zero.Add(1)
	e0 := d.locate(opts.TemplateDB)
	e1 := d.locate(name)
	d.mutex.Unlock()
	lock2(&e0.Mutex, &e1.Mutex)
	if db := e1.db; db != nil {
		e1.Unlock()
		switch opts.ErrorIfExists {
		case true:
			reply <- ErrDBExist
		default:
			reply <- db
		}
		switch e0.db {
		case nil:
			reply := make(chan interface{}, 1)
			go d.openEntry(e0, opts.TemplateDB, reply)
		default:
			e0.Unlock()
			d.zero.Done()
		}
		return
	}
	switch db := e0.db; db {
	case nil:
		go d.openTemplate(e0, opts.TemplateDB, e1, name, opts, reply)
	default:
		e0.Unlock()
		go d.cloneEntry(db, db.getSnapshot(), e1, name, opts, reply)
	}
}

func (d *Directory) Delete(name string) error {
	d.mutex.Lock()
	d.zero.Add(1)
	defer d.zero.Done()
	e := d.locate(name)
	d.mutex.Unlock()
	e.Lock()
	defer e.Unlock()
	if e.db != nil {
		e.db.Close()
		e.db = nil
	}
	return os.RemoveAll(d.Dir + name)
}

// Close closes Directory. Ensure that no other calls after closing.
func (d *Directory) Close() {
	d.mutex.Lock()
	d.zero.Wait()
	defer d.mutex.Unlock()
	for _, e := range d.databases {
		if e.db != nil {
			e.db.Close()
			e.db = nil
		}
	}
	d.file.Close()
	if d.writable != nil {
		filer.UnlockFile(d.writable)
		d.writable.Close()
	}
}
