package protocol

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

const (
	OK        = 0  // ok response
	ERROR     = 1  // error response
	HANDSHAKE = 2  // version selection
	OPEN      = 3  // open database
	DROP      = 4  // drop database
	LIST      = 5  // list databases
	CLOSE     = 6  // close database
	CACHE     = 7  // config cache path
	TOUCH     = 8  // update in memory key's timestamp
	GET       = 9  // get key
	SET       = 10 // set key
	DELETE    = 11 // delete key
)
