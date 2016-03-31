package path

import (
	"strings"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type Path struct {
	Full string
	Segs []string
	Offs []int
}

func (path *Path) String() string {
	return path.Full
}

// Subpath [0, i]
func (path *Path) Sub(i int) string {
	off := path.Offs[i]
	seg := path.Segs[i]
	return path.Full[:off+len(seg)]
}

func (path *Path) Init(s string) bool {
	return path.init(s, false)
}

func (path *Path) InitPattern(s string) bool {
	return path.init(s, true)
}

func (path *Path) init(s string, wildcard bool) bool {
	if !check(s, wildcard) {
		return false
	}
	segs := strings.Split(s[1:], tree.Separator)
	offs := make([]int, len(segs))
	offset := 1
	for i, k := range segs {
		if k == "" {
			return false
		}
		offs[i] = offset
		offset += 1 + len(k)
	}
	path.Full = s
	path.Segs = segs
	path.Offs = offs
	return true
}

func New(path string) *Path {
	var p Path
	if p.Init(path) {
		return &p
	}
	return nil
}

func isValidChar(c byte) bool {
	switch {
	case '0' <= c && c <= '9':
	case 'a' <= c && c <= 'z':
	case 'A' <= c && c <= 'Z':
	case c == '-' || c == '_':
	case c == ':' || c == '|':
	default:
		return false
	}
	return true
}

func check(s string, wildcard bool) bool {
	if s == "" || s[0] != '/' {
		return false
	}
	var last byte = '/'
	for i, n := 1, len(s); i < n; i++ {
		c := s[i]
		switch c {
		case '/':
			if last == '/' {
				return false
			}
		case '*':
			switch {
			case !wildcard:
				return false
			case last != '/':
				return false
			}
		default:
			switch {
			case last == '*':
				return false
			case !isValidChar(c):
				return false
			}
		}
		last = c
	}
	return last != '/'
}
