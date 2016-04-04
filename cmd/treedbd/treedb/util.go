package treedb

import (
	"bytes"
	"fmt"
)

var zeroBytes = []byte{}

func dupBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}

func bprintf(format string, args ...interface{}) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, format, args...)
	return buf.Bytes()
}

func copyCachePaths(paths map[string]int64) map[string]int64 {
	newly := make(map[string]int64, len(paths)+1)
	for k, v := range paths {
		newly[k] = v
	}
	return newly
}
