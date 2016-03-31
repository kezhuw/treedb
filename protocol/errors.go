package protocol

import "fmt"

const (
	EcodeDBClosed       = 0
	EcodeDBShutdown     = 1
	EcodeDBReadonly     = 2
	EcodePathNotFound   = 3
	EcodePathNotTree    = 4
	EcodeInvalidPath    = 5
	EcodeInvalidParam   = 6
	EcodeInvalidValue   = 7
	EcodeInvalidMessage = 8
	EcodeUnknownRequest = 9
	EcodeMismatchedType = 10
	EcodeInternalError  = 11
)

var errs = map[int]string{
	EcodeDBClosed:       "db closed",
	EcodeDBShutdown:     "db shutdown",
	EcodeDBReadonly:     "db readonly",
	EcodePathNotFound:   "not found",
	EcodePathNotTree:    "not tree",
	EcodeInvalidPath:    "invalid path",
	EcodeInvalidParam:   "invalid parameter",
	EcodeInvalidValue:   "invalid value",
	EcodeInvalidMessage: "invalid message",
	EcodeUnknownRequest: "unknown request",
	EcodeMismatchedType: "mismatched type",
	EcodeInternalError:  "server internal error",
}

type Error struct {
	Code int
	Info string
}

func (e *Error) Error() string {
	info := e.Info
	if info != "" {
		info = ", info: " + info
	}
	if str, ok := errs[e.Code]; ok {
		return "treedb: " + str + info
	}
	return fmt.Sprintf("treedb: unknown error code: %d%s", e.Code, info)
}
