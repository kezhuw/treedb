package client

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/kezhuw/treedb"
	"github.com/kezhuw/treedb/cmd/treedb/cmds"
)

var (
	ErrNoDB               = errors.New("no database selected")
	ErrInvalidArguments   = errors.New("invalid arguments")
	ErrUnsupportedCommand = errors.New("unsupported command")
)

type State struct {
	client *treedb.Client
	db     *treedb.DB
}

func Dial(addr string) (*State, error) {
	client, err := treedb.Dial(addr, nil)
	if err != nil {
		return nil, err
	}
	return &State{client: client}, nil
}

type handlerFunc func(*State, string) (string, error)

type handlerInfo struct {
	Cmd  *cmds.Command
	Func handlerFunc
}

var handlers = make(map[string]handlerInfo)

func regHandler(name string, f handlerFunc) {
	cmd := cmds.Find(name)
	if cmd == nil {
		panic(fmt.Errorf("no command: %s", name))
	}
	handlers[cmd.Name] = handlerInfo{cmd, f}
}

func init() {
	regHandler("select", (*State).handleSelectDB)
	regHandler("create", (*State).handleCreateDB)

	regHandler("get", (*State).handleGet)
	regHandler("set", (*State).handleSet)
	regHandler("delete", (*State).handleDelete)
	regHandler("cache", (*State).handleCache)
	regHandler("touch", (*State).handleTouch)
}

func (s *State) ExecuteCommand(cmd *cmds.Command, args string) (string, error) {
	h, ok := handlers[cmd.Name]
	if !ok {
		return "", ErrUnsupportedCommand
	}
	if cmd.Kind == cmds.Database && s.db == nil {
		return "", ErrNoDB
	}
	return h.Func(s, args)
}

func splitArgs(s string, n int) (args []string) {
	s = strings.TrimFunc(s, unicode.IsSpace)
	for s != "" {
		i := strings.IndexFunc(s, unicode.IsSpace)
		if i == -1 || n == 1 {
			args = append(args, s)
			break
		}
		n--
		args = append(args, s[:i])
		s = strings.TrimFunc(s[i:], unicode.IsSpace)
	}
	return args
}

func (s *State) handleSelectDB(name string) (string, error) {
	switch {
	case name == "":
		fallthrough
	case strings.IndexFunc(name, unicode.IsSpace) != -1:
		return "", ErrInvalidArguments
	}
	db, err := s.client.OpenDB(name, nil)
	if err != nil {
		return "", err
	}
	if s.db != nil {
		go s.db.Close()
	}
	s.db = db
	return "OK", nil
}

func (s *State) handleCreateDB(str string) (string, error) {
	args := splitArgs(str, 0)
	var name, from string
	switch {
	case len(args) == 1:
		name = args[0]
	case len(args) == 3 && strings.ToLower(args[1]) == "from":
		name = args[0]
		from = args[2]
	default:
		return "", ErrInvalidArguments
	}
	db, err := s.client.OpenDB(name, &treedb.OpenOptions{TemplateDB: from, CreateIfMissing: true, ErrorIfExists: true})
	if err != nil {
		return "", err
	}
	go db.Close()
	return "OK", nil
}

func (s *State) handleGet(path string) (string, error) {
	if path == "" {
		return "", ErrInvalidArguments
	}
	value, err := s.db.Get(path, treedb.FieldAny)
	if err != nil {
		return "", err
	}
	switch value := value.(type) {
	case []byte:
		return string(value), nil
	case map[string]interface{}:
		return "key is a tree, XXX: How to show?", nil
	default:
		return "", fmt.Errorf("unexpected value type: %s", reflect.TypeOf(value))
	}
}

func (s *State) handleSet(str string) (string, error) {
	args := splitArgs(str, 2)
	if len(args) != 2 {
		return "", ErrInvalidArguments
	}
	err := s.db.Set(args[0], []byte(args[1]))
	if err != nil {
		return "", err
	}
	return "OK", nil
}

func (s *State) handleDelete(path string) (string, error) {
	if path == "" {
		return "", ErrInvalidArguments
	}
	err := s.db.Delete(path)
	if err != nil {
		return "", err
	}
	return "OK", nil
}

func (s *State) handleCache(str string) (string, error) {
	args := splitArgs(str, 0)
	if len(args) != 2 {
		return "", ErrInvalidArguments
	}
	var timeout int64
	switch args[1] {
	case "never":
		timeout = treedb.CachePathNever
	case "delete":
		timeout = treedb.CachePathDelete
	default:
		d, err := time.ParseDuration(args[1])
		if err != nil {
			return "", ErrInvalidArguments
		}
		timeout = int64(d)
	}
	err := s.db.Cache(args[0], timeout)
	if err != nil {
		return "", err
	}
	return "OK", nil
}

func (s *State) handleTouch(path string) (string, error) {
	if path == "" {
		return "", ErrInvalidArguments
	}
	err := s.db.Touch(path)
	if err != nil {
		return "", err
	}
	return "OK", nil
}

func (s *State) Close() {
	s.client.Close()
}
