package cmds

import (
	"sort"
	"strings"
)

type Kind int

const (
	Client Kind = iota
	System      = iota
	Database
)

type Command struct {
	Name  string
	Kind  Kind
	Info  string
	Usage string
}

var Commands = []Command{
	{"connect", Client, "connect treedb server", "connect ADDRESS(tcp[4|6]://host:port or unix://filepath)"},
	{"create", System, "create database", "create NAME [FROM TEMPLATE]"},
	{"select", System, "select database", "select NAME"},
	{"get", Database, "get value stored at path", "get PATH"},
	{"set", Database, "set value stored to path", "set PATH VALUE"},
	{"delete", Database, "delete key pointed by path", "delete PATH"},
	{"cache", Database, "set cache timeout for path", "cache PATH TIMEOUT(never or delete or 0 or time.Duration)"},
	{"touch", Database, "update timestamp for in memory path", "touch PATH"},
}

type byName []Command

func (a byName) Len() int           { return len(a) }
func (a byName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool { return a[i].Name < a[j].Name }

func init() {
	sort.Sort(byName(Commands))
}

func Find(name string) *Command {
	name = strings.ToLower(name)
	for i, cmd := range Commands {
		if cmd.Name == name {
			return &Commands[i]
		}
	}
	return nil
}

func Complete(name string) (suggests []string) {
	name = strings.ToLower(name)
	for _, cmd := range Commands {
		if strings.HasPrefix(cmd.Name, name) {
			suggests = append(suggests, cmd.Name)
		}
	}
	return
}
