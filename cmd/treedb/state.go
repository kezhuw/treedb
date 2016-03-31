package main

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/kezhuw/treedb/cmd/treedb/client"
	"github.com/kezhuw/treedb/cmd/treedb/cmds"
)

var (
	ErrNoClient = errors.New("no client connected")
)

const (
	leftPadding   = "    "
	notFoundUsage = "command not found.\n using help to get full list of commands.\n"
)

var (
	widthOfCommandName = func() int {
		n := 0
		for _, cmd := range cmds.Commands {
			if len(cmd.Name) > n {
				n = len(cmd.Name)
			}
		}
		return ((n + 7) / 4) * 4
	}()
	alignedFullUsage = func() string {
		var buf bytes.Buffer
		for _, cmd := range cmds.Commands {
			fmt.Fprintf(&buf, "%s%-*s%s\n", leftPadding, widthOfCommandName, cmd.Name, cmd.Info)
			fmt.Fprintf(&buf, "%s%-*s%s\n", leftPadding, widthOfCommandName, "", cmd.Usage)
		}
		return string(buf.Bytes())
	}()
	cmdUsages = make(map[*cmds.Command]string)
)

func alignedCommandUsage(cmd *cmds.Command) string {
	if s, ok := cmdUsages[cmd]; ok {
		return s
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%-*s%s\n", leftPadding, widthOfCommandName, cmd.Name, cmd.Info)
	fmt.Fprintf(&buf, "%s%-*s%s\n", leftPadding, widthOfCommandName, "", cmd.Usage)
	s := string(buf.Bytes())
	cmdUsages[cmd] = s
	return s
}

func handleAuxCommand(name string, args string) string {
	name = strings.ToLower(name)

	if name == "version" {
		return "0.0.0"
	}

	if name == "help" {
		return handleHelpCommand(args)
	}

	return ""
}

func handleHelpCommand(args string) string {
	if args == "" {
		return alignedFullUsage
	}
	cmd := cmds.Find(args)
	if cmd == nil {
		return notFoundUsage
	}
	return alignedCommandUsage(cmd)
}

type State struct {
	client *client.State
}

func (s *State) Execute(line string) (string, string, error) {
	var name string
	var args string
	line = strings.TrimFunc(line, unicode.IsSpace)
	switch i := strings.IndexFunc(line, unicode.IsSpace); i {
	case -1:
		name = line
	default:
		name = line[:i]
		args = strings.TrimFunc(line[i:], unicode.IsSpace)
	}

	usage := handleAuxCommand(name, args)
	if usage != "" {
		return "", usage, nil
	}

	cmd := cmds.Find(name)
	if cmd == nil {
		return "", notFoundUsage, nil
	}

	switch cmd.Kind {
	case cmds.Client:
		result, err := s.executeCommand(cmd, args)
		return result, "", err
	case cmds.System, cmds.Database:
		if s.client == nil {
			return "", "", ErrNoClient
		}
		result, err := s.client.ExecuteCommand(cmd, args)
		if err == client.ErrInvalidArguments {
			usage = alignedCommandUsage(cmd)
		}
		return result, usage, err
	}

	return "", "", nil
}

func (s *State) executeCommand(cmd *cmds.Command, args string) (string, error) {
	if cmd.Name != "connect" {
		return "", errors.New("unsupported command")
	}
	return s.handleConnect(args)
}

func (s *State) handleConnect(addr string) (string, error) {
	c, err := client.Dial(addr)
	if err != nil {
		return "", err
	}
	if s.client != nil {
		go s.client.Close()
	}
	s.client = c
	return "OK", nil
}
