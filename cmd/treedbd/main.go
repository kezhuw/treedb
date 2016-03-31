package main

import (
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/kezhuw/treedb/cmd/treedbd/config"
	"github.com/kezhuw/treedb/cmd/treedbd/server"
)

var usage = `Usage:
--config              filename     File contains configures
--listen              address      Local address to listen on
--data-dir            directory    Filesystem directory to store databases
--data-readonly       readonly     Whether data directory is readonly or not
--shutdown-timeout    duration     Wait duration before closing online client connection forcibly
`

func catchMissingArgumentError(args []string, i *int, err *error) {
	if r := recover(); r != nil {
		switch r.(type) {
		case runtime.Error:
			*err = fmt.Errorf("missing value of option %s", args[*i])
		default:
			panic(r)
		}
	}
}

func parse(cfg *config.Config, args []string) (err error) {
	var i = 0
	defer catchMissingArgumentError(args, &i, &err)
	for i < len(args) {
		switch option := args[i]; option {
		case "--config":
			err := config.Load(cfg, args[i+1])
			if err != nil {
				return err
			}
			i += 2
		case "--listen":
			cfg.Listens = append(cfg.Listens, args[i+1])
			i += 2
		case "--data-dir":
			cfg.DataDir = args[i+1]
			i += 2
		case "--data-readonly":
			switch {
			case i+1 < len(args) && !strings.HasPrefix(args[i+1], "--"):
				readonly, err := strconv.ParseBool(args[i+1])
				if err != nil {
					return fmt.Errorf("invalid bool value for option %s: %s", option, args[i+1])
				}
				cfg.DataReadonly = readonly
				i += 2
			default:
				cfg.DataReadonly = true
				i++
			}
		case "--shutdown-timeout":
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return fmt.Errorf("invalid duration value for option %s: %s", option, args[i+1])
			}
			cfg.ShutdownTimeout = config.Duration(d)
			i += 2
		default:
			return fmt.Errorf("unrecognized config option: %s", option)
		}
	}
	return nil
}

func main() {
	var cfg config.Config
	err := parse(&cfg, os.Args[1:])
	if err != nil {
		printUsage(err)
	}

	if cfg.DataDir == "" {
		printUsage(errors.New("Data directory is empty"))
	}
	if len(cfg.Listens) == 0 {
		printUsage(errors.New("No local addresses to listen on"))
	}

	server, err := server.Listen(&cfg)
	if err != nil {
		printUsage(err)
	}
	go http.ListenAndServe(":9090", nil)
	server.Serve()
}

func printUsage(err error) {
	switch err {
	case nil:
		fmt.Printf("%s\n", usage)
		os.Exit(0)
	default:
		fmt.Printf("%s\n\n", err)
		fmt.Printf("%s\n", usage)
		os.Exit(1)
	}
}
