package main

import (
	"fmt"

	"github.com/kezhuw/treedb/cmd/treedb/cmds"
	"github.com/peterh/liner"
)

func autocomplete(line string) []string {
	suggests := cmds.Complete(line)
	for i, s := range suggests {
		suggests[i] = s + " "
	}
	return suggests
}

func main() {
	cli := liner.NewLiner()
	defer cli.Close()

	cli.SetCompleter(autocomplete)

	fmt.Printf(" Type help for usage.\n")

	state := &State{}
	for {
		line, err := cli.Prompt("> ")
		if err != nil {
			break
		}
		cli.AppendHistory(line)

		result, usage, err := state.Execute(line)
		switch {
		case err != nil:
			fmt.Println("error:", err)
		case result != "":
			fmt.Println("result:", result)
		}
		if usage != "" {
			fmt.Println(usage)
		}
	}
	fmt.Printf("\n")
}
