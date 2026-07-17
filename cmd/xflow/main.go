// Package main implements the xflow administration CLI.
//
// xflow provides operational subcommands for inspecting and reconciling
// durable scheduling state. It is intentionally separate from the server and
// runner processes: commands connect directly to the configured StateStore,
// invoke backend capabilities, and exit. They never start a task consumer or
// OutboxDispatcher, so running them does not interfere with an active
// control plane beyond the explicit state mutation each command performs.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := executeRoot(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, "xflow:", err)
		os.Exit(1)
	}
}
