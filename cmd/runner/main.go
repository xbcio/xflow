package main

import (
	"log"
	"os"

	"github.com/xbcio/xflow/cmd/internal/xbcinfra"
	"github.com/xbcio/xflow/sdk/runner"
)

func main() {
	args, err := xbcinfra.RunnerArgs(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	// Flush the xbc log sinks before exiting, including when the command
	// fails: sdk/runner returns the error, so the deferred close still runs.
	runErr := runner.Execute(args...)
	if closeErr := xbcinfra.CloseLog(); closeErr != nil {
		log.Printf("xflow-runner: close xbc log: %v", closeErr)
	}
	if runErr != nil {
		log.Fatal(runErr)
	}
}
