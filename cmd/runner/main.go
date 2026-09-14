package main

import (
	"log"
	"os"

	"github.com/xbcio/xflow/sdk/runner"
)

func main() {
	if err := runner.Execute(os.Args[1:]...); err != nil {
		log.Fatal(err)
	}
}
