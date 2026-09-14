package main

import (
	"log"
	"os"

	"github.com/xbcio/xflow/service/runnerapp"
)

func main() {
	if err := runnerapp.Execute(os.Args[1:]...); err != nil {
		log.Fatal(err)
	}
}
