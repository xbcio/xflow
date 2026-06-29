package main

import (
	"log"
	"os"
)

func main() {
	if err := executeRoot(os.Args[1:]...); err != nil {
		log.Fatal(err)
	}
}
