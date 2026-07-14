//go:build ignore

// spin is a WASI test guest that loops forever to exercise in-flight ctx
// cancellation. It never writes stdout.
package main

func main() {
	for {
	}
}
