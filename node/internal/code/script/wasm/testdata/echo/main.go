//go:build ignore

// echo is a minimal WASI test guest: reads a JSON object from stdin and writes
// a JSON object to stdout. Compiled with GOOS=wasip1 GOARCH=wasm.
package main

import (
	"encoding/json"
	"io"
	"os"
)

func main() {
	raw, _ := io.ReadAll(os.Stdin)
	var in map[string]any
	_ = json.Unmarshal(raw, &in)

	out := map[string]any{"echo": in["$input"]}
	if creds, ok := in["$credentials"].(map[string]any); ok {
		if ak, ok := creds["aes_key"].(map[string]any); ok {
			out["credKey"] = ak["key"]
		}
	}
	if c, ok := in["$credential"].(map[string]any); ok {
		out["firstToken"] = c["token"]
	}
	if _, err := os.Open("/etc/hostname"); err != nil {
		out["fsBlocked"] = true
	}

	b, _ := json.Marshal(out)
	_, _ = os.Stdout.Write(b)
}
