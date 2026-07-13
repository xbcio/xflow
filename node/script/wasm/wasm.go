// Package wasm implements the wasm language family (wazero runtime) for the
// xflow.script node. Guests are WASI modules that read a JSON object from
// stdin and write a JSON object to stdout.
package wasm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// decodeCode turns the base64 node param into raw wasm bytes.
func decodeCode(code string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		return nil, fmt.Errorf("wasm: decode base64 module: %w", err)
	}
	return b, nil
}

// encodeStdin marshals the globals object written to guest stdin.
func encodeStdin(globals map[string]any) ([]byte, error) {
	if globals == nil {
		globals = map[string]any{}
	}
	return json.Marshal(globals)
}

// decodeStdout parses guest stdout into the raw completion value.
func decodeStdout(out []byte) (any, error) {
	if len(out) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(out, &v); err != nil {
		return nil, fmt.Errorf("wasm: decode stdout json: %w", err)
	}
	return v, nil
}
