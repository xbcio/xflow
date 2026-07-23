package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/xbcio/xflow/test/integration/internal/evidence"
)

func main() {
	var (
		inPath       = flag.String("in", "", "path to go test -json output")
		rawDir       = flag.String("raw", "", "directory containing raw ledger envelope(s)")
		manifestPath = flag.String("manifest", "", "path to manifest (optional, compiled manifest used by default)")
		outDir       = flag.String("out", "test/integration/testdata/evidence", "output directory for final artifact")
		binaryPath   = flag.String("binary", os.Args[0], "path to the test binary that produced the evidence")
	)
	flag.Parse()

	if *inPath == "" || *rawDir == "" {
		fmt.Fprintln(os.Stderr, "usage: evidence-verify -in <go-test-json> -raw <raw-dir> [-out <dir>] [-binary <path>]")
		os.Exit(2)
	}
	_ = manifestPath // compiled manifest is used; flag accepted for future extensibility

	suiteEvents, err := readGoTestJSON(*inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read test json: %v\n", err)
		os.Exit(1)
	}

	env, err := evidence.MergeRawEnvelopes(*rawDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read raw ledger: %v\n", err)
		os.Exit(1)
	}

	prov := evidence.RealProvenance{TestBinaryPath: *binaryPath}
	v := evidence.NewVerifier(prov)
	res := v.Verify(env, suiteEvents)

	if !res.Passed {
		fmt.Fprintln(os.Stderr, "verification failed:")
		for _, e := range res.Errors {
			fmt.Fprintf(os.Stderr, "  - %s\n", e)
		}
		// Write diagnostic artifact without overwriting any final artifact.
		if diag, err := evidence.AtomicWriteDiagnostic(env, *outDir); err == nil {
			fmt.Fprintf(os.Stderr, "diagnostic artifact: %s\n", diag)
		}
		os.Exit(1)
	}

	artifactPath, digestPath, err := evidence.AtomicFinalize(env, *outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to finalize artifact: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("artifact: %s\n", artifactPath)
	fmt.Printf("digest:   %s\n", digestPath)
}

func readGoTestJSON(path string) ([]evidence.GoTestEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []evidence.GoTestEvent
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev evidence.GoTestEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return nil, fmt.Errorf("parse line %q: %w", line, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
