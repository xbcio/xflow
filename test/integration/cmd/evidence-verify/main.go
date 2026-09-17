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

		// Release-harness inputs. Everything else in the schema-v3 `release`
		// block is recomputed from git, the pinned toolchain files, and the Go
		// runtime; these four things exist only in the harness that invoked the
		// gate. They are flags rather than environment lookups so the recorded
		// values are visible in the command the gate printed.
		gateName        = flag.String("gate-name", "", "release gate name, e.g. g0 (required)")
		gateCommand     = flag.String("gate-command", "", "exact command that ran this gate, e.g. \"make test-g0-evidence-required\" (required)")
		gateStartedAt   = flag.String("gate-started-at", "", "RFC3339 UTC time the gate started (required)")
		containerImages = flag.String("container-images", "", "dependency images as component=reference[@sha256:<digest>] separated by ';' (required)")
		reviewer        = flag.String("reviewer", "", "human who reviewed this evidence; empty records an unsigned artifact")
		reRunner        = flag.String("re-runner", "", "human who re-ran this gate on the candidate SHA; empty records an unsigned artifact")
		unverifiedScope = flag.String("unverified-scope", "", "claims this artifact does NOT establish, separated by ';' (required)")
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

	prov := evidence.RealProvenance{
		TestBinaryPath: *binaryPath,
		ReleaseInput: evidence.ReleaseInput{
			GateName:        *gateName,
			GateCommand:     *gateCommand,
			GateStartedAt:   *gateStartedAt,
			ContainerImages: *containerImages,
			Reviewer:        *reviewer,
			ReRunner:        *reRunner,
			UnverifiedScope: *unverifiedScope,
		},
	}
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
	defer func() { _ = f.Close() }()

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
