package evidence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// MergeRawEnvelopes reads every non-diagnostic .json envelope fragment in dir
// and merges their raw records into a single envelope. All fragments must
// share one RunID — checkRawLedgerIntegrity rejects cross-run records, so the
// merge keeps the first fragment's RunID and does not synthesize one.
//
// The merge concatenates Raw.RuntimeEvents / CounterSnapshots /
// ProtocolObservations / StateSnapshots / SuiteRecords without de-duplication
// or aggregation; the verifier independently recomputes derived_observations
// from the concatenated raw ledger. DerivedObservations and Verification from
// fragment envelopes are discarded (the verifier recomputes them).
//
// An empty directory (or one with no usable fragment) is an error so the CLI
// fails loudly instead of producing a misleading empty-ledger verification.
func MergeRawEnvelopes(dir string) (*Envelope, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read raw dir %q: %w", dir, err)
	}

	// Stable order so the merge is deterministic regardless of filesystem
	// listing order.
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		if strings.Contains(name, ".diagnostic.") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("no raw envelopes in dir %q", dir)
	}

	merged := &Envelope{SchemaVersion: SchemaVersion}
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read fragment %q: %w", path, err)
		}
		var frag Envelope
		if err := json.Unmarshal(data, &frag); err != nil {
			return nil, fmt.Errorf("parse fragment %q: %w", path, err)
		}
		if frag.RunID == "" {
			// A fragment without a RunID cannot be safely merged: the integrity
			// check would flag every record as a cross-run reference. Skip it
			// only if it is genuinely empty; otherwise treat as an error so a
			// misconfigured run surfaces immediately.
			if isEmptyFragment(&frag) {
				continue
			}
			return nil, fmt.Errorf("fragment %q has empty run_id", path)
		}
		if merged.RunID == "" {
			merged.RunID = frag.RunID
			merged.SchemaVersion = frag.SchemaVersion
			merged.StartedAt = frag.StartedAt
			// Source provenance is stamped by the recorder at test time (real
			// git/binary/go-version observations, not fabrications). The merge
			// keeps the first fragment's Source so the verifier can compare it
			// against its own independent recomputation. All fragments in one
			// run stamp identical Source (HEAD/binary/go-version do not change
			// during a single test invocation); keeping the first is therefore
			// representative. Environment / Suite are still NOT merged: they are
			// recomputed by the verifier (Suite) or optional (Environment).
			merged.Source = frag.Source
		}
		if frag.RunID != merged.RunID {
			return nil, fmt.Errorf("fragment %q run_id %q != %q (cross-run merge refused)", path, frag.RunID, merged.RunID)
		}
		merged.Raw.RuntimeEvents = append(merged.Raw.RuntimeEvents, frag.Raw.RuntimeEvents...)
		merged.Raw.CounterSnapshots = append(merged.Raw.CounterSnapshots, frag.Raw.CounterSnapshots...)
		merged.Raw.ProtocolObservations = append(merged.Raw.ProtocolObservations, frag.Raw.ProtocolObservations...)
		merged.Raw.StateSnapshots = append(merged.Raw.StateSnapshots, frag.Raw.StateSnapshots...)
		merged.Raw.SuiteRecords = append(merged.Raw.SuiteRecords, frag.Raw.SuiteRecords...)
		// Environment / Suite are recomputed by the verifier; do not merge
		// self-reported values. Source is an exception: it is the test-time
		// observed provenance the verifier compares against (see above).
	}
	if merged.RunID == "" {
		return nil, fmt.Errorf("no raw envelopes with run_id in dir %q", dir)
	}
	return merged, nil
}

// isEmptyFragment reports whether the fragment carries no raw records at all.
func isEmptyFragment(env *Envelope) bool {
	return len(env.Raw.RuntimeEvents) == 0 &&
		len(env.Raw.CounterSnapshots) == 0 &&
		len(env.Raw.ProtocolObservations) == 0 &&
		len(env.Raw.StateSnapshots) == 0 &&
		len(env.Raw.SuiteRecords) == 0
}
