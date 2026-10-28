//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// dpLeaf is one entry of a document in reading order: either an object key
// (isKey, val empty) or a scalar. path is the dotted path from the root with
// "[]" for array elements, the same shape dpVolatilePaths uses.
type dpLeaf struct {
	path  string
	val   string
	isKey bool
}

// dpLeaves lists the keys and scalars of doc in the order they appear. It reads
// the document independently of the code under test, so a field the normalizer
// loses, reorders or rewrites shows up as a difference between two listings.
func dpLeaves(t *testing.T, doc []byte) []dpLeaf {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var out []dpLeaf
	dpReadLeaves(t, dec, "", &out)
	return out
}

func dpReadLeaves(t *testing.T, dec *json.Decoder, path string, out *[]dpLeaf) {
	t.Helper()
	tok, err := dec.Token()
	if err != nil {
		t.Fatalf("read token under %q: %v", path, err)
	}
	if d, ok := tok.(json.Delim); ok {
		switch d {
		case '{':
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					t.Fatalf("read key under %q: %v", path, err)
				}
				child := dpJoin(path, kt.(string))
				*out = append(*out, dpLeaf{path: child, isKey: true})
				dpReadLeaves(t, dec, child, out)
			}
		case '[':
			for dec.More() {
				dpReadLeaves(t, dec, dpJoin(path, "[]"), out)
			}
		}
		if _, err := dec.Token(); err != nil { // closing delimiter
			t.Fatalf("read closing delimiter under %q: %v", path, err)
		}
		return
	}
	// SetEscapeHTML(false): json.Marshal would write "<" as < and make a
	// legitimate "<ts>" mask compare unequal to itself.
	var enc bytes.Buffer
	e := json.NewEncoder(&enc)
	e.SetEscapeHTML(false)
	if err := e.Encode(tok); err != nil {
		t.Fatalf("encode scalar under %q: %v", path, err)
	}
	*out = append(*out, dpLeaf{path: path, val: strings.TrimSuffix(enc.String(), "\n")})
}

const (
	dpTestExec  = "exec-3f2b8c1e-5a47-4d0b-9e6a-1c2d3e4f5a6b"
	dpTestChild = "exec-aaaabbbb-cccc-4ddd-8eee-ffff00001111"
	dpTestLease = "lease-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
	dpTestToken = "token-11112222-3333-4444-8555-666677778888"
)

// dpCorpus holds one outbox body and one queue payload shaped like the real
// ones: an envelope with timestamps, ids inside string values, a batch payload
// carrying the parent's issue time, and ordinary payload data that must never
// be touched, including a field that merely shares a name with a volatile one.
// The spacing and the \u escape are deliberate: they are what a decode/encode
// round trip would change.
var dpCorpus = []string{
	`{"id":"expand/` + dpTestExec + `/fan/` + dpTestLease + `/0","task":{` +
		`"execution_id":"` + dpTestExec + `","node_name":"fan/_batch/0","node_idx":1,"type":4,` +
		`"payload":{"Triggered":0,"Name":"","Data":{"_batch_exec":true,` +
		`"parent_exec_id":"` + dpTestExec + `","parent_lease_id":"` + dpTestLease + `",` +
		`"parent_lease_token":"` + dpTestToken + `","child_exec_id":"` + dpTestChild + `/sub/fan/0",` +
		`"parent_issued_at":"2026-10-26T09:30:01.123456789Z","batch_index":0,"items":[1,2,3],` +
		`"label":"a<b","created_at_ms":42}}},"auto_depth":0,"activation_id":1,"unit_idx":1,` +
		`"available_at_ms":1790415001123,"created_at_ms":1790415001456}`,
	`{ "execution_id":"` + dpTestExec + `", "node_name":"fan/_batch/0", "node_idx":1, "type":4,` +
		` "payload":{"Triggered":0,"Name":"","Data":{"parent_lease_id":"` + dpTestLease + `",` +
		`"parent_issued_at":"2026-10-26T09:30:01.123456789Z","items":[1.50,2e3,"x"],"created_at_ms":42}},` +
		` "_namespace":"ns-3f2b8c1e-5a47-4d0b-9e6a-1c2d3e4f5a6b", "_auto_depth":0, "_activation_id":1,` +
		` "_unit_idx":1, "_deliverable_at_ms":1790415001456, "_intent_created_at_ms":1790415001456}`,
}

func dpMustNormalize(t *testing.T, doc string) []byte {
	t.Helper()
	out, err := dpNormalize([]byte(doc))
	if err != nil {
		t.Fatalf("dpNormalize(%.60s...): %v", doc, err)
	}
	return out
}

// TestNormalizerKeepsFieldSet pins the first half of the normalizer's contract:
// whatever it masks, the document keeps exactly the same keys and scalars, in
// the same order, at the same paths. A normalizer that dropped or reordered a
// field would hide precisely the drift the baseline exists to catch.
func TestNormalizerKeepsFieldSet(t *testing.T) {
	for i, doc := range dpCorpus {
		in := dpLeaves(t, []byte(doc))
		out := dpLeaves(t, dpMustNormalize(t, doc))
		if len(in) < 20 {
			t.Fatalf("doc %d: only %d leaves read from the corpus, the check would be vacuous", i, len(in))
		}
		if len(in) != len(out) {
			t.Fatalf("doc %d: %d leaves in, %d out (field set changed)", i, len(in), len(out))
		}
		for j := range in {
			if in[j].path != out[j].path || in[j].isKey != out[j].isKey {
				t.Fatalf("doc %d leaf %d: in %+v, out %+v (field set or order changed)", i, j, in[j], out[j])
			}
		}
	}
}

// dpInUUID and dpOutPlaceTag find the random id on the input side and the
// numbered placeholder on the output side. Replacing either with "#" leaves
// only the text around the id, which is what must survive the masking.
var (
	dpInUUID      = regexp.MustCompile(`(exec|lease|token|ns)-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}`)
	dpOutPlaceTag = regexp.MustCompile(`(exec|lease|token|ns)-[0-9]+`)
)

// TestNormalizerOnlyMasksVolatileValues pins the second half of the contract:
// the only values that change are the wall-clock stamps (replaced by a fixed
// mask) and the random ids inside strings (replaced by a numbered placeholder).
// Everything else, payload data above all, must come out byte for byte as it
// went in. A normalizer that blanked payload values would make every baseline
// comparison pass whatever the engine wrote.
func TestNormalizerOnlyMasksVolatileValues(t *testing.T) {
	masked := 0
	for i, doc := range dpCorpus {
		in := dpLeaves(t, []byte(doc))
		out := dpLeaves(t, dpMustNormalize(t, doc))
		for j := range in {
			if in[j].isKey {
				continue
			}
			switch {
			case dpVolatilePaths[in[j].path]:
				masked++
				if out[j].val != dpMask {
					t.Errorf("doc %d %s: volatile value is %s, want %s", i, in[j].path, out[j].val, dpMask)
				}
			case dpInUUID.MatchString(in[j].val):
				masked++
				if got, want := dpOutPlaceTag.ReplaceAllString(out[j].val, "#"), dpInUUID.ReplaceAllString(in[j].val, "#"); got != want {
					t.Errorf("doc %d %s: id-bearing value became %s, want the text around the id kept (%s)", i, in[j].path, out[j].val, want)
				}
			default:
				if out[j].val != in[j].val {
					t.Errorf("doc %d %s: value %s was rewritten to %s, only volatile values may change", i, in[j].path, in[j].val, out[j].val)
				}
			}
		}
	}
	if masked < 12 {
		t.Fatalf("only %d volatile or id-bearing values met, the corpus no longer exercises the masks", masked)
	}
}

// TestNormalizerNumbersIDsByFirstAppearance checks that one id gets one
// placeholder wherever it occurs, that different ids get different ones, and
// that the result does not depend on the ids' actual values.
func TestNormalizerNumbersIDsByFirstAppearance(t *testing.T) {
	a := `{"x":"` + dpTestExec + `","y":"` + dpTestLease + `","z":"p/` + dpTestExec + `/q"}`
	b := strings.NewReplacer(dpTestExec, dpTestChild, dpTestLease, dpTestToken).Replace(a)
	if strings.Contains(b, "exec-3f2b") || strings.Contains(b, "lease-0a1b") {
		t.Fatal("test setup: the second document still carries the first one's ids")
	}
	want := `{"x":"exec-1","y":"lease-1","z":"p/exec-1/q"}`
	if got := string(dpMustNormalize(t, a)); got != want {
		t.Fatalf("normalize(a) = %s, want %s", got, want)
	}
	// token-... has no counterpart in a, so only the shape is compared.
	if got := string(dpMustNormalize(t, b)); got != `{"x":"exec-1","y":"token-1","z":"p/exec-1/q"}` {
		t.Fatalf("normalize(b) = %s, want the same numbering for different ids", got)
	}
}

// TestNormalizerRejectsWhatItCannotMask covers the two ways a baseline could
// silently stop meaning anything: a volatile field that is not a scalar, and a
// random id the masks do not recognise.
func TestNormalizerRejectsWhatItCannotMask(t *testing.T) {
	for name, doc := range map[string]string{
		"object in a volatile field": `{"created_at_ms":{"v":1}}`,
		"array in a volatile field":  `{"available_at_ms":[1]}`,
		"unknown id prefix":          `{"x":"run-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"}`,
		"not json":                   `{"x":`,
	} {
		if out, err := dpNormalize([]byte(doc)); err == nil {
			t.Errorf("%s: dpNormalize accepted %s and returned %s", name, doc, out)
		}
	}
}
