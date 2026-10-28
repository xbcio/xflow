//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
)

// dpMask replaces the value of a volatile field. It is a JSON string whatever
// the original type was, so a masked document stays valid JSON.
const dpMask = `"<ts>"`

// dpVolatilePaths lists, by dotted path from the document root, the fields whose
// value is a wall-clock stamp and so differs on every run of the same workflow.
// Only the value is replaced; the key stays where it was.
//
// The match is on the full path, not on the key name, so a payload that happens
// to carry a field called created_at_ms is left alone. Array elements add a
// "[]" segment, which no path below needs.
var dpVolatilePaths = map[string]bool{
	// Outbox envelope (the HSET field value under outbox:body).
	"created_at_ms":                      true,
	"available_at_ms":                    true,
	"task.payload.Data.parent_issued_at": true,
	// Queue payload (the asynq task body).
	"_deliverable_at_ms":            true,
	"_intent_created_at_ms":         true,
	"payload.Data.parent_issued_at": true,
}

// dpRandomID matches the identifiers a run mints from a random UUID: execution
// ids, lease ids, lease tokens and the per-run namespace ("ns-<uuid>"). They
// appear inside string values (outbox entry ids, parent_lease_id, child
// execution ids, _namespace), so they are masked wherever they occur in a value
// and never in a key.
var dpRandomID = regexp.MustCompile(`\b(exec|lease|token|ns)-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}`)

// dpAnyUUID finds a UUID-shaped string whatever prefix it carries. It is the
// safety net behind dpRandomID: a random id the mask does not know about shows
// up here instead of silently making a baseline that never matches again.
var dpAnyUUID = regexp.MustCompile(`[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}`)

// dpUnmaskedUUID returns the first UUID-shaped substring left in b, or "".
func dpUnmaskedUUID(b []byte) string {
	return string(dpAnyUUID.Find(b))
}

// dpIDMasker numbers the random ids it meets in order of first appearance, so
// two places that carried the same id still carry the same placeholder
// afterwards ("exec-1", "lease-1", ...). A fresh masker is made per document,
// which keeps the output independent of the order documents were captured in.
type dpIDMasker struct {
	ids  map[string]string
	seen map[string]int
}

func newDPIDMasker() *dpIDMasker {
	return &dpIDMasker{ids: map[string]string{}, seen: map[string]int{}}
}

func (m *dpIDMasker) mask(b []byte) []byte {
	return dpRandomID.ReplaceAllFunc(b, func(id []byte) []byte {
		if p, ok := m.ids[string(id)]; ok {
			return []byte(p)
		}
		kind := string(id[:bytes.IndexByte(id, '-')])
		m.seen[kind]++
		p := fmt.Sprintf("%s-%d", kind, m.seen[kind])
		m.ids[string(id)] = p
		return []byte(p)
	})
}

// dpEdit replaces raw[start:end] with repl.
type dpEdit struct {
	start, end int
	repl       []byte
}

// dpWalker walks one already-validated JSON document byte by byte and records
// the ranges to rewrite. The output is raw with those ranges swapped, so
// everything it does not touch (key order, spacing, number formatting, string
// escaping) survives exactly.
type dpWalker struct {
	raw   []byte
	pos   int
	ids   *dpIDMasker
	edits []dpEdit
}

// dpNormalize masks the volatile values of one outbox body or queue payload:
// the wall-clock stamps listed in dpVolatilePaths, and the random ids inside
// string values. Every id gets a placeholder numbered by first appearance within
// this document, so the result depends on the document alone.
//
// A volatile field that holds an object or array is an error, as is any UUID the
// masks did not recognise: an id that slips through would make a baseline that
// never matches again.
func dpNormalize(raw []byte) ([]byte, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("normalize: input is not one valid JSON value")
	}
	w := &dpWalker{raw: raw, ids: newDPIDMasker()}
	if err := w.value(""); err != nil {
		return nil, err
	}
	out := w.apply()
	if u := dpUnmaskedUUID(out); u != "" {
		return nil, fmt.Errorf("normalize: unmasked random id %q left in %s", u, out)
	}
	return out, nil
}

func (w *dpWalker) skipSpace() {
	for w.pos < len(w.raw) {
		switch w.raw[w.pos] {
		case ' ', '\t', '\n', '\r':
			w.pos++
		default:
			return
		}
	}
}

// stringEnd returns the offset just past the string that opens at w.pos.
func (w *dpWalker) stringEnd() int {
	i := w.pos + 1
	for w.raw[i] != '"' {
		if w.raw[i] == '\\' {
			i++
		}
		i++
	}
	return i + 1
}

// scalarEnd returns the offset just past the number or literal at w.pos.
func (w *dpWalker) scalarEnd() int {
	i := w.pos
	for i < len(w.raw) {
		switch w.raw[i] {
		case ',', '}', ']', ' ', '\t', '\n', '\r':
			return i
		}
		i++
	}
	return i
}

func dpJoin(path, elem string) string {
	if path == "" {
		return elem
	}
	return path + "." + elem
}

// value walks the value that starts at w.pos. path is the dotted path of the
// value from the document root, with "[]" standing for any array element.
func (w *dpWalker) value(path string) error {
	w.skipSpace()
	switch w.raw[w.pos] {
	case '{':
		return w.object(path)
	case '[':
		return w.array(path)
	case '"':
		end := w.stringEnd()
		w.replace(w.pos, end, w.ids.mask(w.raw[w.pos:end]))
		w.pos = end
	default:
		w.pos = w.scalarEnd()
	}
	return nil
}

func (w *dpWalker) object(path string) error {
	w.pos++ // '{'
	for {
		w.skipSpace()
		switch w.raw[w.pos] {
		case '}':
			w.pos++
			return nil
		case ',':
			w.pos++
			continue
		}
		keyEnd := w.stringEnd()
		var key string
		if err := json.Unmarshal(w.raw[w.pos:keyEnd], &key); err != nil {
			return fmt.Errorf("normalize: object key at offset %d: %w", w.pos, err)
		}
		w.pos = keyEnd
		w.skipSpace()
		w.pos++ // ':'
		child := dpJoin(path, key)
		if dpVolatilePaths[child] {
			if err := w.volatile(child); err != nil {
				return err
			}
			continue
		}
		if err := w.value(child); err != nil {
			return err
		}
	}
}

func (w *dpWalker) array(path string) error {
	w.pos++ // '['
	for {
		w.skipSpace()
		switch w.raw[w.pos] {
		case ']':
			w.pos++
			return nil
		case ',':
			w.pos++
			continue
		}
		if err := w.value(dpJoin(path, "[]")); err != nil {
			return err
		}
	}
}

// volatile masks the scalar that starts at w.pos. The key before it is already
// written and is never touched, which is what keeps the field set intact.
func (w *dpWalker) volatile(path string) error {
	w.skipSpace()
	var end int
	switch w.raw[w.pos] {
	case '{', '[':
		return fmt.Errorf("normalize: volatile field %q holds %q, want a scalar", path, w.raw[w.pos])
	case '"':
		end = w.stringEnd()
	default:
		end = w.scalarEnd()
	}
	w.replace(w.pos, end, []byte(dpMask))
	w.pos = end
	return nil
}

// replace records an edit, skipping the ones that would not change a byte.
func (w *dpWalker) replace(start, end int, repl []byte) {
	if bytes.Equal(w.raw[start:end], repl) {
		return
	}
	w.edits = append(w.edits, dpEdit{start: start, end: end, repl: repl})
}

// apply writes raw with the recorded edits swapped in. The walk is left to
// right, so the edits are already in offset order.
func (w *dpWalker) apply() []byte {
	var out bytes.Buffer
	last := 0
	for _, e := range w.edits {
		out.Write(w.raw[last:e.start])
		out.Write(e.repl)
		last = e.end
	}
	out.Write(w.raw[last:])
	return out.Bytes()
}
