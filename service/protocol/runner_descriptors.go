package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/xbcio/xflow/types"
)

// RunnerDescriptorSchema identifies the runner descriptor envelope format
// carried in RegisterRunnerRequest.DescriptorsJSON and HelloFrame.DescriptorsJSON.
// Bump it when the envelope or the encoded types.Descriptor shape changes
// incompatibly; control ignores an envelope whose schema it does not know.
const RunnerDescriptorSchema = "xflow.runner-descriptors/v1"

// Runner descriptor limits. Control enforces them on receipt; a runner's
// payload is never trusted to respect them. A violation drops descriptors but
// never fails a registration.
const (
	// MaxRunnerDescriptorEnvelopeBytes bounds the whole encoded envelope.
	// Exceeding it drops every descriptor.
	MaxRunnerDescriptorEnvelopeBytes = 1 << 20
	// MaxRunnerDescriptorBytes bounds one encoded descriptor. Exceeding it
	// drops that entry only.
	MaxRunnerDescriptorBytes = 64 << 10
	// MaxRunnerDescriptorEntries bounds the number of entries. Exceeding it
	// drops every descriptor.
	MaxRunnerDescriptorEntries = 256
)

// Sentinel errors returned by DecodeRunnerDescriptorEnvelope.
var (
	ErrRunnerDescriptorsTooLarge      = errors.New("runner descriptor envelope exceeds size limit")
	ErrRunnerDescriptorsTooMany       = errors.New("runner descriptor envelope exceeds entry limit")
	ErrRunnerDescriptorsUnknownSchema = errors.New("unknown runner descriptor envelope schema")
	ErrRunnerDescriptorsMalformed     = errors.New("malformed runner descriptor envelope")
	ErrRunnerDescriptorMalformed      = errors.New("malformed runner descriptor")
	ErrRunnerDescriptorTooLarge       = errors.New("runner descriptor exceeds size limit")
)

// RunnerDescriptor is one declared (type, version) and the types.Descriptor
// its handler reports, as a runner hands it to EncodeRunnerDescriptors.
type RunnerDescriptor struct {
	Type       string
	Version    int
	Descriptor types.Descriptor
}

// RunnerDescriptorEntry is one envelope entry on the wire. Descriptor stays
// raw so control can bound each entry's size before decoding it.
type RunnerDescriptorEntry struct {
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	Descriptor json.RawMessage `json:"descriptor"`
}

// RunnerDescriptorEnvelope is the versioned wrapper runners send. It is
// internal runner-to-server data: the inner descriptor is types.Descriptor
// encoded with encoding/json (Go field names as keys), never exposed to the
// browser, which only sees the projected NodeFormSchema.
type RunnerDescriptorEnvelope struct {
	Schema      string                  `json:"schema"`
	Descriptors []RunnerDescriptorEntry `json:"descriptors"`
}

// EncodeRunnerDescriptors encodes descriptors into a RunnerDescriptorSchema
// envelope, sorted by type then version so equal sets encode to equal bytes.
// It returns nil when there is nothing to report. It does not enforce the
// limits; control does.
func EncodeRunnerDescriptors(descriptors []RunnerDescriptor) (json.RawMessage, error) {
	if len(descriptors) == 0 {
		return nil, nil
	}
	sorted := append([]RunnerDescriptor(nil), descriptors...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Type != sorted[j].Type {
			return sorted[i].Type < sorted[j].Type
		}
		return sorted[i].Version < sorted[j].Version
	})
	env := RunnerDescriptorEnvelope{
		Schema:      RunnerDescriptorSchema,
		Descriptors: make([]RunnerDescriptorEntry, 0, len(sorted)),
	}
	for _, d := range sorted {
		raw, err := json.Marshal(d.Descriptor)
		if err != nil {
			return nil, fmt.Errorf("encode runner descriptor %q v%d: %w", d.Type, d.Version, err)
		}
		env.Descriptors = append(env.Descriptors, RunnerDescriptorEntry{
			Type:       d.Type,
			Version:    d.Version,
			Descriptor: raw,
		})
	}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("encode runner descriptor envelope: %w", err)
	}
	return out, nil
}

// DecodeRunnerDescriptorEnvelope decodes and bounds an envelope. Empty input
// or JSON null means the runner reports none and yields a zero envelope with a
// nil error. The envelope-wide limits are checked here; the per-entry limit is
// checked by DecodeRunnerDescriptor so a caller can drop one entry and keep
// the rest. The error wraps one of the ErrRunnerDescriptors* sentinels.
func DecodeRunnerDescriptorEnvelope(raw []byte) (RunnerDescriptorEnvelope, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return RunnerDescriptorEnvelope{}, nil
	}
	if len(raw) > MaxRunnerDescriptorEnvelopeBytes {
		return RunnerDescriptorEnvelope{}, fmt.Errorf("%w: %d bytes > %d",
			ErrRunnerDescriptorsTooLarge, len(raw), MaxRunnerDescriptorEnvelopeBytes)
	}
	var env RunnerDescriptorEnvelope
	if err := json.Unmarshal(trimmed, &env); err != nil {
		return RunnerDescriptorEnvelope{}, fmt.Errorf("%w: %v", ErrRunnerDescriptorsMalformed, err)
	}
	if env.Schema != RunnerDescriptorSchema {
		return RunnerDescriptorEnvelope{}, fmt.Errorf("%w: %q", ErrRunnerDescriptorsUnknownSchema, env.Schema)
	}
	if len(env.Descriptors) > MaxRunnerDescriptorEntries {
		return RunnerDescriptorEnvelope{}, fmt.Errorf("%w: %d entries > %d",
			ErrRunnerDescriptorsTooMany, len(env.Descriptors), MaxRunnerDescriptorEntries)
	}
	return env, nil
}

// DecodeRunnerDescriptor decodes one entry's raw descriptor. Numbers decode
// as json.Number so integers above 2^53 keep their precision. The error wraps
// ErrRunnerDescriptorTooLarge or ErrRunnerDescriptorMalformed.
func DecodeRunnerDescriptor(raw json.RawMessage) (types.Descriptor, error) {
	if len(raw) > MaxRunnerDescriptorBytes {
		return types.Descriptor{}, fmt.Errorf("%w: %d bytes > %d",
			ErrRunnerDescriptorTooLarge, len(raw), MaxRunnerDescriptorBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var d types.Descriptor
	if err := dec.Decode(&d); err != nil {
		return types.Descriptor{}, fmt.Errorf("%w: %v", ErrRunnerDescriptorMalformed, err)
	}
	if dec.More() {
		return types.Descriptor{}, fmt.Errorf("%w: trailing data", ErrRunnerDescriptorMalformed)
	}
	return d, nil
}
