package rstate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Node outputs are the largest thing this store keeps in Redis. One is committed
// per node, holding that node's whole output map — in a collection pipeline, a
// batch of captured HTTP request/response bodies, measured at 810 KB mean and
// 2.9 MB max, with several hundred executions in flight at once putting
// hundreds of megabytes into the keyspace.
//
// Those bytes are highly redundant (repeated header names, JSON keys, and
// near-identical records), so storing them compressed shrinks the keyspace
// several-fold for negligible CPU. The compressed form is an implementation
// detail of the store: every reader gets the plain map back, and nothing above
// this layer — in particular no node handler — can observe that the bytes on
// the wire were compressed.
//
// Compression is off unless ConfigureOutputCompression turns it on. Writes are
// the half that needs a switch: a process that predates this code cannot decode
// a frame, so a mixed-version fleet must not write compressed values until every
// process has been upgraded. Reads need no switch — see decodeOutputValue.
const (
	// outputCompressMinBytes skips values too small to benefit. A zstd frame
	// header costs bytes of its own, and the per-node entries that are a few
	// hundred bytes (an empty output, a single scalar) are not worth the call.
	outputCompressMinBytes = 1024

	// maxOutputCompressBytes bounds one decodeOutputValue decompression.
	//
	// The bytes came off a wire an attacker influences, so a small hostile frame
	// must not be able to expand into an allocation that takes the process down.
	// zstd's own default ceiling is 64 GiB, which is no bound at all. The largest
	// legitimate value is a single node's output — observed at 2.9 MB — so this
	// leaves more than an order of magnitude of headroom while turning a
	// decompression bomb into a returned error instead of an OOM.
	maxOutputCompressBytes = 128 << 20
)

// zstdFrameMagic is the opening four bytes of a zstd frame: the magic number
// 0xFD2FB528 in little-endian order.
//
// It is what lets decodeOutputValue serve compressed and uncompressed values
// with no flag and no configuration. A marshalled map[string]any is either an
// object ('{' = 0x7B) or the literal null ('n' = 0x6E), so no stored value this
// store has ever written can start with 0x28. A value written before compression
// existed and one written after are therefore both readable, which is what makes
// this deployable without draining the keyspace first.
var zstdFrameMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// outputCodec is the shared zstd encoder/decoder pair. Both are safe for
// concurrent use (Encoder.EncodeAll and Decoder.DecodeAll are documented as
// such) and both carry window buffers, so they are built once and reused rather
// than per call.
type outputCodec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

var (
	outputCodecOnce sync.Once
	sharedCodec     *outputCodec
	sharedCodecErr  error
)

func codecForOutput() (*outputCodec, error) {
	outputCodecOnce.Do(func() {
		// SpeedFastest rather than a higher level: on this data the ratio barely
		// moves across levels (6.90x at the fastest level against 7.20x at level
		// 3, measured on real batches) while the cost does — and this runs on
		// every node commit, on the path a backlog forms on.
		encoder, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithLowerEncoderMem(true),
		)
		if err != nil {
			sharedCodecErr = fmt.Errorf("build zstd encoder: %w", err)
			return
		}
		// WithDecoderMaxMemory caps what DecodeAll will allocate. It is the only
		// reason this decoder differs from the default; see
		// maxOutputCompressBytes.
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxOutputCompressBytes))
		if err != nil {
			sharedCodecErr = fmt.Errorf("build zstd decoder: %w", err)
			return
		}
		sharedCodec = &outputCodec{encoder: encoder, decoder: decoder}
	})
	return sharedCodec, sharedCodecErr
}

// encodeOutputValue renders a node output into the bytes to store.
//
// Returns the plain JSON whenever compression is disabled, the value is below
// outputCompressMinBytes, or the frame would not actually be smaller. That last
// case is not hypothetical: zstd output can exceed its input on incompressible
// data, and a node output is arbitrary captured traffic, so the only safe rule
// is to keep whichever form is shorter.
func (s *Store) encodeOutputValue(data map[string]any) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	if !s.outputCompression.Load() || len(raw) < outputCompressMinBytes {
		return string(raw), nil
	}
	codec, err := codecForOutput()
	if err != nil {
		// The encoder's options are static and valid, so this can only be an
		// allocation failure. Storing uncompressed is both safe and readable —
		// the read path sniffs the frame rather than trusting configuration —
		// and it keeps a codec problem from turning into a lost node output.
		return string(raw), nil
	}
	frame := codec.encoder.EncodeAll(raw, make([]byte, 0, len(raw)/4))
	if len(frame) >= len(raw) {
		return string(raw), nil
	}
	// Redis and the Lua scripts this is passed through are both binary-safe, so
	// a frame that is not valid UTF-8 is fine here.
	return string(frame), nil
}

// decodeOutputValue turns stored bytes back into a node output map.
//
// Deliberately NOT gated on s.outputCompression. Whether a value is compressed
// is a property of that value, not of this process's configuration, and a reader
// has to serve both — otherwise flipping the switch, or rolling a fleet back,
// would make every value written under the other setting unreadable. That
// asymmetry is what allows the deployment order "ship the code, then enable the
// switch" with no drain and no migration.
func (s *Store) decodeOutputValue(raw []byte) (map[string]any, error) {
	payload := raw
	if bytes.HasPrefix(raw, zstdFrameMagic) {
		codec, err := codecForOutput()
		if err != nil {
			return nil, fmt.Errorf("output is compressed but no decoder is available: %w", err)
		}
		decoded, err := codec.decoder.DecodeAll(raw, nil)
		if err != nil {
			return nil, fmt.Errorf("decompress output: %w", err)
		}
		payload = decoded
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}
