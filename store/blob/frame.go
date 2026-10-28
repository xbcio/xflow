package blob

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Frame header bytes. Every object ObjectBacked writes is one header byte
// followed by the payload, so the encoding lives in the stored bytes themselves
// and a reader never has to guess it from a Ref or from object metadata.
const (
	frameRaw  byte = 0x00 // payload is the original bytes
	frameZstd byte = 0x01 // payload is one zstd frame of the original bytes
)

// frameSlack is added to the read bound for an object. zstd can expand
// incompressible input by a few bytes per block plus a frame header, and the
// bound only has to stop a runaway object, not predict the exact size.
const frameSlack = 1 << 10

// sharedEncoder and sharedDecoder are built once and shared by every
// ObjectBacked. Encoder.EncodeAll and Decoder.DecodeAll are safe for concurrent
// use, and both hold sizeable buffers, so building one per call would be
// wasteful. Neither needs Close: with a nil stream they start no goroutines.
var (
	sharedEncoder = sync.OnceValues(func() (*zstd.Encoder, error) {
		// The fastest level: ratio barely moves with level on this kind of data
		// while the cost does, and Put sits on the node-commit path.
		return zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithLowerEncoderMem(true),
		)
	})

	// WithDecodeAllCapLimit makes DecodeAll refuse to write past cap(dst). Each
	// call hands it a dst whose capacity is the size the Ref promises, which is
	// what keeps a small hostile frame from expanding into a huge allocation.
	sharedDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
		return zstd.NewReader(nil, zstd.WithDecodeAllCapLimit(true))
	})
)

// frameReadLimit returns the most bytes an object holding a blob of the given
// uncompressed size may have: the header byte, the payload, and slack for zstd
// overhead. A longer object is not something this package wrote.
func frameReadLimit(size int64) int64 {
	return 1 + size + size/64 + frameSlack
}

// encodeZstd returns the complete frame (header byte plus zstd payload) for
// data, or ok=false when zstd is unavailable or does not make data smaller. The
// caller then stores the data raw. data is never modified.
func encodeZstd(data []byte) (frame []byte, ok bool) {
	enc, err := sharedEncoder()
	if err != nil {
		// The options are static, so this is an allocation failure. Storing raw
		// is always safe and readable, so it is not worth failing the Put over.
		return nil, false
	}
	dst := make([]byte, 1, 1+len(data)/2)
	dst[0] = frameZstd
	frame = enc.EncodeAll(data, dst)
	if len(frame)-1 >= len(data) {
		return nil, false
	}
	return frame, true
}

// decodeFrame returns the original bytes held in frame, which must decode to
// exactly size bytes. It trusts only the header byte. Anything it cannot decode,
// or that decodes to the wrong length, is reported as ErrCorrupt. The returned
// slice may alias frame.
func decodeFrame(frame []byte, size int64) ([]byte, error) {
	if len(frame) == 0 {
		return nil, fmt.Errorf("%w: empty object, no frame header", ErrCorrupt)
	}
	payload := frame[1:]
	switch frame[0] {
	case frameRaw:
		if int64(len(payload)) != size {
			return nil, fmt.Errorf("%w: raw payload is %d bytes, ref says %d", ErrCorrupt, len(payload), size)
		}
		return payload, nil
	case frameZstd:
		return decodeZstd(payload, size)
	default:
		return nil, fmt.Errorf("%w: unknown frame header 0x%02x", ErrCorrupt, frame[0])
	}
}

// decodeZstd decodes one zstd payload into exactly size bytes.
func decodeZstd(payload []byte, size int64) ([]byte, error) {
	// A frame that declares its own content size must declare the Ref's size.
	// This rejects a decompression bomb before any output buffer exists.
	var h zstd.Header
	if err := h.Decode(payload); err != nil {
		return nil, fmt.Errorf("%w: zstd header: %w", ErrCorrupt, err)
	}
	if h.HasFCS && h.FrameContentSize != uint64(size) {
		return nil, fmt.Errorf("%w: zstd frame declares %d bytes, ref says %d", ErrCorrupt, h.FrameContentSize, size)
	}
	dec, err := sharedDecoder()
	if err != nil {
		return nil, fmt.Errorf("blob: build zstd decoder: %w", err)
	}
	out, err := dec.DecodeAll(payload, make([]byte, 0, size))
	if err != nil {
		return nil, fmt.Errorf("%w: zstd decode: %w", ErrCorrupt, err)
	}
	if int64(len(out)) != size {
		return nil, fmt.Errorf("%w: zstd payload decodes to %d bytes, ref says %d", ErrCorrupt, len(out), size)
	}
	return out, nil
}
