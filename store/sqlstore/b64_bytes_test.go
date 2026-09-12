package sqlstore

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"strings"
	"testing"
)

var (
	_ driver.Valuer = b64Bytes(nil)
	_ sql.Scanner   = (*b64Bytes)(nil)
)

func TestB64BytesValueAndScanRoundTripBinary(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe, 0x80, 0x7f, 'x', 0x00}

	value, err := b64Bytes(raw).Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	encoded, ok := value.(string)
	if !ok {
		t.Fatalf("Value type = %T, want string", value)
	}
	if want := base64.StdEncoding.EncodeToString(raw); encoded != want {
		t.Fatalf("Value = %q, want %q", encoded, want)
	}

	var got b64Bytes
	if err := got.Scan(encoded); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !bytes.Equal([]byte(got), raw) {
		t.Fatalf("round trip = %x, want %x", []byte(got), raw)
	}
}

func TestB64BytesScanAcceptsStringAndByteSliceSources(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe, 'x'}
	encoded := base64.StdEncoding.EncodeToString(raw)

	for _, tt := range []struct {
		name string
		src  any
	}{
		{name: "string", src: encoded},
		{name: "byte slice", src: []byte(encoded)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got b64Bytes
			if err := got.Scan(tt.src); err != nil {
				t.Fatalf("Scan(%T): %v", tt.src, err)
			}
			if !bytes.Equal([]byte(got), raw) {
				t.Fatalf("Scan(%T) = %x, want %x", tt.src, []byte(got), raw)
			}
		})
	}
}

func TestB64BytesNilAndEmptyHandling(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value b64Bytes
	}{
		{name: "nil", value: nil},
		{name: "non-nil empty", value: b64Bytes{}},
	} {
		t.Run("Value/"+tt.name, func(t *testing.T) {
			got, err := tt.value.Value()
			if err != nil {
				t.Fatalf("Value: %v", err)
			}
			if got != "" {
				t.Fatalf("Value = %#v, want empty string", got)
			}
		})
	}

	for _, tt := range []struct {
		name string
		src  any
	}{
		{name: "nil", src: nil},
		{name: "empty string", src: ""},
		{name: "empty byte slice", src: []byte{}},
	} {
		t.Run("Scan/"+tt.name, func(t *testing.T) {
			got := b64Bytes{0x01}
			if err := got.Scan(tt.src); err != nil {
				t.Fatalf("Scan(%T): %v", tt.src, err)
			}
			if got != nil {
				t.Fatalf("Scan(%T) = %x, want nil", tt.src, []byte(got))
			}
		})
	}
}

func TestB64BytesScanRejectsMalformedBase64WithoutExposingPayload(t *testing.T) {
	const secret = "supply-payload-must-not-appear-in-errors"
	got := b64Bytes{0x01, 0x02}

	err := got.Scan("!" + secret)
	if err == nil {
		t.Fatal("Scan malformed base64 error = nil, want error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("Scan error exposed payload")
	}
	if want := []byte{0x01, 0x02}; !bytes.Equal([]byte(got), want) {
		t.Fatalf("Scan changed receiver after error: got %x, want %x", []byte(got), want)
	}
}

type b64BytesUnsupportedSource struct {
	payload string
}

func TestB64BytesScanRejectsUnsupportedSourceWithoutExposingPayload(t *testing.T) {
	const secret = "supply-payload-must-not-appear-in-errors"
	got := b64Bytes{0x01, 0x02}

	err := got.Scan(b64BytesUnsupportedSource{payload: secret})
	if err == nil {
		t.Fatal("Scan unsupported source error = nil, want error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("Scan error exposed payload")
	}
	if want := []byte{0x01, 0x02}; !bytes.Equal([]byte(got), want) {
		t.Fatalf("Scan changed receiver after error: got %x, want %x", []byte(got), want)
	}
}
