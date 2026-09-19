package sqlstore

import (
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
)

// jsonBytes must satisfy driver.Valuer: that interface is the whole mechanism
// by which a JSON column receives text instead of an untyped []byte.
var _ driver.Valuer = jsonBytes(nil)

// TestJSONBytesBindJSONColumnsAsText pins the binding that MySQL 8 requires.
//
// Handing a bare []byte to a JSON column is what produced
//
//	Error 3144 (22032): Cannot create a JSON value from a string with
//	CHARACTER SET 'binary'.
//
// on every sink node commit in a deployed environment. The regression this
// guards is therefore about the Go type of the driver argument, not about the
// SQL: a []byte would still "look" right and still round-trip through
// miniredis-style fakes, because only a real MySQL server rejects it.
func TestJSONBytesBindJSONColumnsAsText(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   jsonBytes
		want string
	}{
		{name: "object", in: jsonBytes(`{"stored":3}`), want: `{"stored":3}`},
		{name: "array", in: jsonBytes(`[1,2]`), want: `[1,2]`},
		{name: "scalar", in: jsonBytes(`null`), want: `null`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.in.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if _, isBytes := got.([]byte); isBytes {
				t.Fatalf("Value() returned %T; a []byte is bound as binary and MySQL "+
					"rejects it for a JSON column", got)
			}
			if tt.in == nil {
				if got != nil {
					t.Fatalf("nil jsonBytes Value() = %#v, want nil", got)
				}
				return
			}
			if got != driver.Value(tt.want) {
				t.Fatalf("Value() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestJSONBytesNilStaysNull keeps the column nullable: a nil []byte used to
// bind NULL, and a silent switch to 'null' would store a JSON null literal
// where the schema and the readers expect SQL NULL.
func TestJSONBytesNilStaysNull(t *testing.T) {
	for _, in := range []jsonBytes{nil, {}, jsonBytes("")} {
		got, err := in.Value()
		if err != nil {
			t.Fatalf("Value() error = %v", err)
		}
		if got != nil {
			t.Fatalf("empty jsonBytes Value() = %#v, want nil", got)
		}
	}
}

func TestJSONBytesScanRoundTrips(t *testing.T) {
	for _, src := range []any{[]byte(`{"a":1}`), `{"a":1}`} {
		var b jsonBytes
		if err := b.Scan(src); err != nil {
			t.Fatalf("Scan(%T) error = %v", src, err)
		}
		if string(b) != `{"a":1}` {
			t.Fatalf("Scan(%T) = %q, want %q", src, b, `{"a":1}`)
		}
	}
	var b jsonBytes
	if err := b.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) error = %v", err)
	}
	if b != nil {
		t.Fatalf("Scan(nil) = %q, want nil", b)
	}
	if err := b.Scan(42); err == nil {
		t.Fatal("Scan(int) succeeded, want an error naming the unsupported type")
	}
}

// TestEveryJSONColumnUsesJSONBytes is the structural half of the guard: the
// behaviour test above only covers the type, so a new model field declared as
// []byte with `type:json` would reintroduce the bug without failing anything.
// Reading the struct tags is the only way to catch a column that no test
// happens to write.
func TestEveryJSONColumnUsesJSONBytes(t *testing.T) {
	models := []any{
		dbSupply{}, dbExecution{}, dbNode{}, dbSignal{},
		dbArtifactBlob{}, dbArtifact{}, dbRegistrationCode{},
		dbEnrollAudit{}, dbIssuedIdentity{},
	}
	want := reflect.TypeOf(jsonBytes(nil))
	jsonColumns := 0

	for _, model := range models {
		typ := reflect.TypeOf(model)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			gormTag := field.Tag.Get("gorm")
			if !strings.Contains(gormTag, "type:json") {
				continue
			}
			jsonColumns++
			if field.Type != want {
				t.Errorf("%s.%s is %s with a JSON column; it must be jsonBytes, "+
					"because a []byte is bound as binary and MySQL refuses to build "+
					"a JSON value from a binary-charset string",
					typ.Name(), field.Name, field.Type)
			}
		}
	}
	if jsonColumns == 0 {
		t.Fatal("found no JSON-tagged columns; the guard is not inspecting these models")
	}
}
