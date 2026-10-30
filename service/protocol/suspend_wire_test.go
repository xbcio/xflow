package protocol

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestTaskResultCarriesSuspendSpecStripFromOutput pins that the suspend request
// crosses the runner to server wire whole. The spec travels as JSON inside the
// result bytes, so a field the codec dropped would silently turn the runner's
// declaration into a no-op on the server that commits the suspension.
func TestTaskResultCarriesSuspendSpecStripFromOutput(t *testing.T) {
	spec := &types.SuspendSpec{
		Mode:            types.ModeMultiSignal,
		Signals:         []string{"gate/approval/alice", "gate/approval/bob"},
		Quorum:          1,
		Timeout:         time.Minute,
		StripFromOutput: []string{"approved", "decisions"},
	}
	data, err := MarshalTaskResult(engine.TaskResult{Suspend: spec})
	if err != nil {
		t.Fatalf("MarshalTaskResult: %v", err)
	}
	got, err := UnmarshalTaskResult(data)
	if err != nil {
		t.Fatalf("UnmarshalTaskResult: %v", err)
	}
	if !reflect.DeepEqual(got.Suspend, spec) {
		t.Fatalf("round-tripped spec = %#v, want %#v", got.Suspend, spec)
	}
}

// TestTaskResultOmitsAnEmptyStripFromOutput keeps the wire form of an ordinary
// suspension as it was: a peer that predates the field never sees the key.
func TestTaskResultOmitsAnEmptyStripFromOutput(t *testing.T) {
	data, err := MarshalTaskResult(engine.TaskResult{Suspend: &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"},
	}})
	if err != nil {
		t.Fatalf("MarshalTaskResult: %v", err)
	}
	if strings.Contains(string(data), "StripFromOutput") {
		t.Fatalf("encoded result = %s, want no StripFromOutput key", data)
	}
}

// TestTaskResultDecodesASpecFromAPeerWithoutStripFromOutput covers the other
// direction: a result from a runner that predates the field decodes with the
// list empty, which the engine reads as "store the input as it arrived".
func TestTaskResultDecodesASpecFromAPeerWithoutStripFromOutput(t *testing.T) {
	got, err := UnmarshalTaskResult([]byte(`{"suspend":{"Mode":0,"Signals":["go"],"Quorum":0,"Timer":0,"Timeout":0}}`))
	if err != nil {
		t.Fatalf("UnmarshalTaskResult: %v", err)
	}
	if got.Suspend == nil || len(got.Suspend.StripFromOutput) != 0 {
		t.Fatalf("decoded spec = %#v, want an empty StripFromOutput", got.Suspend)
	}
}
