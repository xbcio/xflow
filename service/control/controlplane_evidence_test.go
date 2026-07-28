package control

import (
	"testing"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
)

func TestControlPlaneWiresRuntimeEvidenceBuffer(t *testing.T) {
	buf := engine.NewRuntimeEvidenceBuffer(8)
	cp, err := NewControlPlane(Config{
		Backend:               backendlocal.New(),
		RuntimeEvidenceBuffer: buf,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.eng.EvidenceBuffer() != buf {
		t.Fatalf("control internal engine evidence buffer not wired")
	}
}
