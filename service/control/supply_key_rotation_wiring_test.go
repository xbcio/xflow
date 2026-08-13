package control

import (
	"context"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
)

// The rotation scheduler had no production caller at all: Rotate existed, was
// tested, and was never invoked outside tests, so the key lived as long as the
// deployment. This asserts Start actually launches it.
func TestStartLaunchesSupplyKeyRotation(t *testing.T) {
	cp := newTestControlPlaneForRotation(t, Config{
		Backend:                backendlocal.New(),
		EnableSupplyEncryption: true,
	})
	if cp.supplyKeyCancel == nil {
		t.Fatal("Start did not launch the supply key rotation loop; the transport " +
			"key would never rotate for the life of the deployment")
	}
}

// A negative period is the documented way to switch rotation off, and it must
// actually leave the loop unstarted rather than clamping to the default.
func TestNegativePeriodDisablesSupplyKeyRotation(t *testing.T) {
	cp := newTestControlPlaneForRotation(t, Config{
		Backend:                 backendlocal.New(),
		EnableSupplyEncryption:  true,
		SupplyKeyRotationPeriod: -time.Second,
	})
	if cp.supplyKeyCancel != nil {
		t.Fatal("a negative SupplyKeyRotationPeriod must disable rotation entirely")
	}
}

// With encryption off there is no key to rotate, so the loop must not run --
// otherwise it would dereference a nil encryptor on its first tick.
func TestRotationStaysOffWithoutSupplyEncryption(t *testing.T) {
	cp := newTestControlPlaneForRotation(t, Config{Backend: backendlocal.New()})
	if cp.supplyKeyCancel != nil {
		t.Fatal("rotation started without supply encryption enabled")
	}
}

// One tick of the loop must actually change the key when this replica owns the
// slot. Asserting on the loop's body rather than waiting for a tick keeps the
// test off the wall clock: the refresh cadence is deliberately much shorter
// than the rotation period, and neither is a value a test should wait out.
func TestRotationPassRotatesWhenTheSlotIsFree(t *testing.T) {
	cp := newTestControlPlaneForRotation(t, Config{
		Backend:                backendlocal.New(),
		EnableSupplyEncryption: true,
	})
	before := cp.supplyEncryptor.CurrentKeyID()

	cp.supplyKeyRotationPass(context.Background(), time.Hour)

	if cp.supplyEncryptor.CurrentKeyID() == before {
		t.Fatal("a rotation pass owning the slot left the key unchanged")
	}
}

func newTestControlPlaneForRotation(t *testing.T, cfg Config) *ControlPlane {
	t.Helper()
	cp, err := NewControlPlane(cfg)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cp.Shutdown(ctx)
	})
	return cp
}
