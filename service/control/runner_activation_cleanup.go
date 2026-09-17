package control

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// DeactivationObligationState is the durable lifecycle of a drain-triggered
// trigger cleanup. pending_fence is deliberately not deliverable: creating the
// intent before fencing the activation closes the crash window without asking a
// still-authoritative owner to stop early.
type DeactivationObligationState string

const (
	// DeactivationObligationPendingFence records the old owner before the
	// activation authority fence has completed. It is a recovery intent, never a
	// runner directive.
	DeactivationObligationPendingFence DeactivationObligationState = "pending_fence"
	// DeactivationObligationReady is durable, generation-fenced cleanup work
	// that must be re-delivered until its matching deactivated receipt arrives.
	DeactivationObligationReady DeactivationObligationState = "ready"
)

var (
	// ErrDeactivationObligationNotFound reports an impossible transition from a
	// fence intent to a deliverable cleanup obligation. Retaining the pending
	// record is safer than treating a missing record as cleanup complete.
	ErrDeactivationObligationNotFound = errors.New("deactivation obligation not found")
	// ErrInvalidDeactivationReceipt reports a direct directory caller attempting
	// to acknowledge something other than a deactivation. Core treats a stale or
	// unmatched receipt as a harmless no-op, but malformed internal calls should
	// remain visible to their caller.
	ErrInvalidDeactivationReceipt = errors.New("invalid deactivation receipt")
)

// DeactivationObligation identifies cleanup owed by one draining runner for
// one formerly-owned entry activation generation. The identity intentionally
// excludes SessionID and DrainGeneration: a re-register of the same runner
// explicitly rebinds delivery to its new fenced session, while an old session's
// receipt must never clear that new delivery attempt.
type DeactivationObligation struct {
	RunnerID        string
	SessionID       string
	Namespace       namespace.Namespace
	WorkflowID      types.WorkflowID
	WorkflowVersion string
	EntryUnitID     string
	ReplicaIndex    uint32
	Generation      uint64
	DrainGeneration uint64
	State           DeactivationObligationState
}

// Directive returns the runner-facing cleanup instruction. The drain generation
// is control-plane bookkeeping only and deliberately does not travel over the
// runner protocol: the receipt is fenced by the obligation's stored session,
// activation identity, and activation generation.
func (o DeactivationObligation) Directive() protocol.DeactivateDirective {
	return protocol.DeactivateDirective{
		Namespace:       string(o.Namespace),
		WorkflowID:      string(o.WorkflowID),
		WorkflowVersion: o.WorkflowVersion,
		EntryUnitID:     o.EntryUnitID,
		ReplicaIndex:    o.ReplicaIndex,
		Generation:      o.Generation,
	}
}

// deactivationObligationID is an opaque, delimiter-safe durable map key. It
// scopes an activation generation to the runner that owned it; two replicas and
// two workflow versions therefore cannot collide.
func deactivationObligationID(o DeactivationObligation) string {
	hash := sha256.New()
	for _, component := range []string{
		o.RunnerID,
		string(o.Namespace),
		string(o.WorkflowID),
		o.WorkflowVersion,
		o.EntryUnitID,
		strconv.FormatUint(uint64(o.ReplicaIndex), 10),
		strconv.FormatUint(o.Generation, 10),
	} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(component)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(component))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// deactivationInventoryKey is the namespace-free identity the pre-existing
// RegisterRunner activation inventory protocol can report. Namespace is
// deliberately absent because older runner wire DTOs did not carry it; the
// durable obligation itself still carries the full namespace identity.
type deactivationInventoryKey struct {
	WorkflowID      string
	WorkflowVersion string
	EntryUnitID     string
	ReplicaIndex    uint32
}

func deactivationInventoryKeyFromItem(item protocol.ActivationInventoryItem) deactivationInventoryKey {
	return deactivationInventoryKey{
		WorkflowID:      item.WorkflowID,
		WorkflowVersion: item.WorkflowVersion,
		EntryUnitID:     item.EntryUnitID,
		ReplicaIndex:    item.ReplicaIndex,
	}
}

func deactivationInventoryKeyFromObligation(obligation DeactivationObligation) deactivationInventoryKey {
	return deactivationInventoryKey{
		WorkflowID:      string(obligation.WorkflowID),
		WorkflowVersion: obligation.WorkflowVersion,
		EntryUnitID:     obligation.EntryUnitID,
		ReplicaIndex:    obligation.ReplicaIndex,
	}
}

func deactivationObligationFromAck(ns namespace.Namespace, ack protocol.ActivationAck) DeactivationObligation {
	return DeactivationObligation{
		RunnerID:        ack.RunnerID,
		Namespace:       ns,
		SessionID:       ack.SessionID,
		WorkflowID:      types.WorkflowID(ack.WorkflowID),
		WorkflowVersion: ack.WorkflowVersion,
		EntryUnitID:     ack.GroupID,
		ReplicaIndex:    ack.ReplicaIndex,
		Generation:      ack.Generation,
	}
}

func validateDeactivationObligation(o DeactivationObligation) error {
	if o.RunnerID == "" || o.Namespace == "" || o.WorkflowID == "" ||
		o.WorkflowVersion == "" || o.EntryUnitID == "" || o.Generation == 0 ||
		o.DrainGeneration == 0 {
		return fmt.Errorf("invalid deactivation obligation")
	}
	return nil
}

// DeactivationObligationDirectory is an optional RunnerDirectory capability
// used only for drain-triggered trigger cleanup. It lives in service/control:
// neither engine activation state nor generic backend contracts acquire remote
// operational semantics.
//
// EnsureDeactivationObligation creates a pending-fence intent only while the
// runner is still draining at the supplied control generation. applicable=false
// means a concurrent resume/superseding control transition won, so the caller
// must not fence a now-active owner on behalf of the stale drain.
type DeactivationObligationDirectory interface {
	EnsureDeactivationObligation(ctx context.Context, obligation DeactivationObligation) (applicable bool, err error)
	MarkDeactivationObligationReady(ctx context.Context, obligation DeactivationObligation) error
	CancelPendingDeactivationObligation(ctx context.Context, obligation DeactivationObligation) error
	PendingDeactivationObligations(ctx context.Context) ([]DeactivationObligation, error)
	// RebindDeactivationObligations moves delivery to a replacement session only
	// for activations that that session explicitly reported as still hosted at
	// the exact old generation. An unreported old subscription remains a blocker
	// instead of being guessed clean after a reconnect.
	RebindDeactivationObligations(ctx context.Context, runnerID, sessionID string, reported []protocol.ActivationInventoryItem) error
	DeactivationDirectives(ctx context.Context, runnerID, sessionID string) ([]protocol.DeactivateDirective, error)
	AcknowledgeDeactivation(ctx context.Context, ack protocol.ActivationAck) (acknowledged bool, err error)
}
