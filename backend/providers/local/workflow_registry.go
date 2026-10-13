package local

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

type workflowMutationKey struct {
	namespace  string
	mutationID string
}

type workflowMutation struct {
	fingerprint [sha256.Size]byte
	result      backend.WorkflowReplaceResult
}

type workflowActivationProjection struct {
	sequence      uint64
	intent        backend.WorkflowActivationProjectionIntent
	applied       bool
	leaseToken    string
	leaseDeadline time.Time
}

// workflowVersionEntry is one retired version of a workflow, kept in
// retirement order. record is the payload the version carried while it was
// live, which is what GetWorkflowVersion returns for an archived version.
type workflowVersionEntry struct {
	version  string
	hash     string
	revision uint64
	record   backend.WorkflowRecord
}

type workflowRegistry struct {
	mu                 sync.Mutex
	byID               map[types.WorkflowID]backend.WorkflowRecord
	byKey              map[string]types.WorkflowID
	versions           map[types.WorkflowID][]workflowVersionEntry
	retention          int
	mutations          map[workflowMutationKey]workflowMutation
	projections        map[workflowMutationKey]*workflowActivationProjection
	projectionNS       map[namespace.Namespace]struct{}
	revision           uint64
	projectionSequence uint64
	clock              func() time.Time
}

var _ backend.DurableWorkflowReplaceCapability = (*workflowRegistry)(nil)
var _ backend.WorkflowVersionCapability = (*workflowRegistry)(nil)

func newWorkflowRegistry() *workflowRegistry {
	return newWorkflowRegistryWithRetention(backend.DefaultWorkflowVersionRetention)
}

// newWorkflowRegistryWithRetention builds an in-memory registry that keeps at
// most retention retired versions per workflow. A value <= 0 keeps every
// retired version, matching WithWorkflowVersionRetention.
func newWorkflowRegistryWithRetention(retention int) *workflowRegistry {
	return &workflowRegistry{
		byID:        make(map[types.WorkflowID]backend.WorkflowRecord),
		byKey:       make(map[string]types.WorkflowID),
		versions:    make(map[types.WorkflowID][]workflowVersionEntry),
		retention:   retention,
		mutations:   make(map[workflowMutationKey]workflowMutation),
		projections: make(map[workflowMutationKey]*workflowActivationProjection),
		projectionNS: map[namespace.Namespace]struct{}{
			namespace.Default: {},
		},
		clock: time.Now,
	}
}

func (r *workflowRegistry) AddWorkflow(ctx context.Context, rec backend.WorkflowRecord) (backend.WorkflowRecord, error) {
	if err := ctx.Err(); err != nil {
		return backend.WorkflowRecord{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return backend.WorkflowRecord{}, err
	}
	if id, ok := r.byKey[rec.Key]; ok {
		existing := r.byID[id]
		if existing.DefinitionHash != rec.DefinitionHash {
			// The key is live under a different definition: the version label
			// is already published, and a label never resolves to two
			// definitions. Re-publishing it with the stored definition is not
			// a conflict.
			return backend.WorkflowRecord{}, &backend.WorkflowVersionImmutableError{Version: rec.Version}
		}
		return existing, nil
	}
	if rec.ID == "" {
		rec.ID = r.newWorkflowIDLocked()
	} else if _, ok := r.byID[rec.ID]; ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowConflict
	}
	if err := ctx.Err(); err != nil {
		return backend.WorkflowRecord{}, err
	}
	rec.RegistryRevision = r.nextRevisionLocked()
	r.byKey[rec.Key] = rec.ID
	r.byID[rec.ID] = rec
	return rec, nil
}

func (r *workflowRegistry) GetWorkflow(_ context.Context, id types.WorkflowID) (backend.WorkflowRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.byID[id]
	if !ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return rec, nil
}

func (r *workflowRegistry) GetWorkflowByKey(_ context.Context, key string) (backend.WorkflowRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id, ok := r.byKey[key]
	if !ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return r.byID[id], nil
}

// ListWorkflowVersions returns the current version first, then every retired
// version newest retirement first.
func (r *workflowRegistry) ListWorkflowVersions(ctx context.Context, id types.WorkflowID) ([]backend.WorkflowVersionInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	current, ok := r.byID[id]
	if !ok {
		return nil, backend.ErrWorkflowNotFound
	}
	rows := make([]backend.WorkflowVersionInfo, 0, len(r.versions[id])+1)
	rows = append(rows, backend.WorkflowVersionInfo{
		Version:          current.Version,
		DefinitionHash:   current.DefinitionHash,
		RegistryRevision: current.RegistryRevision,
		Current:          true,
	})
	retired := make([]backend.WorkflowVersionInfo, 0, len(r.versions[id]))
	for _, entry := range r.versions[id] {
		if entry.version == current.Version {
			continue
		}
		retired = append(retired, backend.WorkflowVersionInfo{
			Version:          entry.version,
			DefinitionHash:   entry.hash,
			RegistryRevision: entry.revision,
			Archived:         true,
		})
	}
	sort.Slice(retired, func(i, j int) bool {
		if retired[i].RegistryRevision != retired[j].RegistryRevision {
			return retired[i].RegistryRevision > retired[j].RegistryRevision
		}
		return retired[i].Version < retired[j].Version
	})
	return append(rows, retired...), nil
}

// GetWorkflowVersion returns the addressed version: the current one (an empty
// version means "current") from the live record, anything else from the
// retired-version history verbatim.
func (r *workflowRegistry) GetWorkflowVersion(ctx context.Context, id types.WorkflowID, version string) (backend.WorkflowRecord, error) {
	if err := ctx.Err(); err != nil {
		return backend.WorkflowRecord{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return backend.WorkflowRecord{}, err
	}
	current, ok := r.byID[id]
	if !ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	if version == "" || version == current.Version {
		return current, nil
	}
	entry, ok := r.retiredVersionLocked(id, version)
	if !ok {
		return backend.WorkflowRecord{}, backend.ErrWorkflowVersionNotFound
	}
	return entry.record, nil
}

// UpdateDefinitionHash atomically updates the DefinitionHash of the record
// holding id, but only if the currently-stored hash still matches
// expectedOldHash. The mutex makes the check-and-set atomic with respect to
// other in-process registrars.
func (r *workflowRegistry) UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	rec, ok := r.byID[id]
	if !ok {
		return backend.ErrWorkflowNotFound
	}
	if rec.DefinitionHash != expectedOldHash {
		return backend.ErrWorkflowConflict
	}
	if rec.DefinitionHash == newHash {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rec.DefinitionHash = newHash
	rec.RegistryRevision = r.nextRevisionLocked()
	r.byID[id] = rec
	return nil
}

func (r *workflowRegistry) RemoveWorkflow(ctx context.Context, id types.WorkflowID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	rec, ok := r.byID[id]
	if !ok {
		return backend.ErrWorkflowNotFound
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(r.byID, id)
	delete(r.byKey, rec.Key)
	// Version history is reachable only through the id, and ids are never
	// reused, so a removed workflow takes its history with it. Keeping it would
	// leak payloads nobody can address.
	delete(r.versions, id)
	return nil
}

// ListWorkflows returns one page of the ids registered in ns, newest registry
// revision first and then by id ascending, matching the distributed registry's
// enumeration contract.
//
// The in-memory registry is single-process and namespace-agnostic everywhere
// else (GetWorkflow and GetWorkflowByKey ignore the context namespace), so
// enumeration is the one place where a scope is supplied explicitly and must be
// honoured: ns is matched exactly and is validated first, so an empty or
// malformed namespace is rejected rather than interpreted as "every namespace",
// and an unknown namespace yields an empty result rather than an error.
//
// A record whose Namespace field is empty resolves to namespace.Default, the
// same mapping workflowProjectionNamespace already applies to projection
// namespaces for records written before the field was populated.
func (r *workflowRegistry) ListWorkflows(ctx context.Context, ns namespace.Namespace, opts backend.WorkflowListOptions) ([]types.WorkflowID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := namespace.Validate(ns); err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	if opts.Limit < 0 || opts.Offset < 0 {
		return nil, fmt.Errorf("list workflows: limit and offset must not be negative (limit=%d offset=%d)", opts.Limit, opts.Offset)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records := make([]backend.WorkflowRecord, 0, len(r.byID))
	for _, rec := range r.byID {
		if workflowProjectionNamespace(rec.Namespace) != ns {
			continue
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].RegistryRevision != records[j].RegistryRevision {
			return records[i].RegistryRevision > records[j].RegistryRevision
		}
		return records[i].ID < records[j].ID
	})

	start := opts.Offset
	if start > len(records) {
		start = len(records)
	}
	records = records[start:]
	if opts.Limit > 0 && opts.Limit < len(records) {
		records = records[:opts.Limit]
	}
	ids := make([]types.WorkflowID, 0, len(records))
	for _, rec := range records {
		ids = append(ids, rec.ID)
	}
	return ids, nil
}

// CompareAndReplaceWorkflow atomically validates an expected revision and
// replaces both registry indexes. A committed MutationID is retained so a
// retry can recover the exact result without reapplying the mutation.
func (r *workflowRegistry) CompareAndReplaceWorkflow(ctx context.Context, req backend.WorkflowReplaceRequest) (backend.WorkflowReplaceResult, error) {
	if err := ctx.Err(); err != nil {
		return backend.WorkflowReplaceResult{}, err
	}
	if req.MutationID == "" {
		return backend.WorkflowReplaceResult{}, fmt.Errorf("replace workflow: mutation id is required")
	}

	replacement := req.Replacement
	// RegistryRevision is storage-owned and therefore neither accepted from the
	// caller nor included in the idempotency fingerprint.
	replacement.RegistryRevision = 0
	fingerprint, err := workflowReplaceFingerprint(req.Expected, replacement)
	if err != nil {
		return backend.WorkflowReplaceResult{}, err
	}
	mutationKey := workflowMutationKey{
		namespace:  string(workflowProjectionNamespace(replacement.Namespace)),
		mutationID: req.MutationID,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return backend.WorkflowReplaceResult{}, err
	}
	if prior, ok := r.mutations[mutationKey]; ok {
		if prior.fingerprint != fingerprint {
			return backend.WorkflowReplaceResult{}, workflowReplaceConflict(backend.WorkflowReplaceConflictMutationIDReuse)
		}
		return prior.result, nil
	}

	current, ok := r.byID[req.Expected.ID]
	if !ok || !workflowRevisionMatches(current, req.Expected) || r.byKey[current.Key] != current.ID {
		return backend.WorkflowReplaceResult{}, workflowReplaceConflict(backend.WorkflowReplaceConflictStaleRevision)
	}
	previous := backend.RevisionOfWorkflow(current)

	if owner, occupied := r.byKey[replacement.Key]; occupied && owner != current.ID {
		return backend.WorkflowReplaceResult{}, workflowReplaceConflict(backend.WorkflowReplaceConflictDestinationKey)
	}
	if replacement.ID != "" && replacement.ID != current.ID {
		if _, occupied := r.byID[replacement.ID]; occupied {
			return backend.WorkflowReplaceResult{}, workflowReplaceConflict(backend.WorkflowReplaceConflictDestinationID)
		}
	}
	if replacement.ID == "" {
		replacement.ID = r.newWorkflowIDLocked()
	}

	replacement.RegistryRevision = current.RegistryRevision
	if reflect.DeepEqual(current, replacement) {
		result := backend.WorkflowReplaceResult{
			Status:   backend.WorkflowReplaceUnchanged,
			Previous: previous,
			Current:  current,
		}
		if err := ctx.Err(); err != nil {
			return backend.WorkflowReplaceResult{}, err
		}
		r.mutations[mutationKey] = workflowMutation{fingerprint: fingerprint, result: result}
		r.addWorkflowActivationProjectionLocked(mutationKey, result)
		return result, nil
	}

	// A retired version label is immutable: its payload is what a pinned replay
	// or a subworkflow reference resolves to. Re-installing the label with the
	// payload it was retired with (a rollback) is allowed; a replace that keeps
	// the current label retires nothing and stays updatable in place.
	if current.Version != replacement.Version {
		if entry, ok := r.retiredVersionLocked(current.ID, replacement.Version); ok && entry.hash != replacement.DefinitionHash {
			return backend.WorkflowReplaceResult{}, &backend.WorkflowVersionImmutableError{Version: replacement.Version}
		}
	}

	if err := ctx.Err(); err != nil {
		return backend.WorkflowReplaceResult{}, err
	}
	replacement.RegistryRevision = r.nextRevisionLocked()
	delete(r.byID, current.ID)
	delete(r.byKey, current.Key)
	r.byID[replacement.ID] = replacement
	r.byKey[replacement.Key] = replacement.ID
	r.retireVersionLocked(current, replacement)

	result := backend.WorkflowReplaceResult{
		Status:   backend.WorkflowReplaceReplaced,
		Previous: previous,
		Current:  replacement,
	}
	r.mutations[mutationKey] = workflowMutation{fingerprint: fingerprint, result: result}
	r.addWorkflowActivationProjectionLocked(mutationKey, result)
	return result, nil
}

// retireVersionLocked applies one committed replacement to the workflow's
// version history, mirroring the registry script: history follows the surviving
// record id, a replace that changes the version retires the previous payload
// under its own label, the installed label is promoted out of history, and
// retention evicts the oldest retirements beyond the limit.
func (r *workflowRegistry) retireVersionLocked(current, replacement backend.WorkflowRecord) {
	if current.Version != replacement.Version {
		if current.ID != replacement.ID {
			if entries, ok := r.versions[current.ID]; ok {
				r.versions[replacement.ID] = entries
				delete(r.versions, current.ID)
			}
		}
		entries := r.versions[replacement.ID]
		entries = append(entries, workflowVersionEntry{
			version:  current.Version,
			hash:     current.DefinitionHash,
			revision: current.RegistryRevision,
			record:   current,
		})
		r.versions[replacement.ID] = entries
	}
	// The installed version is live again, so it is no longer retired.
	r.dropVersionLocked(replacement.ID, replacement.Version)
	if r.retention > 0 {
		if entries := r.versions[replacement.ID]; len(entries) > r.retention {
			r.versions[replacement.ID] = entries[len(entries)-r.retention:]
		}
	}
}

// retiredVersionLocked returns the history entry holding version, if any.
func (r *workflowRegistry) retiredVersionLocked(id types.WorkflowID, version string) (workflowVersionEntry, bool) {
	for _, entry := range r.versions[id] {
		if entry.version == version {
			return entry, true
		}
	}
	return workflowVersionEntry{}, false
}

func (r *workflowRegistry) dropVersionLocked(id types.WorkflowID, version string) {
	entries := r.versions[id]
	for i, entry := range entries {
		if entry.version == version {
			r.versions[id] = append(entries[:i:i], entries[i+1:]...)
			return
		}
	}
}

// ListWorkflowActivationProjectionNamespaces returns every namespace that has committed a workflow
// activation projection intent. Membership is append-only and includes the
// default namespace even before its first replacement.
func (r *workflowRegistry) ListWorkflowActivationProjectionNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespaces := make([]namespace.Namespace, 0, len(r.projectionNS))
	for ns := range r.projectionNS {
		namespaces = append(namespaces, ns)
	}
	sort.Slice(namespaces, func(i, j int) bool { return namespaces[i] < namespaces[j] })
	return namespaces, nil
}

// ListPending returns a stable sequence page of unacknowledged intents. The
// sequence is private to this in-memory registry and serves only as an opaque
// cursor for one namespace scan.
func (r *workflowRegistry) ListPendingWorkflowActivationProjections(ctx context.Context, ns namespace.Namespace, cursor uint64, limit int) ([]backend.WorkflowActivationProjectionRef, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		return nil, 0, fmt.Errorf("list pending workflow activation projections: limit must be positive")
	}
	ns = workflowProjectionNamespace(string(ns))

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	type pendingProjection struct {
		sequence   uint64
		mutationID string
	}
	pending := make([]pendingProjection, 0)
	for key, projection := range r.projections {
		if key.namespace != string(ns) || projection.applied || projection.sequence <= cursor {
			continue
		}
		pending = append(pending, pendingProjection{
			sequence:   projection.sequence,
			mutationID: key.mutationID,
		})
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].sequence < pending[j].sequence })

	more := len(pending) > limit
	if more {
		pending = pending[:limit]
	}
	refs := make([]backend.WorkflowActivationProjectionRef, len(pending))
	for i, projection := range pending {
		refs[i] = backend.WorkflowActivationProjectionRef{MutationID: projection.mutationID}
	}
	if !more || len(pending) == 0 {
		return refs, 0, nil
	}
	return refs, pending[len(pending)-1].sequence, nil
}

// ClaimWorkflowActivationProjection acquires an intent with a caller-supplied fencing token. An unexpired
// same-token retry returns the original deadline; a different token observes
// Busy until the lease expires.
func (r *workflowRegistry) ClaimWorkflowActivationProjection(ctx context.Context, ns namespace.Namespace, mutationID, leaseToken string, leaseTTL time.Duration) (backend.WorkflowActivationProjectionClaim, error) {
	if err := ctx.Err(); err != nil {
		return backend.WorkflowActivationProjectionClaim{}, err
	}
	if mutationID == "" {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection: mutation id is required")
	}
	if leaseToken == "" {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q: lease token is required", mutationID)
	}
	if leaseTTL <= 0 {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q: lease TTL must be positive", mutationID)
	}
	key := workflowMutationKey{
		namespace:  string(workflowProjectionNamespace(string(ns))),
		mutationID: mutationID,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return backend.WorkflowActivationProjectionClaim{}, err
	}
	projection, ok := r.projections[key]
	if !ok {
		return backend.WorkflowActivationProjectionClaim{}, fmt.Errorf("claim workflow activation projection %q: intent not found", mutationID)
	}
	if projection.applied {
		return backend.WorkflowActivationProjectionClaim{
			State:  backend.WorkflowActivationProjectionClaimApplied,
			Intent: projection.intent,
		}, nil
	}

	now := r.nowLocked()
	if projection.leaseToken != "" && now.Before(projection.leaseDeadline) {
		if projection.leaseToken == leaseToken {
			return backend.WorkflowActivationProjectionClaim{
				State:         backend.WorkflowActivationProjectionClaimAcquired,
				Intent:        projection.intent,
				LeaseDeadline: projection.leaseDeadline,
			}, nil
		}
		return backend.WorkflowActivationProjectionClaim{
			State:         backend.WorkflowActivationProjectionClaimBusy,
			LeaseDeadline: projection.leaseDeadline,
		}, nil
	}

	projection.leaseToken = leaseToken
	projection.leaseDeadline = now.Add(leaseTTL)
	return backend.WorkflowActivationProjectionClaim{
		State:         backend.WorkflowActivationProjectionClaimAcquired,
		Intent:        projection.intent,
		LeaseDeadline: projection.leaseDeadline,
	}, nil
}

// AckWorkflowActivationProjection applies a token fence before removing an intent from pending. Applied
// state is retained with the mutation ledger so response-loss retries can
// distinguish completion from a missing or busy intent.
func (r *workflowRegistry) AckWorkflowActivationProjection(ctx context.Context, ns namespace.Namespace, mutationID, leaseToken string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if mutationID == "" {
		return false, fmt.Errorf("ack workflow activation projection: mutation id is required")
	}
	if leaseToken == "" {
		return false, fmt.Errorf("ack workflow activation projection %q: lease token is required", mutationID)
	}
	key := workflowMutationKey{
		namespace:  string(workflowProjectionNamespace(string(ns))),
		mutationID: mutationID,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}
	projection, ok := r.projections[key]
	if !ok {
		return false, fmt.Errorf("ack workflow activation projection %q: intent not found", mutationID)
	}
	if projection.applied {
		return true, nil
	}
	if projection.leaseToken != leaseToken || !r.nowLocked().Before(projection.leaseDeadline) {
		return false, nil
	}
	projection.applied = true
	projection.leaseToken = ""
	projection.leaseDeadline = time.Time{}
	return true, nil
}

func (r *workflowRegistry) addWorkflowActivationProjectionLocked(key workflowMutationKey, result backend.WorkflowReplaceResult) {
	if _, exists := r.projections[key]; exists {
		return
	}
	r.projectionSequence++
	if r.projectionSequence == 0 {
		panic("local workflow activation projection sequence exhausted")
	}
	ns := workflowProjectionNamespace(key.namespace)
	r.projectionNS[ns] = struct{}{}
	r.projections[key] = &workflowActivationProjection{
		sequence: r.projectionSequence,
		intent: backend.WorkflowActivationProjectionIntent{
			Namespace:  ns,
			MutationID: key.mutationID,
			Previous:   result.Previous,
			Current:    result.Current,
		},
	}
}

func (r *workflowRegistry) nowLocked() time.Time {
	if r.clock == nil {
		return time.Now().UTC()
	}
	return r.clock().UTC()
}

func workflowProjectionNamespace(raw string) namespace.Namespace {
	if raw == "" {
		return namespace.Default
	}
	return namespace.Namespace(raw)
}

func (r *workflowRegistry) nextRevisionLocked() uint64 {
	r.revision++
	if r.revision == 0 {
		panic("local workflow registry revision exhausted")
	}
	return r.revision
}

func (r *workflowRegistry) newWorkflowIDLocked() types.WorkflowID {
	for {
		id := types.WorkflowID(uuid.NewString())
		if _, occupied := r.byID[id]; !occupied {
			return id
		}
	}
}

func workflowRevisionMatches(rec backend.WorkflowRecord, expected backend.WorkflowRevision) bool {
	return rec.ID == expected.ID &&
		rec.Key == expected.Key &&
		rec.Version == expected.Version &&
		rec.DefinitionHash == expected.DefinitionHash &&
		rec.RegistryRevision == expected.RegistryRevision
}

func workflowReplaceConflict(kind backend.WorkflowReplaceConflictKind) error {
	return &backend.WorkflowReplaceConflictError{Kind: kind}
}

func workflowReplaceFingerprint(expected backend.WorkflowRevision, replacement backend.WorkflowRecord) ([sha256.Size]byte, error) {
	// Expected's mutable CAS fields are deliberately excluded. After a
	// response is lost, a caller may re-read the committed current revision and
	// retry the same logical operation. The operation remains the same when its
	// source ID and full replacement are the same; the ledger lookup above the
	// source CAS then recovers the original result.
	payload, err := json.Marshal(struct {
		ExpectedID  types.WorkflowID       `json:"expected_id"`
		Replacement backend.WorkflowRecord `json:"replacement"`
	}{
		ExpectedID:  expected.ID,
		Replacement: replacement,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("fingerprint workflow replacement: %w", err)
	}
	return sha256.Sum256(payload), nil
}
