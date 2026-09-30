package apiserver

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

type runnerPoolCreateRequest struct {
	Name              string              `json:"name"`
	OwnerKind         store.PoolOwnerKind `json:"owner_kind"`
	AllowedNamespaces []string            `json:"allowed_namespaces"`
	AllowedNodeTypes  []string            `json:"allowed_node_types"`
	Labels            map[string]string   `json:"labels,omitempty"`
	MaxInstances      int                 `json:"max_instances,omitempty"`
	InheritNamespaces bool                `json:"inherit_namespaces,omitempty"`
	Paused            bool                `json:"paused,omitempty"`
}

type runnerPoolUpdateRequest struct {
	Name              *string            `json:"name,omitempty"`
	AllowedNamespaces *[]string          `json:"allowed_namespaces,omitempty"`
	AllowedNodeTypes  *[]string          `json:"allowed_node_types,omitempty"`
	Labels            *map[string]string `json:"labels,omitempty"`
	MaxInstances      *int               `json:"max_instances,omitempty"`
	InheritNamespaces *bool              `json:"inherit_namespaces,omitempty"`
	Paused            *bool              `json:"paused,omitempty"`
}

type runnerPoolView struct {
	ID                string              `json:"id"`
	Name              string              `json:"name"`
	OwnerKind         store.PoolOwnerKind `json:"owner_kind"`
	OwnerNamespace    string              `json:"owner_namespace"`
	AllowedNamespaces []string            `json:"allowed_namespaces"`
	AllowedNodeTypes  []string            `json:"allowed_node_types"`
	Labels            map[string]string   `json:"labels"`
	MaxInstances      int                 `json:"max_instances"`
	InheritNamespaces bool                `json:"inherit_namespaces"`
	Paused            bool                `json:"paused"`
	CreatedAt         string              `json:"created_at"`
}

type runnerPoolTokenCreateRequest struct {
	ExpiresInSeconds *int64 `json:"expires_in_seconds,omitempty"`
}

type runnerPoolInstanceView struct {
	SystemID       string              `json:"system_id"`
	RunnerID       string              `json:"runner_id"`
	InstanceUID    string              `json:"instance_uid,omitempty"`
	State          store.InstanceState `json:"state"`
	CreatedAt      string              `json:"created_at"`
	LastEnrolledAt string              `json:"last_enrolled_at"`
}

func newRunnerPoolView(pool store.RunnerPool) runnerPoolView {
	labels := pool.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return runnerPoolView{
		ID: pool.ID, Name: pool.Name, OwnerKind: pool.OwnerKind,
		OwnerNamespace:    pool.OwnerNamespace,
		AllowedNamespaces: emptyIfNil(pool.AllowedNamespaces),
		AllowedNodeTypes:  emptyIfNil(pool.AllowedNodeTypes), Labels: labels,
		MaxInstances: pool.MaxInstances, InheritNamespaces: pool.InheritNamespaces,
		Paused:    pool.Paused,
		CreatedAt: pool.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func runnerPoolUnavailable(w http.ResponseWriter, r *http.Request) {
	writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
}

func runnerPoolNotFound(w http.ResponseWriter, r *http.Request) {
	writeFail(w, r, http.StatusNotFound, "runner_pool_not_found", "runner pool not found")
}

func runnerPoolScopeFor(r *http.Request, globalScope string) (control.OwnerScope, bool) {
	return ownerScopeFor(r, globalScope)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// scopeNarrower reports whether next grants no value that current did not.
func scopeNarrower(current, next []string) bool {
	if containsString(current, "*") {
		return true
	}
	if containsString(next, "*") {
		return false
	}
	for _, value := range next {
		if !containsString(current, value) {
			return false
		}
	}
	return true
}

func validatePoolNodeTypes(p Principal, nodeTypes []string) error {
	if !p.HasScope(ScopeManagementRunnerPoolWriteGlobal) && containsString(nodeTypes, "*") {
		return errors.New("allowed_node_types wildcard requires management.runner_pool.write_global")
	}
	return nil
}

func resolvePoolNamespaces(p Principal, requested []string) ([]string, error) {
	return resolveRequestedNamespacesForGlobalScope(p, requested, ScopeManagementRunnerPoolWriteGlobal)
}

func generateRunnerPoolID() (string, error) {
	id, _, err := control.GenerateRegistrationCode()
	if err != nil {
		return "", err
	}
	return "pool-" + id, nil
}

func (m *managementModule) handleCreateRunnerPool(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	p, ok := principalFromRequest(r)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	var req runnerPoolCreateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Name) == "" || req.MaxInstances < 0 {
		writeFail(w, r, http.StatusBadRequest, "bad_request", "name is required and max_instances must not be negative")
		return
	}
	if req.OwnerKind == "" {
		req.OwnerKind = store.PoolOwnerTenant
	}
	if req.OwnerKind != store.PoolOwnerTenant && req.OwnerKind != store.PoolOwnerPlatform {
		writeFail(w, r, http.StatusBadRequest, "bad_request", "owner_kind must be tenant or platform")
		return
	}
	if req.OwnerKind == store.PoolOwnerPlatform && !p.HasScope(ScopeManagementRunnerPoolWriteGlobal) {
		writeFail(w, r, http.StatusForbidden, "runner_pool_forbidden", "platform runner pools require global scope")
		return
	}
	namespaces, err := resolvePoolNamespaces(p, req.AllowedNamespaces)
	if err != nil {
		status, code := http.StatusForbidden, "namespace_forbidden"
		if errors.Is(err, errRunnerPoolMissingNamespaces) {
			status, code = http.StatusBadRequest, "bad_request"
		}
		writeFail(w, r, status, code, err.Error())
		return
	}
	if err := validatePoolNodeTypes(p, req.AllowedNodeTypes); err != nil {
		writeFail(w, r, http.StatusForbidden, "node_types_forbidden", err.Error())
		return
	}
	id, err := generateRunnerPoolID()
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	ownerNamespace := p.Namespace
	if req.OwnerKind == store.PoolOwnerPlatform {
		ownerNamespace = ""
	}
	pool := store.RunnerPool{
		ID: id, Name: strings.TrimSpace(req.Name), OwnerKind: req.OwnerKind,
		OwnerNamespace: ownerNamespace, AllowedNamespaces: namespaces,
		AllowedNodeTypes: append([]string(nil), req.AllowedNodeTypes...), Labels: req.Labels,
		MaxInstances: req.MaxInstances, InheritNamespaces: req.InheritNamespaces,
		Paused: req.Paused, CreatedAt: time.Now().UTC(),
	}
	if err := m.pools.CreatePool(r.Context(), pool); err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusCreated, newRunnerPoolView(pool))
}

func (m *managementModule) handleListRunnerPools(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	scope, ok := runnerPoolScopeFor(r, ScopeManagementRunnerPoolReadGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	pools, err := m.pools.ListPools(r.Context(), scope)
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	out := make([]runnerPoolView, 0, len(pools))
	for _, pool := range pools {
		out = append(out, newRunnerPoolView(pool))
	}
	writeData(w, r, http.StatusOK, out)
}

func (m *managementModule) getRunnerPool(r *http.Request, globalScope string) (store.RunnerPool, error) {
	scope, ok := runnerPoolScopeFor(r, globalScope)
	if !ok {
		return store.RunnerPool{}, store.ErrOwnerScopeUnset
	}
	return m.pools.GetPool(r.Context(), r.PathValue("id"), scope)
}

func (m *managementModule) handleGetRunnerPool(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	pool, err := m.getRunnerPool(r, ScopeManagementRunnerPoolReadGlobal)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, newRunnerPoolView(pool))
}

func (m *managementModule) handleUpdateRunnerPool(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	p, ok := principalFromRequest(r)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	scope, ok := runnerPoolScopeFor(r, ScopeManagementRunnerPoolWriteGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	pool, err := m.pools.GetPool(r.Context(), r.PathValue("id"), scope)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	var req runnerPoolUpdateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name != nil {
		if strings.TrimSpace(*req.Name) == "" {
			writeFail(w, r, http.StatusBadRequest, "bad_request", "name must not be empty")
			return
		}
		pool.Name = strings.TrimSpace(*req.Name)
	}
	if req.AllowedNamespaces != nil {
		next, err := resolvePoolNamespaces(p, *req.AllowedNamespaces)
		if err != nil {
			writeFail(w, r, http.StatusForbidden, "namespace_forbidden", err.Error())
			return
		}
		if !scopeNarrower(pool.AllowedNamespaces, next) {
			writeFail(w, r, http.StatusBadRequest, "runner_pool_scope_widening", "allowed_namespaces may only be narrowed")
			return
		}
		pool.AllowedNamespaces = next
	}
	if req.AllowedNodeTypes != nil {
		if err := validatePoolNodeTypes(p, *req.AllowedNodeTypes); err != nil {
			writeFail(w, r, http.StatusForbidden, "node_types_forbidden", err.Error())
			return
		}
		if !scopeNarrower(pool.AllowedNodeTypes, *req.AllowedNodeTypes) {
			writeFail(w, r, http.StatusBadRequest, "runner_pool_scope_widening", "allowed_node_types may only be narrowed")
			return
		}
		pool.AllowedNodeTypes = append([]string(nil), (*req.AllowedNodeTypes)...)
	}
	if req.Labels != nil {
		pool.Labels = *req.Labels
	}
	if req.MaxInstances != nil {
		if *req.MaxInstances < 0 {
			writeFail(w, r, http.StatusBadRequest, "bad_request", "max_instances must not be negative")
			return
		}
		pool.MaxInstances = *req.MaxInstances
	}
	if req.InheritNamespaces != nil {
		pool.InheritNamespaces = *req.InheritNamespaces
	}
	if req.Paused != nil {
		pool.Paused = *req.Paused
	}
	if err := m.pools.UpdatePool(r.Context(), pool, scope); errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	} else if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, newRunnerPoolView(pool))
}

func (m *managementModule) handleDeleteRunnerPool(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	scope, ok := runnerPoolScopeFor(r, ScopeManagementRunnerPoolWriteGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	id := r.PathValue("id")
	if err := m.pools.DeletePool(r.Context(), id, scope); errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	} else if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, map[string]string{"id": id, "status": "deleted"})
}

func (m *managementModule) handleCreateRunnerPoolToken(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil || m.codes == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	pool, err := m.getRunnerPool(r, ScopeManagementRunnerPoolWriteGlobal)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	var req runnerPoolTokenCreateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	now := time.Now().UTC()
	expiresAt, err := resolveRegistrationCodeExpiry(now, m.registrationCodeTTL, req.ExpiresInSeconds)
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	id, plaintext, err := control.GenerateRegistrationCode()
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	code := control.RegistrationCode{
		ID: id, CodeHash: control.HashSecret(plaintext), PoolID: pool.ID,
		OwnerNamespace:    pool.OwnerNamespace,
		AllowedNamespaces: append([]string(nil), pool.AllowedNamespaces...),
		AllowedNodeTypes:  append([]string(nil), pool.AllowedNodeTypes...),
		CreatedAt:         now, ExpiresAt: expiresAt, MaxUses: 0,
	}
	if err := m.codes.Create(r.Context(), code); err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	response := runnerPoolTokenCreateResponse{ID: id, Code: plaintext, MaxUses: 0}
	if !expiresAt.IsZero() {
		response.ExpiresAt = expiresAt.Format(time.RFC3339)
	}
	w.Header().Set("Cache-Control", "no-store")
	writeData(w, r, http.StatusOK, response)
}

func (m *managementModule) poolCodes(r *http.Request, globalScope string) ([]control.RegistrationCode, error) {
	pool, err := m.getRunnerPool(r, globalScope)
	if err != nil {
		return nil, err
	}
	scope, ok := runnerPoolScopeFor(r, globalScope)
	if !ok {
		return nil, store.ErrOwnerScopeUnset
	}
	codes, err := m.codes.List(r.Context(), scope)
	if err != nil {
		return nil, err
	}
	out := make([]control.RegistrationCode, 0)
	for _, code := range codes {
		if code.PoolID == pool.ID {
			out = append(out, code)
		}
	}
	return out, nil
}

func (m *managementModule) handleListRunnerPoolTokens(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil || m.codes == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	codes, err := m.poolCodes(r, ScopeManagementRunnerPoolReadGlobal)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	out := make([]runnerPoolTokenView, 0, len(codes))
	for _, code := range codes {
		out = append(out, newRunnerPoolTokenView(code))
	}
	writeData(w, r, http.StatusOK, out)
}

func (m *managementModule) handleRevokeRunnerPoolToken(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil || m.codes == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	codes, err := m.poolCodes(r, ScopeManagementRunnerPoolWriteGlobal)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	tokenID := r.PathValue("token_id")
	found := false
	for _, code := range codes {
		if code.ID == tokenID {
			found = true
			break
		}
	}
	if !found {
		writeFail(w, r, http.StatusNotFound, "runner_pool_token_not_found", "runner pool token not found")
		return
	}
	scope, _ := runnerPoolScopeFor(r, ScopeManagementRunnerPoolWriteGlobal)
	if err := m.codes.Revoke(r.Context(), tokenID, scope); err != nil {
		if errors.Is(err, store.ErrRegistrationCodeNotFound) {
			writeFail(w, r, http.StatusNotFound, "runner_pool_token_not_found", "runner pool token not found")
		} else {
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		}
		return
	}
	writeData(w, r, http.StatusOK, map[string]string{"id": tokenID, "status": "revoked"})
}

func (m *managementModule) handleRunnerPoolTokenAudit(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil || m.codes == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	codes, err := m.poolCodes(r, ScopeManagementRunnerPoolReadGlobal)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	tokenID := r.PathValue("token_id")
	found := false
	for _, code := range codes {
		if code.ID == tokenID {
			found = true
			break
		}
	}
	if !found {
		writeFail(w, r, http.StatusNotFound, "runner_pool_token_not_found", "runner pool token not found")
		return
	}
	scope, ok := runnerPoolScopeFor(r, ScopeManagementRunnerPoolReadGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	records, err := m.codes.EnrollAudit(r.Context(), tokenID, scope)
	if errors.Is(err, control.ErrRegistrationCodeNotFound) {
		writeFail(w, r, http.StatusNotFound, "runner_pool_token_not_found", "runner pool token not found")
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeData(w, r, http.StatusOK, newEnrollAuditViews(records))
}

func (m *managementModule) handleListRunnerPoolInstances(w http.ResponseWriter, r *http.Request) {
	if m.pools == nil {
		runnerPoolUnavailable(w, r)
		return
	}
	scope, ok := runnerPoolScopeFor(r, ScopeManagementRunnerPoolReadGlobal)
	if !ok {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	instances, err := m.pools.ListInstances(r.Context(), r.PathValue("id"), scope)
	if errors.Is(err, store.ErrRunnerPoolNotFound) {
		runnerPoolNotFound(w, r)
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	out := make([]runnerPoolInstanceView, 0, len(instances))
	for _, instance := range instances {
		out = append(out, runnerPoolInstanceView{
			SystemID: instance.SystemID, RunnerID: instance.RunnerID,
			InstanceUID: instance.InstanceUID, State: instance.State,
			CreatedAt:      instance.CreatedAt.UTC().Format(time.RFC3339),
			LastEnrolledAt: instance.LastEnrolledAt.UTC().Format(time.RFC3339),
		})
	}
	writeData(w, r, http.StatusOK, out)
}
