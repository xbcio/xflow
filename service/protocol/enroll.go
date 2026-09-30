package protocol

import "context"

// EnrollPath is the unauthenticated enrollment endpoint. A runner that has no
// identity yet posts a registration code here and receives one back.
const EnrollPath = "/v1/runners/enroll"

// EnrollRequest is what a not-yet-identified runner sends.
type EnrollRequest struct {
	// RegistrationCode travels in the body, never in a header: an Authorization
	// header is echoed into proxy and gateway access logs by default, and this
	// value is a reusable credential.
	RegistrationCode string `json:"registration_code"`
	// ProposedRunnerID is an audit hint ONLY. The server generates the real id
	// and never honors this field — otherwise "enroll as an id that already
	// exists" would be an identity-takeover path.
	ProposedRunnerID string `json:"proposed_runner_id,omitempty"`
	// Namespaces / NodeTypes are what this runner intends to serve. The server
	// intersects them against the code's ceiling and rejects anything outside.
	Namespaces []string `json:"namespaces,omitempty"`
	NodeTypes  []string `json:"node_types,omitempty"`
	// SystemID is the instance's idempotency key within a pool (pod name,
	// hostname, or a persisted random value). A pool-bound code maps
	// (pool, SystemID) to one stable runner ID; re-enrolling rotates the token
	// and returns the same ID. Empty takes the legacy path: a new runner ID on
	// every enroll.
	SystemID string `json:"system_id,omitempty"`
	// InstanceUID identifies the process instance (pod UID); see
	// RegisterRunnerRequest.InstanceUID.
	InstanceUID string `json:"instance_uid,omitempty"`
}

// EnrollResponse is the issued identity. Token is returned exactly once; the
// server keeps only its hash.
type EnrollResponse struct {
	RunnerID string `json:"runner_id"`
	Token    string `json:"token"`
	// ExpiresAt is RFC3339 UTC, or empty when the identity never expires. It is
	// an appended field: a runner built before it existed unmarshals the same
	// response and simply ignores it, which is what makes rolling the server
	// ahead of the fleet safe.
	ExpiresAt string `json:"expires_at,omitempty"`
	// Namespaces is the namespace set the runner must register for. A runner
	// that declared none uses it verbatim; empty (an older server) keeps the
	// runner's own default.
	Namespaces []string `json:"namespaces,omitempty"`
	// CredentialGeneration is the token's generation (1 on first issue). Its
	// presence also tells a runner the server supports idempotent re-enroll.
	CredentialGeneration int64 `json:"credential_generation,omitempty"`
	// Labels are the pool's server-owned labels, merged into registration.
	Labels map[string]string `json:"labels,omitempty"`
}

// Enroll exchanges a registration code for a runner identity. It deliberately
// carries no bearer token: at this point the caller has nothing to bear.
func (c *Client) Enroll(ctx context.Context, req EnrollRequest) (EnrollResponse, error) {
	var resp EnrollResponse
	if err := c.post(ctx, EnrollPath, req, &resp); err != nil {
		return EnrollResponse{}, err
	}
	return resp, nil
}
