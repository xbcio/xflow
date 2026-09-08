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
