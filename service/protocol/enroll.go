package protocol

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
}
