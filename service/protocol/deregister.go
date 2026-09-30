package protocol

import "context"

// DeregisterPath is the runner-facing endpoint a runner calls on graceful
// shutdown to give up its live session.
//
// It exists for the live-instance guard: while a session is live, a
// registration from a different instance under the same runner ID is refused.
// Without an explicit goodbye, a replacement pod that reuses the ID (a
// StatefulSet member recreated with a new pod UID) waits out the full live
// window. Deregistering ends the window at once.
//
// It is HTTP only, like ReportMetricsPath and RenewIdentityPath. Not calling
// it is harmless: the session simply ages out of the live window.
const DeregisterPath = "/v1/runners/deregister"

// DeregisterRequest names the session being given up. SessionID fences the
// call: a stale session cannot end its replacement's liveness. AuthToken has
// the same header-override role as RenewIdentityRequest.AuthToken; this struct
// must never be logged whole.
type DeregisterRequest struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
	AuthToken string `json:"auth_token,omitempty"`
}

// Deregister gives up this runner's live session.
func (c *Client) Deregister(ctx context.Context, req DeregisterRequest) error {
	return c.post(ctx, DeregisterPath, req, nil)
}
