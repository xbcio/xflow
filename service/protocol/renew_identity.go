package protocol

import "context"

// RenewIdentityPath is the runner-facing endpoint a runner calls to extend its
// own issued identity's validity.
//
// It is deliberately NOT named renew: RenewLeasePath already exists and means
// task/group lease renewal. Two endpoints both called "renew" is exactly how a
// reader wires the wrong one.
const RenewIdentityPath = "/v1/runners/renew-identity"

// RenewIdentityRequest carries the runner id and its auth token.
//
// AuthToken exists for the same reason HeartbeatRequest.AuthToken does: the
// real transport is the Authorization header, and overrideTokenFromHeader
// overwrites this field from that header on every call. But the override
// writes INTO this field, so the field has to exist in the body for the
// header value to have anywhere to land — without it, the header is read but
// then dropped, and the server always authenticates against an empty string.
// A conforming client never populates it in the body it sends. This struct
// must never be logged or printed whole: only RunnerID is safe to surface.
type RenewIdentityRequest struct {
	RunnerID  string `json:"runner_id"`
	AuthToken string `json:"auth_token,omitempty"`
}

// RenewIdentityResponse returns the new expiry, RFC3339 UTC, or empty when the
// server issues no expiry at all (no TTL configured on the server — the
// runner should stop its renewal loop).
//
// The token is NOT rotated and is therefore NOT returned. Rotation would have
// to hot-swap the credential baked into the live protocol client, and it buys
// nothing against a stolen token — the thief can renew too. Revocation, not
// rotation, is the answer to theft.
type RenewIdentityResponse struct {
	ExpiresAt string `json:"expires_at,omitempty"`
}

// RenewIdentity extends this runner's identity.
func (c *Client) RenewIdentity(ctx context.Context, req RenewIdentityRequest) (RenewIdentityResponse, error) {
	var resp RenewIdentityResponse
	if err := c.post(ctx, RenewIdentityPath, req, &resp); err != nil {
		return RenewIdentityResponse{}, err
	}
	return resp, nil
}
