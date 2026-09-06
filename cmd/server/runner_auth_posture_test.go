package main

import (
	"errors"
	"strings"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/control"
)

// The SDK requires every server to declare a runner-auth posture, and rejects
// declaring more than one. This binary previously passed WithServerAuth
// unconditionally — including the DisabledAuthenticator{} that an empty
// --auth-policy produces, which control.IsConfigured does not accept. The
// result was a server that could not start in any mode, reporting SDK option
// names to an operator who only has flags.

func policyAuth(t *testing.T) control.Authenticator {
	t.Helper()
	auth, err := control.NewStaticTokenAuthenticator("runner-", "tok", []string{"default"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestRunnerAuthPostureForDerivesEachCase(t *testing.T) {
	disabled := control.DisabledAuthenticator{}

	tests := []struct {
		name   string
		auth   control.Authenticator
		enroll bool
		want   runnerAuthPosture
	}{
		// --auth-policy set: the loaded policy store is the posture, whether
		// or not enrollment is also on. Enrollment issues identities that this
		// same store then checks, so it must not shadow the policy.
		{"policy only", policyAuth(t), false, posturePolicy},
		{"policy and enroll", policyAuth(t), true, posturePolicy},

		// --enroll alone: identities arrive at enrollment time. Passing
		// WithServerInsecureNoRunnerAuth here would be rejected by NewServer
		// as a conflicting declaration.
		{"enroll only", disabled, true, postureEnroll},

		// Neither: the permissive dev default. This is the case that used to
		// make the binary unstartable.
		{"neither", disabled, false, postureInsecure},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := runnerAuthPostureFor(tc.auth, tc.enroll); got != tc.want {
				t.Fatalf("runnerAuthPostureFor = %v, want %v", got, tc.want)
			}
		})
	}
}

// serverOptsForPosture mirrors the switch in main: given a posture, which
// declaration option does the binary pass? Kept here so the test exercises the
// same three-way mapping main does rather than a paraphrase of it.
func serverOptsForPosture(p runnerAuthPosture, auth control.Authenticator) []xflowsdk.ServerOption {
	switch p {
	case posturePolicy:
		return []xflowsdk.ServerOption{xflowsdk.WithServerAuth(auth)}
	case postureInsecure:
		return []xflowsdk.ServerOption{xflowsdk.WithServerInsecureNoRunnerAuth()}
	default:
		return nil
	}
}

// TestEveryPostureSatisfiesTheSDKGate is the one with teeth: it builds a real
// server for each posture and asserts NewServer accepts the declaration. A
// posture that declares nothing trips ErrRunnerAuthPostureUndeclared; one that
// declares two trips the mutual-exclusion error. Both are start-up failures
// that no unit test of the mapping alone would catch.
func TestEveryPostureSatisfiesTheSDKGate(t *testing.T) {
	auth := policyAuth(t)
	disabled := control.DisabledAuthenticator{}

	tests := []struct {
		name   string
		auth   control.Authenticator
		enroll bool
	}{
		{"--auth-policy", auth, false},
		{"--auth-policy --enroll", auth, true},
		{"--enroll", disabled, true},
		{"no runner auth flags", disabled, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := serverOptsForPosture(runnerAuthPostureFor(tc.auth, tc.enroll), tc.auth)
			if tc.enroll {
				opts = append(opts, xflowsdk.WithServerEnroll(
					control.NewMemoryRegistrationCodeStore(),
					control.NewMemoryIssuedIdentityStore()))
			}

			_, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, opts...)
			if errors.Is(err, xflowsdk.ErrRunnerAuthPostureUndeclared) {
				t.Fatalf("posture declared nothing the SDK recognises: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "mutually exclusive") {
				t.Fatalf("posture declared two conflicting options: %v", err)
			}
			if err != nil {
				t.Fatalf("NewServer rejected a valid posture: %v", err)
			}
		})
	}
}
