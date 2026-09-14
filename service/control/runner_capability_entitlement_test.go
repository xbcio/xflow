package control

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func capabilityEntitlementPolicyStore(t *testing.T, nodeTypes []string) *FilePolicyStore {
	t.Helper()
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:              "team-a",
			IDPrefix:          "runner-",
			Token:             "team-a-token",
			AllowedNodeTypes:  nodeTypes,
			AllowedNamespaces: []string{"*"},
		}},
	}, false)
	if err != nil {
		t.Fatalf("policy store: %v", err)
	}
	return store
}

func TestRegisterRejectsUnauthorizedCapability(t *testing.T) {
	c := newEntitlementTestCore(t, capabilityEntitlementPolicyStore(t, []string{"xflow.function"}))

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		AuthToken:    "team-a-token",
		Capabilities: []protocol.Capability{{NodeType: "xflow.script"}},
	}, TransportInfo{})

	if !errors.Is(err, ErrAuthCapabilityDenied) {
		t.Fatalf("want ErrAuthCapabilityDenied, got %v", err)
	}
	if _, ok := c.runners.Runner(context.Background(), "runner-1"); ok {
		t.Fatal("unauthorized-capability runner was registered")
	}
}

func TestRegisterAcceptsAuthorizedCapabilities(t *testing.T) {
	c := newEntitlementTestCore(t, capabilityEntitlementPolicyStore(t, []string{"xflow.function", "xflow.script"}))

	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Capabilities: []protocol.Capability{
			{NodeType: "xflow.function"},
			{NodeType: "xflow.script"},
		},
	}, TransportInfo{}); err != nil {
		t.Fatalf("register rejected authorized capabilities: %v", err)
	}
}

func TestRegisterWildcardPolicyAllowsDeclaredCapability(t *testing.T) {
	c := newEntitlementTestCore(t, capabilityEntitlementPolicyStore(t, []string{"*"}))

	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		AuthToken:    "team-a-token",
		Capabilities: []protocol.Capability{{NodeType: "custom.node"}},
	}, TransportInfo{}); err != nil {
		t.Fatalf("wildcard policy rejected declared capability: %v", err)
	}
}

func TestRegisterRejectsBlankCapability(t *testing.T) {
	for _, nodeType := range []string{"", " \t "} {
		t.Run(fmt.Sprintf("%q", nodeType), func(t *testing.T) {
			c := newEntitlementTestCore(t, capabilityEntitlementPolicyStore(t, []string{"*"}))

			_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
				RunnerID:     "runner-1",
				Concurrency:  1,
				AuthToken:    "team-a-token",
				Capabilities: []protocol.Capability{{NodeType: nodeType}},
			}, TransportInfo{})

			if !errors.Is(err, ErrInvalidCapability) {
				t.Fatalf("want ErrInvalidCapability, got %v", err)
			}
			if errors.Is(err, ErrAuthCapabilityDenied) {
				t.Fatalf("blank capability must be invalid rather than denied, got %v", err)
			}
		})
	}
}

func TestCapabilityDenialIsObservable(t *testing.T) {
	obs := &recordingNamespaceAuthObserver{}
	c := newEntitlementTestCore(t, capabilityEntitlementPolicyStore(t, []string{"xflow.function"}))
	c.authObserver = obs

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  1,
		AuthToken:    "team-a-token",
		Capabilities: []protocol.Capability{{NodeType: "xflow.script"}},
	}, TransportInfo{})
	if !errors.Is(err, ErrAuthCapabilityDenied) {
		t.Fatalf("setup: want ErrAuthCapabilityDenied, got %v", err)
	}

	want := []string{"register:allow", "register:deny_capability"}
	if len(obs.decisions) != len(want) {
		t.Fatalf("auth decisions = %v, want exactly %v", obs.decisions, want)
	}
	for i := range want {
		if obs.decisions[i] != want[i] {
			t.Fatalf("auth decisions = %v, want exactly %v", obs.decisions, want)
		}
	}
}

func TestNormalizeRunnerErrorPreservesRegistrationEntitlementSentinels(t *testing.T) {
	for _, want := range []error{ErrInvalidCapability, ErrAuthNamespaceDenied, ErrAuthCapabilityDenied} {
		got := normalizeRunnerError(fmt.Errorf("wrapped: %w", want), nil, "register")
		if !errors.Is(got, want) {
			t.Fatalf("normalizeRunnerError(%v) = %v, want sentinel preserved", want, got)
		}
	}
}
