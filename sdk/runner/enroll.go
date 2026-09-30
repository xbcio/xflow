package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/xbcio/xflow/service/protocol"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// enrollHTTPTimeout is an absolute safety net around the single enrollment
// call: connection setup plus full body read. The caller's context still
// governs normal cancellation.
const enrollHTTPTimeout = 30 * time.Second

// resolveRunnerIdentity settles who this runner is before anything connects.
//
// Precedence, highest first:
//  1. an identity already in the store — a restart reuses it and never spends
//     another registration token;
//  2. a registration token — enroll, then persist what was issued;
//  3. neither — leave cfg untouched, which is the pre-enrollment behavior
//     (--id plus a statically configured --token).
//
// The enrollment call is HTTP even when --transport=grpc: the enroll endpoint
// is mounted only on the control plane's HTTP server, and no gRPC enroll
// client exists anywhere in the tree. A gRPC runner that enrolls therefore
// needs --server to be a reachable http(s) origin in addition to
// --grpc-target.
//
// Enroll mode accepts no explicitly configured runner ID: the server is the
// sole ID issuer, so file, env, or flag configuration fails before any network
// request instead of being silently overridden.
func resolveRunnerIdentity(ctx context.Context, cfg runnerConfig, store identityStore) (runnerConfig, error) {
	stored, ok, err := store.Load()
	if err != nil {
		return cfg, err
	}
	if ok {
		if err := validateProfileRunnerID(cfg.profile, stored.RunnerID); err != nil {
			return cfg, err
		}
		cfg.runnerID = stored.RunnerID
		cfg.token = stored.Token
		return cfg, nil
	}
	if strings.TrimSpace(cfg.registrationToken) == "" {
		if err := validateProfileRunnerID(cfg.profile, cfg.runnerID); err != nil {
			return cfg, err
		}
		return cfg, nil
	}
	if cfg.runnerIDExplicit {
		return cfg, errors.New("runner id must not be configured when enrolling with a registration token")
	}

	// Enrollment's transport is fixed at HTTP regardless of --transport (see
	// the function comment above), so validateTransportSecurity's per-transport
	// reasoning never looks at cfg.serverURL under the grpc transport — it has
	// no reason to, since a grpc runner without a registration token never
	// dials it. This code path is the exception: it dials cfg.serverURL over
	// HTTP no matter which transport was chosen, so it needs its own gate
	// here, at the one place that knows enrollment is about to happen,
	// exercised before the HTTP client (and the registration token) go anywhere
	// near the network.
	if err := validateEnrollTransportSecurity(cfg); err != nil {
		return cfg, err
	}

	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		return cfg, err
	}
	httpClient, err := xflowsdk.NewRunnerHTTPClient(sdkCfg, enrollHTTPTimeout)
	if err != nil {
		return cfg, fmt.Errorf("enroll: build http client: %w", err)
	}

	resp, err := protocol.NewClient(cfg.serverURL, httpClient).Enroll(ctx, protocol.EnrollRequest{
		RegistrationCode: cfg.registrationToken,
		Namespaces:       declaredNamespaceStrings(cfg),
		NodeTypes:        capabilityNodeTypes(cfg.capabilities),
		SystemID:         cfg.systemID,
		InstanceUID:      xflowsdk.DefaultRunnerInstanceUID(),
	})
	if err != nil {
		// The code itself is never echoed: an error string ends up in logs.
		return cfg, fmt.Errorf("enroll at %s: %w", cfg.serverURL, err)
	}
	if strings.TrimSpace(resp.RunnerID) == "" || strings.TrimSpace(resp.Token) == "" {
		return cfg, fmt.Errorf("enroll at %s returned an incomplete identity", cfg.serverURL)
	}
	if len(resp.Namespaces) == 0 {
		return cfg, fmt.Errorf("enroll at %s returned no namespaces", cfg.serverURL)
	}
	if len(cfg.namespaces) == 0 {
		cfg.namespaces = parseNamespaces(resp.Namespaces)
	}
	cfg.labels, err = mergeEnrolledLabels(cfg, resp.Labels)
	if err != nil {
		return cfg, fmt.Errorf("enroll at %s: %w", cfg.serverURL, err)
	}

	issued := identity{RunnerID: resp.RunnerID, Token: resp.Token}
	if err := validateProfileRunnerID(cfg.profile, issued.RunnerID); err != nil {
		return cfg, err
	}
	if err := store.Save(issued); err != nil {
		// Saving is not optional: an unsaved identity means the next restart
		// re-enrolls, and a one-time code will not be there to do it with.
		return cfg, fmt.Errorf("persist enrolled identity: %w", err)
	}
	slog.Info("runner enrolled", "runner_id", issued.RunnerID, "identity_store", cfg.identityStoreKind)
	cfg.runnerID = issued.RunnerID
	cfg.token = issued.Token
	return cfg, nil
}

func declaredNamespaceStrings(cfg runnerConfig) []string {
	if len(cfg.namespaces) == 0 {
		return nil
	}
	out := make([]string, len(cfg.namespaces))
	for i, ns := range cfg.namespaces {
		out[i] = string(ns)
	}
	return out
}

func mergeEnrolledLabels(cfg runnerConfig, serverLabels map[string]string) (map[string]string, error) {
	if len(serverLabels) == 0 {
		return cfg.labels, nil
	}
	manualLabels := parseLabels(cfg.labelRaw)
	if len(cfg.profile.RequiredLabels) > 0 && manualLabels == nil {
		manualLabels = make(map[string]string, len(cfg.profile.RequiredLabels))
	}
	for key, value := range cfg.profile.RequiredLabels {
		manualLabels[key] = value
	}
	autoLabels := map[string]string(nil)
	if cfg.autoLabels {
		autoLabels = detectRunnerLabels()
	}
	merged := cloneStringMap(cfg.labels)
	if merged == nil {
		merged = make(map[string]string, len(serverLabels))
	}
	for key, value := range serverLabels {
		if _, exists := manualLabels[key]; exists {
			return nil, fmt.Errorf("server-owned label %q conflicts with a runner label", key)
		}
		if _, exists := merged[key]; exists {
			if _, detected := autoLabels[key]; !detected {
				return nil, fmt.Errorf("server-owned label %q conflicts with a runner label", key)
			}
		}
		merged[key] = value
	}
	return merged, nil
}

// validateEnrollTransportSecurity refuses to send a registration token over a
// plaintext connection.
//
// This lives here rather than as part of validateTransportSecurity in
// config.go, even though the two share a purpose, because the two gates see
// different transports. validateTransportSecurity judges cfg.serverURL only
// under the http transport, and judges the grpc transport solely by TLS
// material — it is right to do so, because under --transport=grpc every
// *other* runner call goes out over gRPC, so cfg.serverURL's scheme is simply
// not the signal for that traffic. Enrollment breaks that assumption: it
// always dials cfg.serverURL over HTTP, independent of --transport, because
// no gRPC enroll client exists anywhere in the tree (see the enroll endpoint
// comment above). A config-time gate keyed on --transport therefore cannot
// see this call coming.
//
// It has two call sites, both intentional, neither a substitute for the
// other, and both keyed on the same two-part test: cfg.registrationToken is
// non-empty AND no identity is already stored. Neither half is sufficient on
// its own. The registration token alone is not: resolveRunnerIdentity (this
// file) checks store.Load() first and returns immediately on a stored
// identity, before it ever looks at the registration token, so a runner that
// already enrolled and still carries a stale --registration-token in its
// environment (nothing forces it to be cleared on restart) never dials out
// and never needs this gate — keying only on the registration token would
// misjudge that deployment as an upcoming plaintext enrollment and refuse it.
// The stored-identity check alone is not sufficient either: with no
// registration token configured there is nothing to enroll with, stored
// identity or not.
//
// validateRunnerConfig (config.go) calls it during config validation, under
// that same conjunction, reusing the store it already built to validate
// --identity-store/--identity-file — reached by every subcommand that shares
// bindRunnerFlags, including `config validate` and `verify` — so that a
// registration token paired with a plaintext --server fails the same "does
// this config even make sense" check that catches every other malformed
// value, rather than passing a preflight and only then crash-looping in
// `run`. resolveRunnerIdentity (this file) calls it again immediately before
// the HTTP client goes out: that is the one call site that actually knows
// enrollment is happening now, with the live cfg.serverURL and registration
// code in hand, so it stays as the last word before the network — config
// validation checking first does not make it safe to remove the check that
// runs right before the dial.
//
// The registration token is never included in the returned error: it is a
// reusable credential, and an error string routinely ends up in logs.
func validateEnrollTransportSecurity(cfg runnerConfig) error {
	u, err := url.Parse(cfg.serverURL)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("enroll: --server must be an absolute http or https URL: %q", cfg.serverURL)
	}
	if cfg.allowPlaintext {
		return nil
	}
	if u.Scheme == "https" {
		return nil
	}
	return fmt.Errorf(
		"refusing to enroll: --server %q is plaintext, so the registration token and the issued "+
			"runner token would cross the network in the clear; use an https:// URL or pass "+
			"--allow-plaintext to accept the risk",
		cfg.serverURL)
}
