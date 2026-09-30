package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

func TestBuildServerOptionsSharesRunnerPoolStoreWithManagementAndEnroll(t *testing.T) {
	cfg := serverConfig{
		mode: "dev", enroll: true, management: true,
		trustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
	}
	codes := control.NewMemoryRegistrationCodeStore()
	ids := control.NewMemoryIssuedIdentityStore()
	pools := control.NewMemoryRunnerPoolStore()
	auth := apiserver.NewBearerPrincipalAuth("0123456789abcdef0123456789abcdef", "ops", []string{
		"management.runner_pool.read", "management.runner_pool.write",
	})
	deps := serverDeps{
		workflowAuth: auth, principalAuth: auth, audit: apiserver.NewInMemoryAuditSink(),
		registrationCodeStore: codes, issuedIdentityStore: ids, runnerPoolStore: pools,
	}
	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{}, buildServerOptions(cfg, deps)...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	request := func(method, path, body string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer 0123456789abcdef0123456789abcdef")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if path == protocol.EnrollPath {
			req.Header.Set("X-Forwarded-For", "198.51.100.77")
		}
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp, data
	}

	resp, body := request(http.MethodPost, "/v1/management/runner-pools",
		`{"name":"workers","allowed_namespaces":["default"],"allowed_node_types":["xflow.http"]}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create pool status=%d body=%s", resp.StatusCode, body)
	}
	var poolEnvelope struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &poolEnvelope); err != nil || poolEnvelope.Data.ID == "" {
		t.Fatalf("decode pool: err=%v body=%s", err, body)
	}

	resp, body = request(http.MethodPost, "/v1/management/runner-pools/"+poolEnvelope.Data.ID+"/tokens", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create token status=%d body=%s", resp.StatusCode, body)
	}
	var tokenEnvelope struct {
		Data struct {
			ID   string `json:"id"`
			Code string `json:"code"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &tokenEnvelope); err != nil || tokenEnvelope.Data.Code == "" {
		t.Fatalf("decode token: err=%v body=%s", err, body)
	}

	enrollBody, err := json.Marshal(protocol.EnrollRequest{
		RegistrationCode: tokenEnvelope.Data.Code, SystemID: "pod-1", InstanceUID: "pod-uid-1",
		Namespaces: []string{"default"}, NodeTypes: []string{"xflow.http"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, body = request(http.MethodPost, protocol.EnrollPath, string(enrollBody))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("enroll status=%d body=%s", resp.StatusCode, body)
	}
	var enrolled protocol.EnrollResponse
	if err := json.Unmarshal(body, &enrolled); err != nil {
		t.Fatalf("decode enroll: %v body=%s", err, body)
	}
	if enrolled.RunnerID == "" || enrolled.CredentialGeneration != 1 {
		t.Fatalf("enroll response=%+v", enrolled)
	}
	audit, err := codes.EnrollAudit(context.Background(), tokenEnvelope.Data.ID, control.OwnerScope{All: true})
	if err != nil || len(audit) != 1 {
		t.Fatalf("enroll audit=%+v err=%v, want one record", audit, err)
	}
	if audit[0].SourceIP != "198.51.100.77" {
		t.Fatalf("enroll audit source IP=%q, want forwarded client IP", audit[0].SourceIP)
	}
}

func TestParseServerConfigTrustedProxies(t *testing.T) {
	cfg, err := parseServerConfig([]string{"--mode=dev", "--trusted-proxies=10.0.0.7/8, 2001:db8::/32"})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	want := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32")}
	if len(cfg.trustedProxies) != len(want) {
		t.Fatalf("trusted proxies = %v, want %v", cfg.trustedProxies, want)
	}
	for i := range want {
		if cfg.trustedProxies[i] != want[i] {
			t.Fatalf("trusted proxy %d = %v, want %v", i, cfg.trustedProxies[i], want[i])
		}
	}
	if _, err := parseServerConfig([]string{"--mode=dev", "--trusted-proxies=not-a-cidr"}); err == nil || !strings.Contains(err.Error(), "invalid CIDR") {
		t.Fatalf("invalid trusted proxies error = %v, want invalid CIDR", err)
	}
}
