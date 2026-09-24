package action

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

const (
	browserCDPNodeType            = "xflow.browser.cdp"
	defaultBrowserMaxContexts     = 1
	defaultBrowserQueueTimeout    = 5 * time.Second
	defaultBrowserConnectTimeout  = 5 * time.Second
	defaultBrowserNavigateTimeout = 30 * time.Second
	defaultBrowserTotalTimeout    = 45 * time.Second
	defaultBrowserSettle          = 2 * time.Second
	defaultBrowserRetries         = 3
	defaultBrowserRetryBackoff    = 2 * time.Second
	defaultBrowserEarlyRatio      = 0.1
	defaultBrowserEarlyMin        = 5 * time.Minute
	defaultBrowserTTLCap          = 4 * time.Hour

	maxBrowserURLBytes        = 16 << 10
	maxBrowserStringBytes     = 16 << 10
	maxBrowserListItems       = 256
	maxBrowserRetries         = 100
	maxBrowserCapturedHeaders = 128
	maxBrowserHeaderValues    = 16
	maxBrowserCredentialBytes = 1 << 20
	maxBrowserDiscoveryBytes  = 1 << 20
)

const (
	browserHostDeniedSourceEndpoints          = "endpoints"
	browserHostDeniedSourceNavigationPolicy   = "navigation_policy"
	browserHostDeniedSourceInterceptedRequest = "intercepted_request"

	browserTimeoutPhaseQueue   = "queue"
	browserTimeoutPhaseConnect = "connect"
	browserTimeoutPhaseRun     = "run"

	browserRejectedURLInvalid = "invalid"
)

// browserPausedRequestBufSize bounds the queue between the CDP event listener
// and the single worker that continues each paused request.
//
// It was 128, which real pages exceed instantly: measured against a ULP login
// landing page, the first second produces 406 paused requests, so 261 were
// dropped. A dropped event is not merely an uncaptured header — its request is
// never continued, so the page hangs and the node fails with
// browser.resource_exhausted. At 4096 the same page shows zero overflow (peak
// queue depth 388) and becomes ready in ~10s.
//
// The value has to absorb a whole page-load BURST, not a steady rate: the worker
// drains one request per CDP round trip, so no per-request speed makes it keep up
// with 400 requests arriving in the same second.
const browserPausedRequestBufSize = 4096

// CDPNode implements xflow.browser.cdp. It is deliberately a credential
// harvesting primitive, not a general-purpose browser automation API.
type CDPNode struct {
	nodeinternal.BaseNode
	Params map[string]any
}

func init() {
	registry.Register(&CDPNode{})
}

// BrowserCDP creates a restricted remote-CDP credential-harvesting node.
// Params use the same schema documented by Descriptor and the DSL; values are
// validated strictly at execution time after expression evaluation.
func BrowserCDP(params map[string]any) *CDPNode {
	return &CDPNode{Params: params}
}

func (n *CDPNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:         browserCDPNodeType,
		Kind:         types.NodeKindAction,
		DisplayName:  "Browser Harvest (CDP)",
		Capabilities: []string{"browser.cdp.v1"},
		Params: []types.ParamSpec{
			{Name: "debugging_url", DisplayName: "CDP Endpoint", Type: types.ParamString, Required: true, Description: "Remote Chrome DevTools endpoint"},
			{Name: "entry_url", DisplayName: "Entry URL", Type: types.ParamString, Required: true},
			{Name: "seed_cookies", DisplayName: "Seed Cookies", Type: types.ParamArray},
			{Name: "plan", DisplayName: "Navigation Plan", Type: types.ParamObject},
			{Name: "harvest", DisplayName: "Harvest Rules", Type: types.ParamObject},
			{Name: "ttl", DisplayName: "TTL Rules", Type: types.ParamObject},
			{Name: "target_host", DisplayName: "Target Host", Type: types.ParamString},
			{Name: "timeout_ms", DisplayName: "Navigate Timeout (ms)", Type: types.ParamNumber, Default: 30000},
			{Name: "total_timeout_ms", DisplayName: "Total Timeout (ms)", Type: types.ParamNumber, Default: 45000},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *CDPNode) NodeType() string { return browserCDPNodeType }

func (n *CDPNode) OnError(strategy types.OnError) types.Builder {
	n.SetOnError(strategy)
	return n
}

func (n *CDPNode) RawParams() any { return n.Params }

// BrowserCDPConfig is process-wide admission configuration for CDP nodes.
// Endpoints contains hostname patterns only: exact hosts, suffixes
// prefixed by '.', or wildcards prefixed by "*.". An empty list denies
// every CDP endpoint. Each installed snapshot owns its own concurrency semaphore.
type BrowserCDPConfig struct {
	Endpoints      []string
	MaxContexts    int
	QueueTimeout   time.Duration
	ConnectTimeout time.Duration
}

// BrowserCDPConfigDefaults returns fail-closed defaults.
func BrowserCDPConfigDefaults() BrowserCDPConfig {
	return BrowserCDPConfig{
		Endpoints:      nil,
		MaxContexts:    defaultBrowserMaxContexts,
		QueueTimeout:   defaultBrowserQueueTimeout,
		ConnectTimeout: defaultBrowserConnectTimeout,
	}
}

type browserConfigSnapshot struct {
	config BrowserCDPConfig
	sem    chan struct{}
}

var browserConfigState = struct {
	sync.Mutex
	snapshot *browserConfigSnapshot
	leases   int
}{snapshot: newBrowserConfigSnapshot(BrowserCDPConfigDefaults())}

func newBrowserConfigSnapshot(cfg BrowserCDPConfig) *browserConfigSnapshot {
	cfg.Endpoints = append([]string(nil), cfg.Endpoints...)
	return &browserConfigSnapshot{config: cfg, sem: make(chan struct{}, cfg.MaxContexts)}
}

// AcquireBrowserCDPConfig installs cfg while one or more runner instances hold
// a lease. Identical configurations share the snapshot; conflicting live
// configurations fail rather than silently changing another runner's policy.
func AcquireBrowserCDPConfig(cfg BrowserCDPConfig) (func(), error) {
	normalized, err := normalizeBrowserCDPConfig(cfg)
	if err != nil {
		return nil, err
	}

	browserConfigState.Lock()
	defer browserConfigState.Unlock()
	if browserConfigState.leases > 0 {
		if !sameBrowserCDPConfig(browserConfigState.snapshot.config, normalized) {
			return nil, fmt.Errorf("browser CDP configuration conflicts with an active runner")
		}
	} else {
		browserConfigState.snapshot = newBrowserConfigSnapshot(normalized)
	}
	browserConfigState.leases++

	var once sync.Once
	return func() {
		once.Do(func() {
			browserConfigState.Lock()
			defer browserConfigState.Unlock()
			browserConfigState.leases--
			if browserConfigState.leases == 0 {
				browserConfigState.snapshot = newBrowserConfigSnapshot(BrowserCDPConfigDefaults())
			}
		})
	}, nil
}

func normalizeBrowserCDPConfig(cfg BrowserCDPConfig) (BrowserCDPConfig, error) {
	if cfg.MaxContexts <= 0 {
		return BrowserCDPConfig{}, fmt.Errorf("browser CDP max contexts must be positive")
	}
	if cfg.QueueTimeout <= 0 {
		return BrowserCDPConfig{}, fmt.Errorf("browser CDP queue timeout must be positive")
	}
	if cfg.ConnectTimeout <= 0 {
		return BrowserCDPConfig{}, fmt.Errorf("browser CDP connect timeout must be positive")
	}

	patterns, err := normalizedHostPatterns(cfg.Endpoints)
	if err != nil {
		return BrowserCDPConfig{}, fmt.Errorf("browser CDP endpoints contains an invalid host")
	}
	cfg.Endpoints = patterns
	return cfg, nil
}

func sameBrowserCDPConfig(a, b BrowserCDPConfig) bool {
	if a.MaxContexts != b.MaxContexts || a.QueueTimeout != b.QueueTimeout || a.ConnectTimeout != b.ConnectTimeout {
		return false
	}
	return reflect.DeepEqual(a.Endpoints, b.Endpoints)
}

func currentBrowserConfigSnapshot() *browserConfigSnapshot {
	browserConfigState.Lock()
	defer browserConfigState.Unlock()
	return browserConfigState.snapshot
}

type browserSeedCookie struct {
	Name   string
	Value  string
	URL    string
	Path   string
	Secure bool
}

type browserNavigationPlan struct {
	WaitSelector   string
	Settle         time.Duration
	FallbackRoot   bool
	FallbackReload bool
	FallbackSettle time.Duration
	Retries        int
	RetryBackoff   time.Duration
	RetryOn        []string
}

type browserHarvestRules struct {
	CookieURLs            []string
	CookieExcludePrefixes []string
	HeaderExclude         map[string]struct{}
	RequestHost           string
	SkipSuffixes          []string
	APIPathPatterns       []string
}

type browserTTLRules struct {
	EarlyExpireRatio float64
	EarlyExpireMin   time.Duration
	CredentialTTLCap time.Duration
}

type browserParams struct {
	DebuggingURL *url.URL
	EntryURL     *url.URL
	FallbackURL  *url.URL
	TargetHost   string
	SeedCookies  []browserSeedCookie
	Plan         browserNavigationPlan
	Harvest      browserHarvestRules
	TTL          browserTTLRules
	Timeout      time.Duration
	TotalTimeout time.Duration
	Policy       HostPolicy
}

type browserParamError struct{ field string }

func (e *browserParamError) Error() string { return "invalid browser parameter: " + e.field }

type browserHostDeniedError struct {
	source      string
	rejectedURL string
}

func (*browserHostDeniedError) Error() string { return "browser destination denied" }

type browserTimeoutError struct {
	phase string
}

func (*browserTimeoutError) Error() string { return "browser operation timed out" }

type browserUnavailableError struct{ cause error }

func (e *browserUnavailableError) Error() string {
	if e.cause == nil {
		return "remote browser unavailable"
	}
	return e.cause.Error()
}
func (e *browserUnavailableError) Unwrap() error { return e.cause }

type browserNavigationError struct{ cause error }

func (e *browserNavigationError) Error() string {
	if e.cause == nil {
		return "browser navigation failed"
	}
	return e.cause.Error()
}
func (e *browserNavigationError) Unwrap() error { return e.cause }

type browserResourceError struct{ cause error }

func (e *browserResourceError) Error() string {
	if e.cause == nil {
		return "browser resource limit reached"
	}
	return e.cause.Error()
}
func (e *browserResourceError) Unwrap() error { return e.cause }

func parseBrowserCDPParams(raw map[string]any, snapshot *browserConfigSnapshot, policy HostPolicy) (*browserParams, error) {
	if raw == nil {
		return nil, &browserParamError{field: "parameters"}
	}
	if err := rejectUnknownKeys(raw, "parameters", "debugging_url", "entry_url", "seed_cookies", "plan", "harvest", "ttl", "target_host", "timeout_ms", "total_timeout_ms"); err != nil {
		return nil, err
	}

	debugRaw, err := requiredBrowserString(raw, "debugging_url", false)
	if err != nil {
		return nil, err
	}
	debugURL, err := parseBrowserURL("debugging_url", debugRaw, "http", "https", "ws", "wss")
	if err != nil || !validBrowserDebuggingURL(debugURL) {
		return nil, &browserParamError{field: "debugging_url"}
	}
	debugHost, err := normalizeHostOnly(debugURL.Hostname())
	if err != nil || !browserEndpointAllowed(snapshot.config.Endpoints, debugHost) {
		return nil, newBrowserHostDeniedError(browserHostDeniedSourceEndpoints, debugURL.String())
	}

	entryRaw, err := requiredBrowserString(raw, "entry_url", false)
	if err != nil {
		return nil, err
	}
	entryURL, err := parseBrowserURL("entry_url", entryRaw, "http", "https")
	if err != nil {
		return nil, err
	}

	targetHost := ""
	if value, exists := raw["target_host"]; exists {
		var ok bool
		targetHost, ok = value.(string)
		if !ok || targetHost == "" {
			return nil, &browserParamError{field: "target_host"}
		}
		if targetHost, err = normalizeHostOnly(targetHost); err != nil {
			return nil, &browserParamError{field: "target_host"}
		}
	} else {
		targetHost, err = normalizeHostOnly(entryURL.Hostname())
		if err != nil {
			return nil, &browserParamError{field: "entry_url"}
		}
	}

	p := &browserParams{
		DebuggingURL: debugURL,
		EntryURL:     entryURL,
		TargetHost:   targetHost,
		Policy:       policy,
		Plan: browserNavigationPlan{
			WaitSelector:   "body",
			Settle:         defaultBrowserSettle,
			FallbackRoot:   true,
			FallbackReload: true,
			FallbackSettle: defaultBrowserSettle,
			Retries:        defaultBrowserRetries,
			RetryBackoff:   defaultBrowserRetryBackoff,
			RetryOn:        []string{"ERR_INSUFFICIENT_RESOURCES"},
		},
		TTL: browserTTLRules{
			EarlyExpireRatio: defaultBrowserEarlyRatio,
			EarlyExpireMin:   defaultBrowserEarlyMin,
			CredentialTTLCap: defaultBrowserTTLCap,
		},
		Timeout:      defaultBrowserNavigateTimeout,
		TotalTimeout: defaultBrowserTotalTimeout,
	}

	if err := parseBrowserSeedCookies(raw, p); err != nil {
		return nil, err
	}
	if err := parseBrowserPlan(raw, p); err != nil {
		return nil, err
	}
	if err := parseBrowserHarvest(raw, p); err != nil {
		return nil, err
	}
	if err := parseBrowserTTL(raw, p); err != nil {
		return nil, err
	}
	if p.Timeout, err = optionalMilliseconds(raw, "timeout_ms", p.Timeout, false); err != nil {
		return nil, err
	}
	if p.TotalTimeout, err = optionalMilliseconds(raw, "total_timeout_ms", p.TotalTimeout, false); err != nil {
		return nil, err
	}

	p.FallbackURL, err = parseBrowserURL("fallback", entryURL.Scheme+"://"+hostForBrowserURL(targetHost)+"/", "http", "https")
	if err != nil {
		return nil, &browserParamError{field: "target_host"}
	}

	// All known destinations are checked before the semaphore is acquired and
	// before any CDP connection can be attempted. Redirect and subresource URLs
	// are checked again by Fetch interception in the real executor.
	if err := applyBrowserHostPolicy(policy, entryURL.Hostname(), entryURL.String()); err != nil {
		return nil, err
	}
	if p.Plan.FallbackRoot {
		if err := applyBrowserHostPolicy(policy, p.FallbackURL.Hostname(), p.FallbackURL.String()); err != nil {
			return nil, err
		}
	}
	if err := applyBrowserHostPolicy(policy, p.Harvest.RequestHost, browserURLForHost(p.Harvest.RequestHost)); err != nil {
		return nil, err
	}
	for _, cookieURL := range p.Harvest.CookieURLs {
		u, _ := url.Parse(cookieURL)
		if err := applyBrowserHostPolicy(policy, u.Hostname(), cookieURL); err != nil {
			return nil, err
		}
	}
	for _, cookie := range p.SeedCookies {
		u, _ := url.Parse(cookie.URL)
		if err := applyBrowserHostPolicy(policy, u.Hostname(), cookie.URL); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func parseBrowserSeedCookies(raw map[string]any, p *browserParams) error {
	value, exists := raw["seed_cookies"]
	if !exists {
		return nil
	}
	items, err := browserArray(value, "seed_cookies")
	if err != nil {
		return err
	}
	if len(items) > maxBrowserListItems {
		return &browserParamError{field: "seed_cookies"}
	}
	p.SeedCookies = make([]browserSeedCookie, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok || m == nil {
			return &browserParamError{field: "seed_cookies"}
		}
		if err := rejectUnknownKeys(m, "seed_cookies", "name", "value", "url", "path", "secure"); err != nil {
			return err
		}
		name, err := requiredBrowserString(m, "name", false)
		if err != nil || !validCookieName(name) {
			return &browserParamError{field: "seed_cookies.name"}
		}
		value, err := requiredBrowserString(m, "value", true)
		if err != nil || len(value) > maxBrowserStringBytes || strings.ContainsAny(value, "\r\n\x00") {
			return &browserParamError{field: "seed_cookies.value"}
		}
		cookieURLRaw, err := requiredBrowserString(m, "url", false)
		if err != nil {
			return &browserParamError{field: "seed_cookies.url"}
		}
		cookieURL, err := parseBrowserURL("seed_cookies.url", cookieURLRaw, "http", "https")
		if err != nil {
			return err
		}
		path := "/"
		if pathValue, ok := m["path"]; ok {
			path, ok = pathValue.(string)
			if !ok || path == "" || !strings.HasPrefix(path, "/") || len(path) > maxBrowserStringBytes {
				return &browserParamError{field: "seed_cookies.path"}
			}
		}
		secure := false
		if secureValue, ok := m["secure"]; ok {
			secure, ok = secureValue.(bool)
			if !ok {
				return &browserParamError{field: "seed_cookies.secure"}
			}
		}
		p.SeedCookies = append(p.SeedCookies, browserSeedCookie{Name: name, Value: value, URL: cookieURL.String(), Path: path, Secure: secure})
	}
	return nil
}

func parseBrowserPlan(raw map[string]any, p *browserParams) error {
	plan, exists, err := optionalBrowserMap(raw, "plan")
	if err != nil || !exists {
		return err
	}
	if err := rejectUnknownKeys(plan, "plan", "navigate", "fallback", "retries", "retry_backoff_ms", "retry_on"); err != nil {
		return err
	}
	if navigate, ok, err := optionalBrowserMap(plan, "navigate"); err != nil {
		return err
	} else if ok {
		if err := rejectUnknownKeys(navigate, "plan.navigate", "wait_selector", "settle_ms"); err != nil {
			return err
		}
		if value, exists := navigate["wait_selector"]; exists {
			selector, ok := value.(string)
			if !ok || selector == "" || len(selector) > maxBrowserStringBytes {
				return &browserParamError{field: "plan.navigate.wait_selector"}
			}
			p.Plan.WaitSelector = selector
		}
		if p.Plan.Settle, err = optionalMilliseconds(navigate, "settle_ms", p.Plan.Settle, true); err != nil {
			return err
		}
	}
	if fallback, ok, err := optionalBrowserMap(plan, "fallback"); err != nil {
		return err
	} else if ok {
		if err := rejectUnknownKeys(fallback, "plan.fallback", "navigate_to_root", "reload", "settle_ms"); err != nil {
			return err
		}
		if p.Plan.FallbackRoot, err = optionalBrowserBool(fallback, "navigate_to_root", p.Plan.FallbackRoot); err != nil {
			return err
		}
		if p.Plan.FallbackReload, err = optionalBrowserBool(fallback, "reload", p.Plan.FallbackReload); err != nil {
			return err
		}
		if p.Plan.FallbackSettle, err = optionalMilliseconds(fallback, "settle_ms", p.Plan.FallbackSettle, true); err != nil {
			return err
		}
	}
	if value, exists := plan["retries"]; exists {
		retries, err := strictBrowserInteger(value, "plan.retries")
		if err != nil || retries < 0 || retries > maxBrowserRetries {
			return &browserParamError{field: "plan.retries"}
		}
		p.Plan.Retries = int(retries)
	}
	if p.Plan.RetryBackoff, err = optionalMilliseconds(plan, "retry_backoff_ms", p.Plan.RetryBackoff, true); err != nil {
		return err
	}
	if value, exists := plan["retry_on"]; exists {
		p.Plan.RetryOn, err = browserStringArray(value, "plan.retry_on", false)
		if err != nil {
			return err
		}
	}
	return nil
}

func parseBrowserHarvest(raw map[string]any, p *browserParams) error {
	p.Harvest = browserHarvestRules{
		CookieURLs: []string{
			"https://" + hostForBrowserURL(p.TargetHost) + "/",
			"http://" + hostForBrowserURL(p.TargetHost) + "/",
		},
		HeaderExclude: make(map[string]struct{}),
		RequestHost:   p.TargetHost,
	}
	harvest, exists, err := optionalBrowserMap(raw, "harvest")
	if err != nil || !exists {
		return err
	}
	if err := rejectUnknownKeys(harvest, "harvest", "cookie_urls", "cookie_name_exclude_prefixes", "header_exclude", "request_scope"); err != nil {
		return err
	}
	if value, ok := harvest["cookie_urls"]; ok {
		items, err := browserStringArray(value, "harvest.cookie_urls", false)
		if err != nil || len(items) == 0 {
			return &browserParamError{field: "harvest.cookie_urls"}
		}
		p.Harvest.CookieURLs = make([]string, 0, len(items))
		for _, item := range items {
			item = strings.ReplaceAll(item, "{target_host}", hostForBrowserURL(p.TargetHost))
			u, err := parseBrowserURL("harvest.cookie_urls", item, "http", "https")
			if err != nil {
				return err
			}
			if !strings.EqualFold(u.Hostname(), p.TargetHost) {
				return &browserParamError{field: "harvest.cookie_urls"}
			}
			p.Harvest.CookieURLs = append(p.Harvest.CookieURLs, u.String())
		}
	}
	if value, ok := harvest["cookie_name_exclude_prefixes"]; ok {
		p.Harvest.CookieExcludePrefixes, err = browserStringArray(value, "harvest.cookie_name_exclude_prefixes", true)
		if err != nil {
			return err
		}
	}
	if value, ok := harvest["header_exclude"]; ok {
		excluded, err := browserStringArray(value, "harvest.header_exclude", true)
		if err != nil {
			return err
		}
		for _, name := range excluded {
			p.Harvest.HeaderExclude[strings.ToLower(name)] = struct{}{}
		}
	}
	if scope, ok, err := optionalBrowserMap(harvest, "request_scope"); err != nil {
		return err
	} else if ok {
		if err := rejectUnknownKeys(scope, "harvest.request_scope", "host", "skip_suffixes", "api_path_patterns", "whole_host_match"); err != nil {
			return err
		}
		if value, exists := scope["host"]; exists {
			host, ok := value.(string)
			if !ok || host == "" {
				return &browserParamError{field: "harvest.request_scope.host"}
			}
			p.Harvest.RequestHost, err = normalizeHostOnly(host)
			if err != nil {
				return &browserParamError{field: "harvest.request_scope.host"}
			}
		}
		if value, exists := scope["skip_suffixes"]; exists {
			p.Harvest.SkipSuffixes, err = browserStringArray(value, "harvest.request_scope.skip_suffixes", true)
			if err != nil {
				return err
			}
			for i := range p.Harvest.SkipSuffixes {
				p.Harvest.SkipSuffixes[i] = strings.ToLower(p.Harvest.SkipSuffixes[i])
			}
		}
		if value, exists := scope["api_path_patterns"]; exists {
			p.Harvest.APIPathPatterns, err = browserStringArray(value, "harvest.request_scope.api_path_patterns", true)
			if err != nil {
				return err
			}
			for i := range p.Harvest.APIPathPatterns {
				p.Harvest.APIPathPatterns[i] = strings.ToLower(p.Harvest.APIPathPatterns[i])
			}
		}
		wholeHost, err := optionalBrowserBool(scope, "whole_host_match", true)
		if err != nil {
			return err
		}
		if !wholeHost {
			return &browserParamError{field: "harvest.request_scope.whole_host_match"}
		}
	}
	return nil
}

func parseBrowserTTL(raw map[string]any, p *browserParams) error {
	ttl, exists, err := optionalBrowserMap(raw, "ttl")
	if err != nil || !exists {
		return err
	}
	if err := rejectUnknownKeys(ttl, "ttl", "early_expire_ratio", "early_expire_min_ms", "credential_ttl_cap_ms"); err != nil {
		return err
	}
	if value, ok := ttl["early_expire_ratio"]; ok {
		ratio, err := strictBrowserNumber(value, "ttl.early_expire_ratio")
		if err != nil || ratio < 0 || ratio > 1 {
			return &browserParamError{field: "ttl.early_expire_ratio"}
		}
		p.TTL.EarlyExpireRatio = ratio
	}
	if p.TTL.EarlyExpireMin, err = optionalMilliseconds(ttl, "early_expire_min_ms", p.TTL.EarlyExpireMin, true); err != nil {
		return err
	}
	if p.TTL.CredentialTTLCap, err = optionalMilliseconds(ttl, "credential_ttl_cap_ms", p.TTL.CredentialTTLCap, false); err != nil {
		return err
	}
	return nil
}

func rejectUnknownKeys(m map[string]any, prefix string, allowed ...string) error {
	set := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	for key := range m {
		if _, ok := set[key]; !ok {
			return &browserParamError{field: prefix}
		}
	}
	return nil
}

func requiredBrowserString(m map[string]any, key string, allowEmpty bool) (string, error) {
	value, ok := m[key]
	if !ok {
		return "", &browserParamError{field: key}
	}
	result, ok := value.(string)
	if !ok || (!allowEmpty && result == "") || result != strings.TrimSpace(result) || len(result) > maxBrowserStringBytes {
		return "", &browserParamError{field: key}
	}
	return result, nil
}

func optionalBrowserMap(m map[string]any, key string) (map[string]any, bool, error) {
	value, ok := m[key]
	if !ok {
		return nil, false, nil
	}
	result, ok := value.(map[string]any)
	if !ok || result == nil {
		return nil, false, &browserParamError{field: key}
	}
	return result, true, nil
}

func optionalBrowserBool(m map[string]any, key string, fallback bool) (bool, error) {
	value, ok := m[key]
	if !ok {
		return fallback, nil
	}
	result, ok := value.(bool)
	if !ok {
		return false, &browserParamError{field: key}
	}
	return result, nil
}

func browserArray(value any, field string) ([]any, error) {
	if value == nil {
		return nil, &browserParamError{field: field}
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Array && rv.Kind() != reflect.Slice {
		return nil, &browserParamError{field: field}
	}
	if rv.Len() > maxBrowserListItems {
		return nil, &browserParamError{field: field}
	}
	result := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result, nil
}

func browserStringArray(value any, field string, allowEmpty bool) ([]string, error) {
	items, err := browserArray(value, field)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok || (!allowEmpty && text == "") || text != strings.TrimSpace(text) || len(text) > maxBrowserStringBytes {
			return nil, &browserParamError{field: field}
		}
		result = append(result, text)
	}
	return result, nil
}

func strictBrowserNumber(value any, field string) (float64, error) {
	var result float64
	switch n := value.(type) {
	case int:
		result = float64(n)
	case int8:
		result = float64(n)
	case int16:
		result = float64(n)
	case int32:
		result = float64(n)
	case int64:
		result = float64(n)
	case uint:
		result = float64(n)
	case uint8:
		result = float64(n)
	case uint16:
		result = float64(n)
	case uint32:
		result = float64(n)
	case uint64:
		result = float64(n)
	case float32:
		result = float64(n)
	case float64:
		result = n
	case json.Number:
		var err error
		result, err = strconv.ParseFloat(string(n), 64)
		if err != nil {
			return 0, &browserParamError{field: field}
		}
	default:
		return 0, &browserParamError{field: field}
	}
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0, &browserParamError{field: field}
	}
	return result, nil
}

func strictBrowserInteger(value any, field string) (int64, error) {
	number, err := strictBrowserNumber(value, field)
	if err != nil || math.Trunc(number) != number || number > float64(math.MaxInt64) || number < float64(math.MinInt64) {
		return 0, &browserParamError{field: field}
	}
	return int64(number), nil
}

func optionalMilliseconds(m map[string]any, key string, fallback time.Duration, allowZero bool) (time.Duration, error) {
	value, ok := m[key]
	if !ok {
		return fallback, nil
	}
	millis, err := strictBrowserInteger(value, key)
	maxMillis := int64(math.MaxInt64 / int64(time.Millisecond))
	if err != nil || millis < 0 || (!allowZero && millis == 0) || millis > maxMillis {
		return 0, &browserParamError{field: key}
	}
	return time.Duration(millis) * time.Millisecond, nil
}

func parseBrowserURL(field, raw string, schemes ...string) (*url.URL, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || len(raw) > maxBrowserURLBytes {
		return nil, &browserParamError{field: field}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.Opaque != "" || u.User != nil || u.Hostname() == "" {
		return nil, &browserParamError{field: field}
	}
	allowed := false
	for _, scheme := range schemes {
		if strings.EqualFold(u.Scheme, scheme) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, &browserParamError{field: field}
	}
	if _, err := normalizeHostOnly(u.Hostname()); err != nil {
		return nil, &browserParamError{field: field}
	}
	return u, nil
}

func hostForBrowserURL(host string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
}

func browserEndpointAllowed(allowlist []string, host string) bool {
	patterns, err := compileHostPatterns(allowlist)
	return err == nil && hostPatternsMatch(patterns, host)
}

func browserURLForHost(host string) string {
	normalized, err := normalizeHostOnly(host)
	if err != nil {
		return ""
	}
	return (&url.URL{Scheme: "https", Host: hostForBrowserURL(normalized), Path: "/"}).String()
}

func newBrowserHostDeniedError(source, rejectedURL string) *browserHostDeniedError {
	return &browserHostDeniedError{source: source, rejectedURL: rejectedURL}
}

func applyBrowserHostPolicy(policy HostPolicy, host, rejectedURL string) error {
	if policy == nil {
		return newBrowserHostDeniedError(browserHostDeniedSourceNavigationPolicy, rejectedURL)
	}
	normalized, err := normalizeHostOnly(host)
	if err != nil || policy(normalized) != nil {
		return newBrowserHostDeniedError(browserHostDeniedSourceNavigationPolicy, rejectedURL)
	}
	return nil
}

// resolveBrowserWebSocketEndpoint converts an HTTP(S) DevTools endpoint to its
// websocket URL through /json/version. It deliberately uses a redirect-free,
// bounded client because this request happens before chromedp's CDP transport
// can enforce any of the node's navigation guards.
func resolveBrowserWebSocketEndpoint(ctx context.Context, debuggingURL *url.URL, snapshot *browserConfigSnapshot) (*url.URL, error) {
	if debuggingURL == nil || snapshot == nil {
		return nil, &browserParamError{field: "debugging_url"}
	}
	host, err := normalizeHostOnly(debuggingURL.Hostname())
	if err != nil {
		return nil, &browserParamError{field: "debugging_url"}
	}
	if !browserEndpointAllowed(snapshot.config.Endpoints, host) {
		return nil, newBrowserHostDeniedError(browserHostDeniedSourceEndpoints, debuggingURL.String())
	}

	switch strings.ToLower(debuggingURL.Scheme) {
	case "ws", "wss":
		if !validBrowserWebSocketEndpoint(debuggingURL) {
			return nil, &browserParamError{field: "debugging_url"}
		}
		return debuggingURL, nil
	case "http", "https":
		if debuggingURL.RawQuery != "" || debuggingURL.Fragment != "" || !validBrowserDiscoveryPath(debuggingURL.EscapedPath()) {
			return nil, &browserParamError{field: "debugging_url"}
		}
	default:
		return nil, &browserParamError{field: "debugging_url"}
	}

	discoveryURL := *debuggingURL
	discoveryURL.Path = "/json/version"
	discoveryURL.RawPath = ""
	discoveryURL.RawQuery = ""
	discoveryURL.Fragment = ""
	discoveryCtx, cancel := context.WithTimeout(ctx, snapshot.config.ConnectTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(discoveryCtx, http.MethodGet, discoveryURL.String(), nil)
	if err != nil {
		return nil, &browserParamError{field: "debugging_url"}
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		if discoveryCtx.Err() != nil {
			return nil, &browserTimeoutError{phase: browserTimeoutPhaseConnect}
		}
		return nil, &browserUnavailableError{cause: err}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, &browserUnavailableError{}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBrowserDiscoveryBytes+1))
	if err != nil {
		if discoveryCtx.Err() != nil {
			return nil, &browserTimeoutError{phase: browserTimeoutPhaseConnect}
		}
		return nil, &browserUnavailableError{cause: err}
	}
	if len(body) > maxBrowserDiscoveryBytes {
		return nil, &browserResourceError{}
	}
	var payload struct {
		WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.WebSocketDebuggerURL == "" {
		return nil, &browserUnavailableError{}
	}
	endpoint, err := url.Parse(payload.WebSocketDebuggerURL)
	if err != nil || !validBrowserWebSocketEndpoint(endpoint) {
		return nil, &browserUnavailableError{}
	}
	endpointHost, err := normalizeHostOnly(endpoint.Hostname())
	if err != nil || !browserEndpointAllowed(snapshot.config.Endpoints, endpointHost) {
		return nil, newBrowserHostDeniedError(browserHostDeniedSourceEndpoints, endpoint.String())
	}
	return endpoint, nil
}

func validBrowserDebuggingURL(debuggingURL *url.URL) bool {
	if debuggingURL == nil {
		return false
	}
	switch strings.ToLower(debuggingURL.Scheme) {
	case "http", "https":
		return debuggingURL.RawQuery == "" && debuggingURL.Fragment == "" && validBrowserDiscoveryPath(debuggingURL.EscapedPath())
	case "ws", "wss":
		return validBrowserWebSocketEndpoint(debuggingURL)
	default:
		return false
	}
}

func validBrowserDiscoveryPath(path string) bool {
	return path == "" || path == "/" || path == "/json/version"
}

func validBrowserWebSocketEndpoint(endpoint *url.URL) bool {
	if endpoint == nil || endpoint.User != nil || endpoint.Hostname() == "" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return false
	}
	if !strings.EqualFold(endpoint.Scheme, "ws") && !strings.EqualFold(endpoint.Scheme, "wss") {
		return false
	}
	path := endpoint.EscapedPath()
	if !strings.HasPrefix(path, "/devtools/browser/") || strings.TrimPrefix(path, "/devtools/browser/") == "" || strings.Contains(strings.TrimPrefix(path, "/devtools/browser/"), "/") {
		return false
	}
	return !strings.Contains(strings.TrimPrefix(path, "/devtools/browser/"), "%")
}

type browserAttemptResult struct {
	Headers      http.Header
	Cookies      []*network.Cookie
	FallbackUsed bool
}

type browserAttemptExecutorFunc func(context.Context, *browserParams, *browserConfigSnapshot) (*browserAttemptResult, error)

// executeBrowserCDPAttempt is a package seam so parsing, retry, admission and
// classification tests do not need a live Chrome. Production always starts
// with the real implementation below.
var executeBrowserCDPAttempt browserAttemptExecutorFunc = realBrowserCDPAttempt
var browserCDPNow = time.Now

func (n *CDPNode) Execute(ctx context.Context, input *types.Input) (out *types.Output, err error) {
	defer func() {
		if recover() != nil {
			out = nil
			err = browserUnavailableClassified()
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if input == nil {
		return nil, browserInvalidParamsClassified()
	}

	snapshot := currentBrowserConfigSnapshot()
	params, parseErr := parseBrowserCDPParams(input.Params, snapshot, HTTPHostPolicy)
	if parseErr != nil {
		var denied *browserHostDeniedError
		if errors.As(parseErr, &denied) {
			return nil, browserHostDeniedClassified(denied)
		}
		return nil, browserInvalidParamsClassified()
	}
	if input.Timeout > 0 && input.Timeout < params.TotalTimeout {
		params.TotalTimeout = input.Timeout
	}

	totalCtx, totalCancel := context.WithTimeout(ctx, params.TotalTimeout)
	defer totalCancel()
	if err := acquireBrowserSlot(totalCtx, snapshot); err != nil {
		var timeout *browserTimeoutError
		if errors.As(err, &timeout) {
			return nil, browserTimeoutClassified(timeout)
		}
		return nil, browserTimeoutClassified(&browserTimeoutError{phase: browserTimeoutPhaseQueue})
	}
	defer func() { <-snapshot.sem }()

	for attempt := 0; attempt <= params.Plan.Retries; attempt++ {
		attemptCtx, attemptCancel := context.WithTimeout(totalCtx, params.Timeout)
		result, attemptErr := executeBrowserCDPAttempt(attemptCtx, params, snapshot)
		attemptCancel()
		if attemptErr == nil {
			if result == nil {
				return nil, browserUnavailableClassified()
			}
			return buildBrowserOutput(params, result, attempt)
		}

		var denied *browserHostDeniedError
		if errors.As(attemptErr, &denied) {
			return nil, browserHostDeniedClassified(denied)
		}
		var timeout *browserTimeoutError
		if errors.As(attemptErr, &timeout) {
			return nil, browserTimeoutClassified(timeout)
		}
		if totalCtx.Err() != nil || errors.Is(attemptErr, context.Canceled) || errors.Is(attemptErr, context.DeadlineExceeded) {
			return nil, browserTimeoutClassified(&browserTimeoutError{phase: browserTimeoutPhaseRun})
		}

		if matchesBrowserRetry(attemptErr, params.Plan.RetryOn) {
			if attempt >= params.Plan.Retries {
				return nil, browserResourceClassified()
			}
			if err := waitBrowserBackoff(totalCtx, params.Plan.RetryBackoff); err != nil {
				var timeout *browserTimeoutError
				if errors.As(err, &timeout) {
					return nil, browserTimeoutClassified(timeout)
				}
				return nil, browserTimeoutClassified(&browserTimeoutError{phase: browserTimeoutPhaseRun})
			}
			continue
		}
		var exhausted *browserResourceError
		if errors.As(attemptErr, &exhausted) {
			return nil, browserResourceClassified()
		}
		return nil, browserUnavailableClassified()
	}
	return nil, browserUnavailableClassified()
}

func acquireBrowserSlot(ctx context.Context, snapshot *browserConfigSnapshot) error {
	timer := time.NewTimer(snapshot.config.QueueTimeout)
	defer timer.Stop()
	select {
	case snapshot.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return &browserTimeoutError{phase: browserTimeoutPhaseQueue}
	case <-timer.C:
		return &browserTimeoutError{phase: browserTimeoutPhaseQueue}
	}
}

func waitBrowserBackoff(ctx context.Context, delay time.Duration) error {
	if delay == 0 {
		select {
		case <-ctx.Done():
			return &browserTimeoutError{phase: browserTimeoutPhaseRun}
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return &browserTimeoutError{phase: browserTimeoutPhaseRun}
	case <-timer.C:
		return nil
	}
}

func matchesBrowserRetry(err error, patterns []string) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	for _, pattern := range patterns {
		if pattern != "" && strings.Contains(text, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

func buildBrowserOutput(params *browserParams, result *browserAttemptResult, retries int) (*types.Output, error) {
	headers := make(http.Header, len(result.Headers)+1)
	for name, values := range result.Headers {
		if strings.EqualFold(name, "Cookie") {
			continue
		}
		canonical := http.CanonicalHeaderKey(name)
		headers[canonical] = append([]string(nil), values...)
	}
	cookies, cookieHeader, err := filterBrowserCookies(result.Cookies, params.Harvest.CookieExcludePrefixes)
	if err != nil {
		return nil, browserResourceClassified()
	}
	if cookieHeader != "" {
		headers["Cookie"] = []string{cookieHeader}
	}
	headerCount := len(headers)
	if _, ok := headers["Cookie"]; ok {
		headerCount--
	}
	if len(cookies) == 0 && headerCount == 0 {
		return nil, browserNoCredentialsClassified()
	}

	expireTime := calculateBrowserCredentialExpireTime(browserCDPNow(), cookies, headers, params.TTL)
	outputHeaders := make(map[string][]string, len(headers))
	for name, values := range headers {
		outputHeaders[name] = append([]string(nil), values...)
	}
	return &types.Output{Data: map[string]any{
		"headers":     outputHeaders,
		"expire_time": expireTime,
		"diag": map[string]any{
			"retries":       retries,
			"cookie_count":  len(cookies),
			"header_count":  headerCount,
			"fallback_used": result.FallbackUsed,
		},
	}}, nil
}

func browserInvalidParamsClassified() *types.ClassifiedError {
	return types.NewPermanentError("browser.invalid_params", "browser parameters are invalid")
}
func browserHostDeniedClassified(denied *browserHostDeniedError) *types.ClassifiedError {
	source := browserHostDeniedSourceNavigationPolicy
	rejectedURL := ""
	if denied != nil {
		source = validatedBrowserHostDeniedSource(denied.source)
		rejectedURL = denied.rejectedURL
	}
	rejectedURL = sanitizeBrowserRejectedURL(rejectedURL)
	err := types.NewPermanentError(
		"browser.host_denied",
		fmt.Sprintf("browser destination is denied by policy (source=%s rejected_url=%s)", source, rejectedURL),
	)
	err.Details = map[string]any{
		"source":       source,
		"rejected_url": rejectedURL,
	}
	return err
}

func validatedBrowserHostDeniedSource(source string) string {
	switch source {
	case browserHostDeniedSourceEndpoints, browserHostDeniedSourceNavigationPolicy, browserHostDeniedSourceInterceptedRequest:
		return source
	default:
		return browserHostDeniedSourceNavigationPolicy
	}
}

func sanitizeBrowserRejectedURL(raw string) string {
	if raw == "" || len(raw) > maxBrowserURLBytes {
		return browserRejectedURLInvalid
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return browserRejectedURLInvalid
	}
	return sanitizeBrowserRejectedURLValue(parsed)
}

func sanitizeBrowserRejectedURLValue(value *url.URL) string {
	if value == nil || value.Scheme == "" || value.Host == "" || value.Opaque != "" || value.Hostname() == "" {
		return browserRejectedURLInvalid
	}
	host, err := normalizeHostOnly(value.Hostname())
	if err != nil {
		return browserRejectedURLInvalid
	}
	sanitized := *value
	sanitized.Scheme = strings.ToLower(sanitized.Scheme)
	sanitized.Host = hostForBrowserURL(host)
	sanitized.User = nil
	sanitized.RawQuery = ""
	sanitized.ForceQuery = false
	sanitized.Fragment = ""
	sanitized.RawFragment = ""
	return sanitized.String()
}

func browserUnavailableClassified() *types.ClassifiedError {
	return types.NewPermanentError("browser.unavailable", "remote browser is unavailable")
}
func browserNoCredentialsClassified() *types.ClassifiedError {
	return types.NewPermanentError("browser.no_credentials", "browser navigation produced no permitted credentials")
}
func browserTimeoutClassified(timeout *browserTimeoutError) *types.ClassifiedError {
	phase := browserTimeoutPhaseRun
	if timeout != nil {
		phase = validatedBrowserTimeoutPhase(timeout.phase)
	}
	err := types.NewTransientError(
		"browser.timeout",
		fmt.Sprintf("browser operation timed out (phase=%s)", phase),
	)
	err.Details = map[string]any{"phase": phase}
	return err
}

func validatedBrowserTimeoutPhase(phase string) string {
	switch phase {
	case browserTimeoutPhaseQueue, browserTimeoutPhaseConnect, browserTimeoutPhaseRun:
		return phase
	default:
		return browserTimeoutPhaseRun
	}
}
func browserResourceClassified() *types.ClassifiedError {
	return types.NewTransientError("browser.resource_exhausted", "browser resources are exhausted")
}

type browserHeaderCollector struct {
	mu     sync.Mutex
	header http.Header
	bytes  int
}

func newBrowserHeaderCollector() *browserHeaderCollector {
	return &browserHeaderCollector{header: make(http.Header)}
}

func (c *browserHeaderCollector) capture(event *fetch.EventRequestPaused, rules browserHarvestRules) error {
	if event == nil || event.Request == nil || !browserRequestInHarvestScope(event, rules) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, raw := range event.Request.Headers {
		lower := strings.ToLower(name)
		if lower == "cookie" || strings.HasPrefix(lower, ":") {
			continue
		}
		if _, excluded := rules.HeaderExclude[lower]; excluded {
			continue
		}
		canonical := http.CanonicalHeaderKey(name)
		if canonical == "" || len(canonical) > 256 {
			continue
		}
		values := browserHeaderStrings(raw)
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n\x00") || len(value) > maxBrowserStringBytes {
				return &browserResourceError{}
			}
			if containsString(c.header[canonical], value) {
				continue
			}
			if len(c.header) >= maxBrowserCapturedHeaders && len(c.header[canonical]) == 0 {
				return &browserResourceError{}
			}
			if len(c.header[canonical]) >= maxBrowserHeaderValues || c.bytes+len(canonical)+len(value) > maxBrowserCredentialBytes {
				return &browserResourceError{}
			}
			c.header.Add(canonical, value)
			c.bytes += len(canonical) + len(value)
		}
	}
	return nil
}

func (c *browserHeaderCollector) snapshot() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(http.Header, len(c.header))
	for name, values := range c.header {
		result[name] = append([]string(nil), values...)
	}
	return result
}

func browserHeaderStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case []string:
		return typed
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}

func browserRequestInHarvestScope(event *fetch.EventRequestPaused, rules browserHarvestRules) bool {
	u, err := url.Parse(event.Request.URL)
	if err != nil || !strings.EqualFold(u.Hostname(), rules.RequestHost) {
		return false
	}
	path := strings.ToLower(u.EscapedPath())
	for _, suffix := range rules.SkipSuffixes {
		if suffix != "" && strings.HasSuffix(path, suffix) {
			return false
		}
	}
	if event.ResourceType == network.ResourceTypeXHR || event.ResourceType == network.ResourceTypeFetch {
		return true
	}
	for _, pattern := range rules.APIPathPatterns {
		if pattern != "" && strings.Contains(path, pattern) {
			return true
		}
	}
	return false
}

func realBrowserCDPAttempt(ctx context.Context, params *browserParams, snapshot *browserConfigSnapshot) (*browserAttemptResult, error) {
	debuggingURL, err := resolveBrowserWebSocketEndpoint(ctx, params.DebuggingURL, snapshot)
	if err != nil {
		return nil, err
	}
	allocatorCtx, allocatorCancel := chromedp.NewRemoteAllocator(ctx, debuggingURL.String())
	allocator := chromedp.FromContext(allocatorCtx).Allocator
	browserCtx, browserCancel := chromedp.NewContext(allocatorCtx, chromedp.WithNewBrowserContext())
	runCtx, runCancel := context.WithCancel(browserCtx)
	defer func() {
		runCancel()
		_ = chromedp.Cancel(browserCtx)
		browserCancel()
		allocatorCancel()
		allocator.Wait()
	}()

	paused := make(chan *fetch.EventRequestPaused, browserPausedRequestBufSize)
	fatal := make(chan error, 1)
	reportFatal := func(err error) {
		select {
		case fatal <- err:
		default:
		}
		runCancel()
	}
	collector := newBrowserHeaderCollector()
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		defer func() {
			if recover() != nil {
				reportFatal(&browserUnavailableError{})
			}
		}()
		for {
			select {
			case <-runCtx.Done():
				return
			case event := <-paused:
				if event == nil {
					continue
				}
				if err := validateInterceptedBrowserURL(event.Request, params.Policy); err != nil {
					_ = chromedp.Run(browserCtx, fetch.FailRequest(event.RequestID, network.ErrorReasonBlockedByClient))
					reportFatal(err)
					return
				}
				if err := collector.capture(event, params.Harvest); err != nil {
					_ = chromedp.Run(browserCtx, fetch.FailRequest(event.RequestID, network.ErrorReasonBlockedByClient))
					reportFatal(err)
					return
				}
				if err := chromedp.Run(runCtx, fetch.ContinueRequest(event.RequestID)); err != nil {
					if runCtx.Err() == nil {
						reportFatal(&browserNavigationError{cause: err})
					}
					return
				}
			}
		}
	}()
	defer func() {
		runCancel()
		worker.Wait()
	}()

	chromedp.ListenTarget(runCtx, func(value any) {
		event, ok := value.(*fetch.EventRequestPaused)
		if !ok {
			return
		}
		select {
		case paused <- event:
		case <-runCtx.Done():
		default:
			reportFatal(&browserResourceError{})
		}
	})

	if err := initializeRemoteBrowser(runCtx, runCancel, snapshot.config.ConnectTimeout); err != nil {
		return nil, err
	}
	if err := chromedp.Run(runCtx, fetch.Enable()); err != nil {
		return nil, classifyBrowserRunError(runCtx, fatal, err, browserTimeoutPhaseConnect, true)
	}

	seedActions := make([]chromedp.Action, 0, len(params.SeedCookies))
	for _, cookie := range params.SeedCookies {
		seedActions = append(seedActions, network.SetCookie(cookie.Name, cookie.Value).
			WithURL(cookie.URL).WithPath(cookie.Path).WithSecure(cookie.Secure))
	}
	if len(seedActions) > 0 {
		if err := chromedp.Run(runCtx, seedActions...); err != nil {
			return nil, classifyBrowserRunError(runCtx, fatal, err, browserTimeoutPhaseRun, false)
		}
	}

	if err := runBrowserNavigation(runCtx, params.EntryURL.String(), false, params.Plan.WaitSelector, params.Plan.Settle); err != nil {
		return nil, classifyBrowserRunError(runCtx, fatal, err, browserTimeoutPhaseRun, false)
	}
	cookies, err := getScopedBrowserCookies(runCtx, params.Harvest.CookieURLs)
	if err != nil {
		return nil, classifyBrowserRunError(runCtx, fatal, err, browserTimeoutPhaseRun, false)
	}
	filtered, _, err := filterBrowserCookies(cookies, params.Harvest.CookieExcludePrefixes)
	if err != nil {
		return nil, err
	}
	headers := collector.snapshot()
	fallbackUsed := false
	if len(filtered) == 0 && len(headers) == 0 && params.Plan.FallbackRoot {
		fallbackUsed = true
		if err := runBrowserNavigation(runCtx, params.FallbackURL.String(), params.Plan.FallbackReload, params.Plan.WaitSelector, params.Plan.FallbackSettle); err != nil {
			return nil, classifyBrowserRunError(runCtx, fatal, err, browserTimeoutPhaseRun, false)
		}
		cookies, err = getScopedBrowserCookies(runCtx, params.Harvest.CookieURLs)
		if err != nil {
			return nil, classifyBrowserRunError(runCtx, fatal, err, browserTimeoutPhaseRun, false)
		}
		headers = collector.snapshot()
	}
	select {
	case fatalErr := <-fatal:
		return nil, fatalErr
	default:
	}
	return &browserAttemptResult{Headers: headers, Cookies: cookies, FallbackUsed: fallbackUsed}, nil
}

func initializeRemoteBrowser(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if recover() != nil {
				done <- &browserUnavailableError{}
			}
		}()
		done <- chromedp.Run(ctx)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return &browserTimeoutError{phase: browserTimeoutPhaseConnect}
		}
		return &browserUnavailableError{cause: err}
	case <-ctx.Done():
		cancel()
		return &browserTimeoutError{phase: browserTimeoutPhaseConnect}
	case <-timer.C:
		cancel()
		return &browserTimeoutError{phase: browserTimeoutPhaseConnect}
	}
}

func runBrowserNavigation(ctx context.Context, destination string, reload bool, selector string, settle time.Duration) error {
	actions := []chromedp.Action{chromedp.Navigate(destination), chromedp.WaitReady(selector, chromedp.ByQuery)}
	if reload {
		actions = append(actions, chromedp.Reload(), chromedp.WaitReady(selector, chromedp.ByQuery))
	}
	if settle > 0 {
		actions = append(actions, chromedp.Sleep(settle))
	}
	return chromedp.Run(ctx, actions...)
}

func getScopedBrowserCookies(ctx context.Context, urls []string) ([]*network.Cookie, error) {
	var cookies []*network.Cookie
	err := chromedp.Run(ctx, chromedp.ActionFunc(func(actionCtx context.Context) error {
		var err error
		cookies, err = network.GetCookies().WithURLs(urls).Do(actionCtx)
		return err
	}))
	return cookies, err
}

func classifyBrowserRunError(ctx context.Context, fatal <-chan error, err error, timeoutPhase string, connecting bool) error {
	select {
	case fatalErr := <-fatal:
		return fatalErr
	default:
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &browserTimeoutError{phase: validatedBrowserTimeoutPhase(timeoutPhase)}
	}
	if connecting {
		return &browserUnavailableError{cause: err}
	}
	return &browserNavigationError{cause: err}
}

func validateInterceptedBrowserURL(request *network.Request, policy HostPolicy) error {
	if request == nil || request.URL == "" || len(request.URL) > maxBrowserURLBytes {
		return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, "")
	}
	u, err := url.Parse(request.URL)
	if err != nil {
		return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, request.URL)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "ws", "wss":
		if u.Hostname() == "" {
			return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, request.URL)
		}
		if err := applyBrowserHostPolicy(policy, u.Hostname(), request.URL); err != nil {
			return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, request.URL)
		}
		return nil
	case "about", "data":
		return nil
	case "blob":
		inner, err := url.Parse(u.Opaque)
		if err != nil || inner.Hostname() == "" {
			return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, request.URL)
		}
		if err := applyBrowserHostPolicy(policy, inner.Hostname(), inner.String()); err != nil {
			return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, inner.String())
		}
		return nil
	default:
		return newBrowserHostDeniedError(browserHostDeniedSourceInterceptedRequest, request.URL)
	}
}

func filterBrowserCookies(cookies []*network.Cookie, excludedPrefixes []string) ([]*network.Cookie, string, error) {
	filtered := make([]*network.Cookie, 0, len(cookies))
	total := 0
	for _, cookie := range cookies {
		if cookie == nil || !validCookieName(cookie.Name) || strings.ContainsAny(cookie.Value, ";\r\n\x00") {
			continue
		}
		excluded := false
		for _, prefix := range excludedPrefixes {
			if strings.HasPrefix(cookie.Name, prefix) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		total += len(cookie.Name) + len(cookie.Value) + 2
		if len(filtered) >= maxBrowserListItems || total > maxBrowserCredentialBytes {
			return nil, "", &browserResourceError{}
		}
		filtered = append(filtered, cookie)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].Name != filtered[j].Name {
			return filtered[i].Name < filtered[j].Name
		}
		if filtered[i].Domain != filtered[j].Domain {
			return filtered[i].Domain < filtered[j].Domain
		}
		return filtered[i].Path < filtered[j].Path
	})
	parts := make([]string, 0, len(filtered))
	for _, cookie := range filtered {
		parts = append(parts, cookie.Name+"="+cookie.Value)
	}
	return filtered, strings.Join(parts, "; "), nil
}

func validCookieName(name string) bool {
	if name == "" || len(name) > 256 {
		return false
	}
	for _, r := range name {
		if r <= 0x20 || r >= 0x7f || strings.ContainsRune("()<>@,;:\\\"/[]?={}", r) {
			return false
		}
	}
	return true
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func calculateBrowserCredentialExpireTime(now time.Time, cookies []*network.Cookie, headers http.Header, rules browserTTLRules) int64 {
	ttl := rules.CredentialTTLCap
	for _, cookie := range cookies {
		if cookie == nil || cookie.Session || cookie.Expires <= 0 || math.IsNaN(cookie.Expires) || math.IsInf(cookie.Expires, 0) || cookie.Expires > float64(math.MaxInt64) {
			continue
		}
		seconds, fraction := math.Modf(cookie.Expires)
		expires := time.Unix(int64(seconds), int64(fraction*float64(time.Second)))
		if candidate := expires.Sub(now); candidate > 0 && candidate < ttl {
			ttl = candidate
		}
	}
	for name, values := range headers {
		if !strings.EqualFold(name, "Authorization") {
			continue
		}
		for _, value := range values {
			if expires, ok := bearerJWTExpiry(value); ok {
				if candidate := expires.Sub(now); candidate > 0 && candidate < ttl {
					ttl = candidate
				}
			}
		}
	}
	if ttl <= 0 || ttl > rules.CredentialTTLCap {
		ttl = rules.CredentialTTLCap
	}
	early := time.Duration(float64(ttl) * rules.EarlyExpireRatio)
	if early < rules.EarlyExpireMin {
		early = rules.EarlyExpireMin
	}
	if early >= ttl {
		early = ttl / 2
	}
	return now.Add(ttl - early).Unix()
}

func bearerJWTExpiry(header string) (time.Time, bool) {
	if len(header) > maxBrowserStringBytes || len(header) < 8 || !strings.EqualFold(header[:7], "bearer ") {
		return time.Time{}, false
	}
	parts := strings.Split(strings.TrimSpace(header[7:]), ".")
	if len(parts) != 3 || len(parts[1]) > maxBrowserStringBytes {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(payload) > maxBrowserStringBytes {
		return time.Time{}, false
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	var claims map[string]any
	if err := decoder.Decode(&claims); err != nil {
		return time.Time{}, false
	}
	exp, ok := claims["exp"].(json.Number)
	if !ok {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseFloat(string(exp), 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 || seconds > float64(math.MaxInt64) {
		return time.Time{}, false
	}
	whole, fraction := math.Modf(seconds)
	return time.Unix(int64(whole), int64(fraction*float64(time.Second))), true
}
