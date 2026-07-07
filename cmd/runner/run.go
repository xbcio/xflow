package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	_ "github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	transportHTTP = "http"
	transportGRPC = "grpc"
)

var (
	reconnectMinBackoff = 2 * time.Second
	reconnectMaxBackoff = 30 * time.Second
)

type runFunc func(ctx context.Context) error

var errStop = errors.New("stop reconnect loop")

type runnerConfig struct {
	configPath        string
	serverURL         string
	transport         string
	grpcTarget        string
	runnerID          string
	concurrency       int
	changed           map[string]bool
	resolutionIssues  map[string]error
	capRaw            string
	capabilities      []protocol.Capability
	labelRaw          []string
	labels            map[string]string
	heartbeatInterval string
	pollWait          string
	// token is the runner's bearer token (matched against the server's
	// runners.yaml policy). Empty means "no auth", which the server accepts
	// only when running with --auth-mode disabled or dry-run.
	token string
	// TLS: serverCA verifies the server's certificate; clientCert/clientKey
	// present a client cert for mTLS. All three empty means plaintext.
	tlsServerCA   string
	tlsClientCert string
	tlsClientKey  string
}

func newRunCommand(opts commandOptions, cfg *runnerConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the xflow task runner",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			recordChangedFlags(cmd, cfg)
			resolved, err := resolveRunnerConfig(*cfg)
			if err != nil {
				return err
			}
			return opts.runFunc(resolved)
		},
	}
	bindRunnerFlags(cmd, cfg)
	return cmd
}

func bindRunnerFlags(cmd *cobra.Command, cfg *runnerConfig) {
	cmd.Flags().StringVar(&cfg.serverURL, "server", cfg.serverURL, "xflow-server base URL (http transport)")
	cmd.Flags().StringVar(&cfg.transport, "transport", cfg.transport, "Runner Protocol transport: http or grpc")
	cmd.Flags().StringVar(&cfg.grpcTarget, "grpc-target", cfg.grpcTarget, "xflow-server gRPC target host:port (grpc transport)")
	cmd.Flags().StringVar(&cfg.runnerID, "id", cfg.runnerID, "Runner ID")
	cmd.Flags().IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Runner concurrency")
	cmd.Flags().StringVar(&cfg.capRaw, "cap", cfg.capRaw, "Comma-separated node type capabilities")
	cmd.Flags().StringArrayVar(&cfg.labelRaw, "label", cfg.labelRaw, "Runner label as key=value; repeatable")
	cmd.Flags().StringVar(&cfg.heartbeatInterval, "heartbeat-interval", cfg.heartbeatInterval, "Heartbeat interval")
	cmd.Flags().StringVar(&cfg.pollWait, "poll-wait", cfg.pollWait, "Poll wait duration when no task is available")
	cmd.Flags().StringVar(&cfg.token, "token", cfg.token, "Runner bearer token (prefer XFLOW_RUNNER_TOKEN env)")
	cmd.Flags().StringVar(&cfg.tlsServerCA, "tls-server-ca", cfg.tlsServerCA, "Path to server CA bundle (enables TLS)")
	cmd.Flags().StringVar(&cfg.tlsClientCert, "tls-client-cert", cfg.tlsClientCert, "Path to client TLS certificate (enables mTLS)")
	cmd.Flags().StringVar(&cfg.tlsClientKey, "tls-client-key", cfg.tlsClientKey, "Path to client TLS private key")
}

func recordChangedFlags(cmd *cobra.Command, cfg *runnerConfig) {
	if cfg.changed == nil {
		cfg.changed = map[string]bool{}
	}
	cmd.Flags().Visit(func(f *pflag.Flag) {
		cfg.changed[f.Name] = true
	})
}

func parseCapabilities(raw string) []protocol.Capability {
	parts := strings.Split(raw, ",")
	capabilities := make([]protocol.Capability, 0, len(parts))
	for _, part := range parts {
		nodeType := strings.TrimSpace(part)
		if nodeType == "" {
			continue
		}
		capabilities = append(capabilities, protocol.Capability{NodeType: nodeType})
	}
	return capabilities
}

type runnerService interface {
	Run(context.Context) error
}

var newRunnerService = func(client runnersvc.ProtocolClient, registry engine.HandlerRegistry, cfg runnersvc.Config) runnerService {
	return runnersvc.New(client, registry, cfg)
}

func runRunner(ctx context.Context, cfg runnerConfig) error {
	serviceCfg, err := runnerServiceConfig(cfg)
	if err != nil {
		return err
	}
	client, cleanup, err := newProtocolClient(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	registry := execution.NewRegistry()
	runner := newRunnerService(client, registry, serviceCfg)
	return runner.Run(ctx)
}

// newProtocolClient builds the Runner Protocol client for the configured
// transport. The returned cleanup releases any transport-owned resources (e.g.
// the gRPC connection); it is a no-op for HTTP.
func newProtocolClient(cfg runnerConfig) (runnersvc.ProtocolClient, func(), error) {
	tlsCfg, err := buildRunnerTLSConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	switch cfg.transport {
	case transportGRPC:
		var creds credentials.TransportCredentials
		if tlsCfg != nil {
			creds = credentials.NewTLS(tlsCfg)
		} else {
			creds = insecure.NewCredentials()
		}
		conn, err := grpc.NewClient(cfg.grpcTarget, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, nil, fmt.Errorf("dial gRPC server %q: %w", cfg.grpcTarget, err)
		}
		client := protocol.NewGRPCClient(conn)
		if cfg.token != "" {
			return client.WithToken(cfg.token), func() { _ = conn.Close() }, nil
		}
		return client, func() { _ = conn.Close() }, nil
	default:
		httpClient := http.DefaultClient
		if tlsCfg != nil {
			httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
		}
		client := protocol.NewClient(cfg.serverURL, httpClient)
		if cfg.token != "" {
			return client.WithToken(cfg.token), func() {}, nil
		}
		return client, func() {}, nil
	}
}

// buildRunnerTLSConfig resolves the runner-side TLS config from CLI flags.
// Returns nil when no CA or cert was configured (plaintext, dev default).
func buildRunnerTLSConfig(cfg runnerConfig) (*tls.Config, error) {
	if cfg.tlsServerCA == "" && cfg.tlsClientCert == "" && cfg.tlsClientKey == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.tlsServerCA != "" {
		caPEM, err := os.ReadFile(cfg.tlsServerCA)
		if err != nil {
			return nil, fmt.Errorf("read server CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("server CA %q contains no valid certs", cfg.tlsServerCA)
		}
		tlsCfg.RootCAs = pool
	}
	switch {
	case cfg.tlsClientCert == "" && cfg.tlsClientKey == "":
		// TLS only, no client auth.
	case cfg.tlsClientCert != "" && cfg.tlsClientKey != "":
		cert, err := tls.LoadX509KeyPair(cfg.tlsClientCert, cfg.tlsClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	default:
		return nil, fmt.Errorf("--tls-client-cert and --tls-client-key must be provided together")
	}
	return tlsCfg, nil
}

func runnerServiceConfig(cfg runnerConfig) (runnersvc.Config, error) {
	_, err := parsePositiveDuration("heartbeat interval", cfg.heartbeatInterval)
	if err != nil {
		return runnersvc.Config{}, err
	}
	pollWait, err := parsePositiveDuration("poll wait", cfg.pollWait)
	if err != nil {
		return runnersvc.Config{}, err
	}
	return runnersvc.Config{
		RunnerID:     cfg.runnerID,
		Concurrency:  cfg.concurrency,
		Labels:       cloneStringMap(cfg.labels),
		Capabilities: cfg.capabilities,
		PollWait:     pollWait,
	}, nil
}

func runWithSignals(cfg runnerConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.runnerID == "" {
		cfg.runnerID = fmt.Sprintf("runner-%d", os.Getpid())
	}
	return runWithReconnect(ctx, func(ctx context.Context) error {
		return runRunner(ctx, cfg)
	})
}

func runWithReconnect(ctx context.Context, fn runFunc) error {
	backoff := reconnectMinBackoff
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, errStop) {
			return err
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
	}
}
