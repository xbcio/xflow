// cmd/server is the xflow control-plane server.
//
// Responsibilities:
//   - Accept workflow submissions via HTTP/gRPC API
//   - Compile WorkflowDef into Graph IR
//   - Enqueue node tasks via TaskQueue
//   - Dispatch queued node tasks to runners via Runner Protocol
//   - Track execution lifecycle (status, completion, cancellation)
//   - Deliver signals to suspended nodes
//   - Serve query APIs (execution status, pending approvals)
//   - Run TimeoutSweep for suspended node expiration
//
// It does NOT execute node handlers — that is the runner's job. Redis, Asynq,
// and StateStore access stay on this side of the boundary.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

type serverConfig struct {
	addr        string
	grpcAddr    string
	redis       string
	memory      bool
	concurrency int
	// authPolicy is the path to runners.yaml. Empty means DisabledAuthenticator
	// (dev / MVP behavior).
	authPolicy string
	// authDryRun logs auth violations but lets the request proceed. Meant for
	// the rollout window between adding runners.yaml and enforcing it.
	authDryRun bool
	// TLS: server cert/key enable TLS on the HTTP + gRPC listeners; presence
	// of tlsClientCA additionally requires a client cert (mTLS).
	tlsCert     string
	tlsKey      string
	tlsClientCA string
}

func main() {
	cfg, err := parseServerConfig(nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := runServer(cfg); err != nil {
		log.Fatal(err)
	}
}

func parseServerConfig(args []string) (serverConfig, error) {
	fs := flag.NewFlagSet("xflow-server", flag.ContinueOnError)
	cfg := serverConfig{addr: ":8080", concurrency: 10}
	fs.StringVar(&cfg.addr, "addr", cfg.addr, "HTTP listen address")
	fs.StringVar(&cfg.grpcAddr, "grpc-addr", "", "gRPC Runner Protocol listen address (empty disables gRPC)")
	fs.StringVar(&cfg.redis, "redis", "", "Redis address for Asynq backend")
	fs.BoolVar(&cfg.memory, "memory", false, "Use in-memory backend")
	fs.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Queue consumer concurrency")
	fs.StringVar(&cfg.authPolicy, "auth-policy", "", "Path to runners.yaml (empty = auth disabled)")
	fs.BoolVar(&cfg.authDryRun, "auth-dry-run", false, "Log auth violations but let requests through (rollout aid)")
	fs.StringVar(&cfg.tlsCert, "tls-cert", "", "Path to server TLS certificate (enables TLS)")
	fs.StringVar(&cfg.tlsKey, "tls-key", "", "Path to server TLS private key (required with --tls-cert)")
	fs.StringVar(&cfg.tlsClientCA, "tls-client-ca", "", "Path to CA bundle to verify runner certs (enables mTLS)")
	if args == nil {
		args = os.Args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if cfg.redis == "" {
		cfg.memory = true
	}
	return cfg, nil
}

func runServer(cfg serverConfig) error {
	runners := control.NewRunnerPool()

	auth, err := buildAuthenticator(cfg)
	if err != nil {
		return err
	}

	tlsCfg, err := buildTLSConfig(cfg)
	if err != nil {
		return err
	}

	var eng *engine.Engine
	var stop func()
	if cfg.memory {
		backend := memory.New(memory.WithConcurrency(cfg.concurrency))
		eng = engine.New(backend.State(), backend.Queue())
		dispatcher := control.NewDispatcher(eng, runners)
		stop = backend.BindHandler(dispatcher.HandleTask)
	} else {
		backend, err := asynq.New(cfg.redis, nil, asynq.WithConcurrency(cfg.concurrency))
		if err != nil {
			return err
		}
		eng = engine.New(backend.State(), backend.Queue())
		dispatcher := control.NewDispatcher(eng, runners)
		stop = backend.BindHandler(eng, dispatcher.HandleTask)
	}
	defer stop()

	if cfg.grpcAddr != "" {
		grpcStop, err := serveGRPC(cfg.grpcAddr, eng, runners, auth, tlsCfg)
		if err != nil {
			return err
		}
		defer grpcStop()
	}

	handler := control.NewServer(eng, runners, control.WithAuthenticator(auth)).Handler()
	if tlsCfg == nil {
		return http.ListenAndServe(cfg.addr, handler)
	}
	srv := &http.Server{Addr: cfg.addr, Handler: handler, TLSConfig: tlsCfg}
	log.Printf("xflow-server: HTTPS listening on %s (mTLS=%v)", cfg.addr, tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert)
	return srv.ListenAndServeTLS("", "")
}

// buildTLSConfig resolves the server TLS config from CLI flags. Returns nil
// when no cert was configured (plaintext, dev default).
func buildTLSConfig(cfg serverConfig) (*tls.Config, error) {
	switch {
	case cfg.tlsCert == "" && cfg.tlsKey == "" && cfg.tlsClientCA == "":
		return nil, nil
	case cfg.tlsCert == "" || cfg.tlsKey == "":
		return nil, fmt.Errorf("--tls-cert and --tls-key must be provided together")
	}
	cert, err := tls.LoadX509KeyPair(cfg.tlsCert, cfg.tlsKey)
	if err != nil {
		return nil, fmt.Errorf("load tls keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if cfg.tlsClientCA != "" {
		caPEM, err := os.ReadFile(cfg.tlsClientCA)
		if err != nil {
			return nil, fmt.Errorf("read tls client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls client CA %q contains no valid certs", cfg.tlsClientCA)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, nil
}

// buildAuthenticator resolves the runner-protocol authenticator from CLI
// flags. Empty --auth-policy falls back to the permissive dev default.
func buildAuthenticator(cfg serverConfig) (control.Authenticator, error) {
	if cfg.authPolicy == "" {
		if cfg.authDryRun {
			log.Println("xflow-server: --auth-dry-run has no effect without --auth-policy; auth remains disabled")
		}
		return control.DisabledAuthenticator{}, nil
	}
	store, err := control.NewFilePolicyStore(cfg.authPolicy, cfg.authDryRun)
	if err != nil {
		return nil, err
	}
	mode := "enforcing"
	if cfg.authDryRun {
		mode = "dry-run"
	}
	log.Printf("xflow-server: runner auth policy loaded from %q (%s)", cfg.authPolicy, mode)
	return store, nil
}

// serveGRPC starts the gRPC Runner Protocol server on its own listener, sharing
// the engine and runner pool with the HTTP server. It returns a stop function
// that gracefully drains the gRPC server.
func serveGRPC(addr string, eng *engine.Engine, runners *control.RunnerPool, auth control.Authenticator, tlsCfg *tls.Config) (func(), error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		log.Printf("xflow-server: gRPC listening on %s (mTLS=%v)", addr, tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert)
	}
	grpcServer := grpc.NewServer(opts...)
	runnerpb.RegisterRunnerProtocolServer(grpcServer, control.NewGRPCServer(eng, runners, control.WithGRPCAuthenticator(auth)))
	go func() {
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Printf("gRPC server stopped: %v", serveErr)
		}
	}()
	return grpcServer.GracefulStop, nil
}
