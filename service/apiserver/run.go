package apiserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// HTTPTimeouts bounds the HTTP server's resource hold per request and the
// graceful-shutdown window. ReadHeaderTimeout defends against Slowloris-style
// slow-header attacks; the full Read/Write/Idle bounds cap a single request
// so a misbehaving client cannot exhaust server goroutines.
type HTTPTimeouts struct {
	ReadHeader time.Duration
	Read       time.Duration
	Write      time.Duration
	Idle       time.Duration
	Shutdown   time.Duration
}

func defaultHTTPTimeouts() HTTPTimeouts {
	return HTTPTimeouts{
		ReadHeader: 10 * time.Second,
		Read:       30 * time.Second,
		Write:      30 * time.Second,
		Idle:       120 * time.Second,
		Shutdown:   15 * time.Second,
	}
}

// WithHTTPTimeouts overrides the HTTP server's timeouts. Defaults are sane for
// the control-plane workload; only override if a deployment needs different
// bounds.
func WithHTTPTimeouts(t HTTPTimeouts) Option {
	return func(s *APIServer) { s.timeouts = t }
}

// loadTLS resolves the server TLS config from cfg.TLS. Returns (nil, nil) when
// no cert was configured (plaintext, dev default). When a client CA is supplied
// without a server cert, the server still requires clients to present a cert
// signed by that CA — but a server cert is required to terminate TLS, so this
// combination is rejected.
func (s *APIServer) loadTLS() (*tls.Config, error) {
	t := s.cfg.TLS
	if t == nil {
		return nil, nil
	}
	switch {
	case t.Cert == "" && t.Key == "" && t.ClientCA == "":
		return nil, nil
	case t.Cert == "" || t.Key == "":
		return nil, fmt.Errorf("--tls-cert and --tls-key must be provided together")
	}
	cert, err := tls.LoadX509KeyPair(t.Cert, t.Key)
	if err != nil {
		return nil, fmt.Errorf("load tls keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if t.ClientCA != "" {
		caPEM, err := os.ReadFile(t.ClientCA)
		if err != nil {
			return nil, fmt.Errorf("read tls client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls client CA %q contains no valid certs", t.ClientCA)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, nil
}

// Run starts the APIServer's transports (gRPC, metrics, HTTP) and blocks until
// ctx is cancelled or a listener fails. On exit it drains in-flight requests
// in the order: HTTP server, gRPC server, metrics server, then the underlying
// control plane. Each step runs even if an earlier step errored.
func (s *APIServer) Run(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return err
	}

	tlsCfg, err := s.loadTLS()
	if err != nil {
		_ = s.Shutdown(ctx)
		return err
	}

	// gRPC Runner Protocol listener.
	var grpcServer *grpc.Server
	if s.cfg.GRPCAddr != "" {
		lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
		if err != nil {
			_ = s.Shutdown(ctx)
			return fmt.Errorf("grpc listen: %w", err)
		}
		var opts []grpc.ServerOption
		if tlsCfg != nil {
			opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
			log.Printf("apiserver: gRPC listening on %s (mTLS=%v)", s.cfg.GRPCAddr, tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert)
		} else {
			log.Printf("apiserver: gRPC listening on %s", s.cfg.GRPCAddr)
		}
		grpcServer = grpc.NewServer(opts...)
		s.RegisterGRPC(grpcServer)
		go func() {
			if serveErr := grpcServer.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
				log.Printf("apiserver: gRPC server stopped: %v", serveErr)
			}
		}()
	}

	// Metrics listener. Independent from the main HTTP server so scraping
	// traffic does not compete with API traffic for a slot in the same mux.
	var metricsStop func()
	if s.cfg.MetricsAddr != "" {
		if s.cfg.Metrics == nil {
			log.Printf("apiserver: metrics-addr %q set but Metrics is nil; skipping metrics server", s.cfg.MetricsAddr)
		} else {
			stop, err := s.serveMetrics()
			if err != nil {
				if grpcServer != nil {
					grpcServer.GracefulStop()
				}
				_ = s.Shutdown(ctx)
				return fmt.Errorf("metrics listen: %w", err)
			}
			metricsStop = stop
		}
	}

	// Main HTTP server.
	httpSrv := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           s.Handler(),
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: s.timeouts.ReadHeader,
		ReadTimeout:       s.timeouts.Read,
		WriteTimeout:      s.timeouts.Write,
		IdleTimeout:       s.timeouts.Idle,
	}

	listenErr := make(chan error, 1)
	go func() {
		if tlsCfg == nil {
			log.Printf("apiserver: HTTP listening on %s", s.cfg.HTTPAddr)
			listenErr <- httpSrv.ListenAndServe()
		} else {
			log.Printf("apiserver: HTTPS listening on %s (mTLS=%v)", s.cfg.HTTPAddr, tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert)
			listenErr <- httpSrv.ListenAndServeTLS("", "")
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("apiserver: shutdown signal received, draining...")
	case err := <-listenErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			if grpcServer != nil {
				grpcServer.GracefulStop()
			}
			if metricsStop != nil {
				metricsStop()
			}
			earlyShutdownCtx, earlyCancel := context.WithTimeout(context.Background(), s.timeouts.Shutdown)
			_ = s.Shutdown(earlyShutdownCtx)
			earlyCancel()
			return err
		}
	}

	// Graceful shutdown: each step runs regardless of earlier errors.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.timeouts.Shutdown)
	defer cancel()

	var errs []error
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("apiserver: http shutdown error: %v", err)
		errs = append(errs, err)
	}
	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
	if metricsStop != nil {
		metricsStop()
	}
	if err := s.Shutdown(shutdownCtx); err != nil {
		log.Printf("apiserver: control plane shutdown error: %v", err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// serveMetrics starts an independent HTTP server that exposes the Prometheus
// scrape endpoint. The returned func stops the server.
func (s *APIServer) serveMetrics() (func(), error) {
	path := s.cfg.MetricsPath
	if path == "" {
		path = "/metrics"
	}
	mux := http.NewServeMux()
	mux.Handle(path, s.cfg.Metrics.Handler())
	srv := &http.Server{
		Addr:              s.cfg.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: s.timeouts.ReadHeader,
	}
	ln, err := net.Listen("tcp", s.cfg.MetricsAddr)
	if err != nil {
		return nil, err
	}
	log.Printf("apiserver: metrics listening on %s", ln.Addr().String())
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("apiserver: metrics server stopped: %v", serveErr)
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
