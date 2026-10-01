package apiserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/control"

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

// loadTLS resolves the server TLS config from cfg.TLS. Returns (nil, nil, nil)
// when no cert was configured (plaintext, dev default). When a client CA is
// supplied without a server cert, the server still requires clients to present
// a cert signed by that CA — but a server cert is required to terminate TLS,
// so this combination is rejected.
//
// The returned *tls.Config reads certificate and (for mTLS) client-CA material
// through the returned *TLSReloader on every handshake via GetCertificate and
// GetConfigForClient, rather than the static Certificates/ClientCAs fields —
// this is what lets TLSReloader.Reload take effect without restarting the
// listener. MinVersion and the other static settings are unaffected by a
// reload; only the reloader's own fields ever change.
func (s *APIServer) loadTLS() (*tls.Config, *TLSReloader, error) {
	t := s.cfg.TLS
	if t == nil {
		return nil, nil, nil
	}
	switch {
	case t.Cert == "" && t.Key == "" && t.ClientCA == "":
		return nil, nil, nil
	case t.Cert == "" || t.Key == "":
		return nil, nil, fmt.Errorf("--tls-cert and --tls-key must be provided together")
	}
	reloader, err := newTLSReloader(t.Cert, t.Key, t.ClientCA)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// NextProtos must be set here, on the base config GetConfigForClient
		// closes over, rather than left for net/http or grpc-go to inject: both
		// http.Server.ServeTLS and grpc/credentials.NewTLS only add "h2" (and,
		// for net/http, "http/1.1") to a *clone* of this config made at listener
		// construction time, not to this pointer itself. Once GetConfigForClient
		// is non-nil, crypto/tls replaces the live connection's config wholesale
		// with whatever it returns (see tls.Conn.readClientHello) — so a clone
		// derived from this pointer without NextProtos already set would negotiate
		// no ALPN protocol at all, breaking HTTP/2 and gRPC (which requires "h2")
		// silently. Listing both here keeps plain HTTP/1.1 clients working too.
		NextProtos: []string{"h2", "http/1.1"},
		// GetCertificate/GetConfigForClient always read the reloader's live
		// snapshot; Certificates/ClientCAs are deliberately left unset so a
		// stale copy can never win a handshake decided by them instead.
		GetCertificate: reloader.GetCertificate,
	}
	tlsCfg.GetConfigForClient = reloader.GetConfigForClient(tlsCfg)
	if reloader.clientAuthType() == tls.RequireAndVerifyClientCert {
		// ClientAuth on the base config only affects the initial handshake
		// negotiation before GetConfigForClient's returned config takes over;
		// setting it here keeps tlsCfg.ClientAuth truthful for any caller (e.g.
		// the mTLS log line below) that reads it directly rather than through a
		// live handshake.
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, reloader, nil
}

// Run starts the APIServer's transports (gRPC, metrics, HTTP) and blocks until
// ctx is cancelled or a listener fails. On exit it first stops advertising
// readiness, then drains in-flight requests in the order: HTTP server, gRPC
// server, metrics server, and finally the underlying control plane. Each step
// runs even if an earlier step errored.
func (s *APIServer) Run(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return err
	}

	tlsCfg, tlsReloader, err := s.loadTLS()
	if err != nil {
		_ = s.Shutdown(ctx)
		return err
	}
	s.tlsReloader.Store(tlsReloader)

	// gRPC Runner Protocol listener.
	var grpcServer *grpc.Server
	if s.cfg.GRPCAddr != "" {
		lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
		if err != nil {
			_ = s.Shutdown(ctx)
			return fmt.Errorf("grpc listen: %w", err)
		}
		var opts []grpc.ServerOption
		// Receive limit matching the HTTP register cap; server-wide, see
		// control.RunnerGRPCServerOptions.
		opts = append(opts, control.RunnerGRPCServerOptions()...)
		if tlsCfg != nil {
			opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
			log.Printf("apiserver: gRPC listening on %s (mTLS=%v)", s.cfg.GRPCAddr, tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert)
		} else {
			log.Printf("apiserver: gRPC listening on %s", s.cfg.GRPCAddr)
		}
		// B1: extract W3C tracecontext from gRPC metadata and start a server
		// span for each unary AND stream RPC, mirroring the HTTP tracing
		// middleware so the runner-protocol RPCs (Register/Heartbeat/PollTask/
		// ReportResult and the Connect bidi stream) inherit a remote parent and
		// the dispatch/commit spans they start are not root spans. Pass-through
		// when no Tracer is configured.
		if s.cfg.Tracer != nil {
			opts = append(opts,
				grpc.UnaryInterceptor(tracing.GRPCUnaryServerInterceptor(s.cfg.Tracer)),
				grpc.StreamInterceptor(tracing.GRPCStreamServerInterceptor(s.cfg.Tracer)),
			)
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

	// Stop advertising readiness before the HTTP listener begins draining, but
	// keep the control plane alive until all transport drains have completed.
	// Calling s.Shutdown here would tear down the control plane too early.
	if s.readiness != nil {
		s.readiness.markShuttingDown()
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
	// Merge the proxied runner metrics into this server's own scrape endpoint so
	// Prometheus keeps scraping exactly one target with no configuration change.
	// The inbox intercepts help/type conflicts itself; if it ever returned a
	// conflicting family, promhttp's default error handling would 500 the WHOLE
	// endpoint, taking the server's own metrics down with it.
	if inbox := metricsInboxFor(s.cp); inbox != nil {
		mux.Handle(path, promhttp.HandlerFor(
			prometheus.Gatherers{s.cfg.Metrics.Registry(), inbox},
			promhttp.HandlerOpts{},
		))
	} else {
		mux.Handle(path, s.cfg.Metrics.Handler())
	}
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
