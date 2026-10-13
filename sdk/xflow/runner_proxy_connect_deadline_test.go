package xflow

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The CONNECT reply is bounded by the dialer's own timeout: a proxy that
// accepts the connection and then goes silent must cost one timeout, not one
// stuck dial, while the tunnel the reply hands back must not inherit that
// bound. These two tests pin each half.

// TestRunnerProxyHungConnectReplyIsCutOff pins the read deadline on the CONNECT
// response: a proxy that accepts the connection and then writes nothing must
// not hold the dial open until the caller's request context dies. The dialer's
// own timeout is the bound, so the failure is a timeout on the CONNECT reply —
// the shape asserted below — and not an indefinite hang.
func TestRunnerProxyHungConnectReplyIsCutOff(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	defer func() {
		if conn := <-accepted; conn != nil {
			_ = conn.Close()
		}
	}()

	proxyURL := &url.URL{Scheme: "http", Host: listener.Addr().String()}
	dialer := &net.Dialer{Timeout: 150 * time.Millisecond}
	start := time.Now()
	_, dialErr := dialConnectTunnel(context.Background(), dialer, "tcp", "control-plane.invalid:8443", proxyURL)
	elapsed := time.Since(start)

	if dialErr == nil {
		t.Fatal("dial through a proxy that never answers CONNECT succeeded")
	}
	if !strings.Contains(dialErr.Error(), "read CONNECT response") {
		t.Fatalf("error %q does not read as a failed CONNECT reply", dialErr)
	}
	if !strings.Contains(dialErr.Error(), "timeout") {
		t.Fatalf("error %q is not a timeout; the silent proxy should hit the read deadline", dialErr)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("the silent proxy held the dial for %v; the CONNECT read must be bounded by the dialer timeout", elapsed)
	}
}

// TestRunnerProxyConnectDeadlineIsClearedForTheTunnel pins the other half: the
// CONNECT read's bound must be cleared before the tunnel is handed back,
// because the caller keeps the connection for the whole request lifetime. The
// proxy answers CONNECT immediately and only then goes quiet for longer than
// the read deadline; the tunnel's first bytes must still arrive.
func TestRunnerProxyConnectDeadlineIsClearedForTheTunnel(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	testDone := make(chan struct{})
	defer close(testDone)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.TrimSpace(line) == "" {
				break
			}
		}
		if _, err := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
			return
		}
		// Far longer than the dialer timeout the CONNECT read was bounded by.
		time.Sleep(400 * time.Millisecond)
		_, _ = conn.Write([]byte("tunnel-bytes"))
		<-testDone
	}()

	proxyURL := &url.URL{Scheme: "http", Host: listener.Addr().String()}
	dialer := &net.Dialer{Timeout: 100 * time.Millisecond}
	conn, err := dialConnectTunnel(context.Background(), dialer, "tcp", "control-plane.invalid:8443", proxyURL)
	if err != nil {
		t.Fatalf("dialConnectTunnel: %v", err)
	}
	defer conn.Close()

	buf := make([]byte, len("tunnel-bytes"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("reading the tunnel after the CONNECT reply: %v "+
			"(the CONNECT read's deadline must be cleared before the tunnel is handed back)", err)
	}
	if string(buf) != "tunnel-bytes" {
		t.Fatalf("tunnel bytes = %q, want %q", buf, "tunnel-bytes")
	}
}
