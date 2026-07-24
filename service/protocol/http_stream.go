package protocol

import (
	"context"
	"time"
)

// httpStream simulates the bidi FrameStream over legacy HTTP long-poll endpoints,
// letting the runner main loop stay transport-agnostic. Performance degrades to
// polling, matching the HTTP-as-debug-entrypoint role.
type httpStream struct {
	client    *Client
	ctx       context.Context
	recvCh    chan ServerFrame
	sendCh    chan RunnerFrame
	stop      context.CancelFunc
	sessionID string
	authToken string
}

// Connect returns an *httpStream that implements FrameStream using the HTTP
// long-poll endpoints (Register / Poll / ReportResult) to simulate a bidi
// stream. The caller must send a HELLO frame first, then can Recv WELCOME +
// TASK frames and Send RESULT frames.
func (c *Client) Connect(ctx context.Context) (FrameStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &httpStream{
		client: c,
		ctx:    ctx,
		recvCh: make(chan ServerFrame, 16),
		sendCh: make(chan RunnerFrame, 16),
		stop:   cancel,
	}
	go s.run()
	return s, nil
}

func (s *httpStream) run() {
	defer close(s.recvCh)

	// First frame from the runner must be HELLO.
	var hello *HelloFrame
	select {
	case fr := <-s.sendCh:
		if fr.Hello == nil {
			s.recvCh <- ServerFrame{Ack: &AckFrame{Accepted: false, Error: "first frame must be HELLO"}}
			return
		}
		hello = fr.Hello
	case <-s.ctx.Done():
		return
	}

	// Register with the control plane.
	s.authToken = s.client.token
	registerResp, err := s.client.Register(s.ctx, RegisterRunnerRequest{
		RunnerID:     hello.RunnerID,
		Concurrency:  hello.Concurrency,
		Capabilities: hello.Capabilities,
		Labels:       hello.Labels,
		Tenants:      hello.Tenants,
		AuthToken:    s.authToken,
	})
	if err != nil {
		s.recvCh <- ServerFrame{Ack: &AckFrame{Accepted: false, Error: err.Error()}}
		return
	}
	s.sessionID = registerResp.SessionID

	// Emit WELCOME.
	s.recvCh <- ServerFrame{Welcome: &WelcomeFrame{RunnerID: hello.RunnerID, ServerTime: time.Now().Unix()}}

	// Goroutine: drain RESULT frames from the runner and send ACKs back.
	resultDone := make(chan struct{})
	go func() {
		defer close(resultDone)
		for {
			select {
			case fr := <-s.sendCh:
				if fr.Result != nil {
					resp, err := s.client.ReportResult(s.ctx, ReportResultRequest{
						RunnerID:  hello.RunnerID,
						SessionID: s.sessionID,
						Lease:     fr.Result.Lease,
						Result:    fr.Result.Result,
						AuthToken: s.authToken,
					})
					if err != nil {
						s.recvCh <- ServerFrame{Ack: &AckFrame{LeaseID: fr.Result.LeaseID, Accepted: false, Error: err.Error()}}
						continue
					}
					s.recvCh <- ServerFrame{Ack: &AckFrame{LeaseID: fr.Result.LeaseID, Accepted: resp.Accepted, Error: resp.Error}}
				}
				if fr.Bye != nil {
					return
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()

	// Polling loop: continuously poll for tasks and emit TASK frames.
	pollWait := time.Second
	for {
		select {
		case <-s.ctx.Done():
			<-resultDone
			return
		default:
		}

		resp, err := s.client.Poll(s.ctx, PollTaskRequest{
			RunnerID:     hello.RunnerID,
			SessionID:    s.sessionID,
			Capacity:     hello.Concurrency,
			Capabilities: hello.Capabilities,
			Labels:       hello.Labels,
			AuthToken:    s.authToken,
		})
		if err != nil {
			<-resultDone
			return
		}
		if resp.Lease != nil {
			select {
			case s.recvCh <- ServerFrame{Task: &TaskFrame{Lease: resp.Lease}}:
			case <-s.ctx.Done():
				<-resultDone
				return
			}
			continue
		}
		if resp.Wait > 0 {
			pollWait = resp.Wait
		}
		select {
		case <-time.After(pollWait):
		case <-s.ctx.Done():
			<-resultDone
			return
		}
	}
}

// Send enqueues a frame to the stream. Returns ctx.Err() if the stream is closed.
func (s *httpStream) Send(fr RunnerFrame) error {
	select {
	case s.sendCh <- fr:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Recv blocks until a frame is available. Returns context.Canceled when the
// stream is closed (matching grpcFrameStream behavior on stream end).
func (s *httpStream) Recv() (ServerFrame, error) {
	fr, ok := <-s.recvCh
	if !ok {
		return ServerFrame{}, context.Canceled
	}
	return fr, nil
}

// Close stops the internal goroutines. Idempotent: context.CancelFunc is safe
// to call multiple times.
func (s *httpStream) Close() error { s.stop(); return nil }
