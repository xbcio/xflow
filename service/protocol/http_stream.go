package protocol

import (
	"context"
	"sync"
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

	controlMu    sync.RWMutex
	recoveryOnly bool
}

// Connect returns an *httpStream that implements FrameStream using the HTTP
// long-poll endpoints (Register / Poll / ReportResult) to simulate a bidi
// stream. The caller must send a HELLO frame first, then can Recv WELCOME,
// CONTROL, and TASK frames and Send RESULT or CONTROL_OBSERVATION frames.
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
	defer s.stop()
	defer close(s.recvCh)

	// First frame from the runner must be HELLO.
	var hello *HelloFrame
	select {
	case fr := <-s.sendCh:
		if fr.Hello == nil {
			s.emit(ServerFrame{Ack: &AckFrame{Accepted: false, Error: "first frame must be HELLO"}})
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
		Namespaces:   hello.Namespaces,
		AuthToken:    s.authToken,
	})
	if err != nil {
		s.emit(ServerFrame{Ack: &AckFrame{Accepted: false, Error: err.Error()}})
		return
	}
	s.sessionID = registerResp.SessionID

	// Emit WELCOME with the initial control projection. A nil Control preserves
	// compatibility with older HTTP control planes that do not send it.
	if !s.emit(ServerFrame{Welcome: &WelcomeFrame{
		RunnerID:   hello.RunnerID,
		ServerTime: time.Now().Unix(),
		Control:    registerResp.Control,
	}}) {
		return
	}

	// Goroutine: drain RESULT and CONTROL_OBSERVATION frames from the runner.
	// Observation forwarding uses the existing heartbeat transport so the HTTP
	// emulation preserves the same server-side control call chain as unary HTTP.
	resultDone := make(chan struct{})
	go s.consumeRunnerFrames(hello, resultDone)

	// Polling loop: continuously poll for tasks and emit CONTROL/TASK frames.
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
			RecoveryOnly: s.isRecoveryOnly(),
		})
		if err != nil {
			s.stop()
			<-resultDone
			return
		}
		if !s.emitControl(resp.Control) {
			<-resultDone
			return
		}
		if resp.Lease != nil {
			if !s.emit(ServerFrame{Task: &TaskFrame{Lease: resp.Lease}}) {
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

func (s *httpStream) consumeRunnerFrames(hello *HelloFrame, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case fr := <-s.sendCh:
			switch {
			case fr.Result != nil:
				resp, err := s.client.ReportResult(s.ctx, ReportResultRequest{
					RunnerID:  hello.RunnerID,
					SessionID: s.sessionID,
					Lease:     fr.Result.Lease,
					Result:    fr.Result.Result,
					AuthToken: s.authToken,
				})
				if err != nil {
					if !s.emit(ServerFrame{Ack: &AckFrame{LeaseID: fr.Result.LeaseID, Accepted: false, Error: err.Error()}}) {
						return
					}
					continue
				}
				if !s.emit(ServerFrame{Ack: &AckFrame{LeaseID: fr.Result.LeaseID, Accepted: resp.Accepted, Error: resp.Error}}) {
					return
				}
			case fr.ControlObservation != nil:
				s.setRecoveryOnly(fr.ControlObservation.RecoveryOnly)
				resp, err := s.client.Heartbeat(s.ctx, HeartbeatRequest{
					RunnerID:  hello.RunnerID,
					SessionID: s.sessionID,
					Capacity:  hello.Concurrency,
					InFlight:  int(fr.ControlObservation.ActiveWorkers),
					DrainObservation: &RunnerDrainObservation{
						Generation:        fr.ControlObservation.Generation,
						RecoveryOnly:      fr.ControlObservation.RecoveryOnly,
						ActiveActivations: fr.ControlObservation.ActiveActivations,
					},
				})
				if err != nil {
					if !s.emit(ServerFrame{Ack: &AckFrame{Accepted: false, Error: err.Error()}}) {
						return
					}
					continue
				}
				if !s.emitControl(resp.Control) {
					return
				}
			case fr.Bye != nil:
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *httpStream) emit(frame ServerFrame) bool {
	select {
	case s.recvCh <- frame:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *httpStream) emitControl(directive *RunnerControlDirective) bool {
	if directive == nil {
		return true
	}
	return s.emit(ServerFrame{Control: &ControlFrame{Directive: directive}})
}

func (s *httpStream) setRecoveryOnly(recoveryOnly bool) {
	s.controlMu.Lock()
	s.recoveryOnly = recoveryOnly
	s.controlMu.Unlock()
}

func (s *httpStream) isRecoveryOnly() bool {
	s.controlMu.RLock()
	defer s.controlMu.RUnlock()
	return s.recoveryOnly
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
