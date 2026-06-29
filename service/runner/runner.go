package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
)

type ProtocolClient interface {
	Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error)
	Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error)
	Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error)
	ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error)
}

type Config struct {
	RunnerID          string
	Concurrency       int
	Capabilities      []protocol.Capability
	HeartbeatInterval time.Duration
	PollWait          time.Duration
}

type Runner struct {
	client   ProtocolClient
	executor *execution.Runner
	config   Config
}

func New(client ProtocolClient, registry engine.HandlerRegistry, config Config) *Runner {
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.PollWait <= 0 {
		config.PollWait = time.Second
	}
	return &Runner{
		client:   client,
		executor: execution.NewRunner(registry),
		config:   config,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	if _, err := r.client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     r.config.RunnerID,
		Concurrency:  r.config.Concurrency,
		Capabilities: r.config.Capabilities,
	}); err != nil {
		return err
	}

	inFlight := 0
	if err := r.heartbeat(ctx, inFlight); err != nil {
		return err
	}

	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			if err := r.heartbeat(ctx, inFlight); err != nil {
				return err
			}
			continue
		default:
		}

		resp, err := r.client.Poll(ctx, protocol.PollTaskRequest{
			RunnerID:     r.config.RunnerID,
			Capacity:     r.config.Concurrency - inFlight,
			Capabilities: r.config.Capabilities,
		})
		if err != nil {
			return err
		}
		if resp.Lease == nil {
			wait := resp.Wait
			if wait <= 0 {
				wait = r.config.PollWait
			}
			if err := sleepContext(ctx, wait); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			continue
		}

		inFlight = 1
		result, execErr := r.executor.Execute(ctx, resp.Lease)
		if execErr != nil {
			result = engine.TaskResult{Error: execErr}
		}
		reportResp, err := r.client.ReportResult(ctx, protocol.ReportResultRequest{
			RunnerID: r.config.RunnerID,
			Lease:    resp.Lease,
			Result:   result,
		})
		inFlight = 0
		if err != nil {
			return err
		}
		if !reportResp.Accepted {
			return fmt.Errorf("task result rejected: %s", reportResp.Error)
		}
	}
}

func (r *Runner) heartbeat(ctx context.Context, inFlight int) error {
	_, err := r.client.Heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID:  r.config.RunnerID,
		Capacity:  r.config.Concurrency,
		InFlight:  inFlight,
		Timestamp: time.Now().Unix(),
	})
	return err
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
