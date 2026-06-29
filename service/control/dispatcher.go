package control

import (
	"context"
	"errors"

	"github.com/xbcio/xflow/engine"
)

var ErrNoRunnerAvailable = errors.New("no runner available for task lease")

type LeaseBuilder interface {
	BuildTaskLease(ctx context.Context, t *engine.Task) (*engine.TaskLease, error)
}

type Dispatcher struct {
	engine  LeaseBuilder
	runners *RunnerPool
}

func NewDispatcher(engine LeaseBuilder, runners *RunnerPool) *Dispatcher {
	return &Dispatcher{
		engine:  engine,
		runners: runners,
	}
}

func (d *Dispatcher) HandleTask(ctx context.Context, task *engine.Task) error {
	lease, err := d.engine.BuildTaskLease(ctx, task)
	if err != nil {
		if err == engine.ErrExecutionInactive {
			return nil
		}
		return err
	}
	if lease == nil {
		return ErrNoRunnerAvailable
	}
	if d.runners == nil || !d.runners.Assign(*lease) {
		return ErrNoRunnerAvailable
	}
	return nil
}
