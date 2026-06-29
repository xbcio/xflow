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
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
)

type serverConfig struct {
	addr        string
	redis       string
	memory      bool
	concurrency int
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
	fs.StringVar(&cfg.redis, "redis", "", "Redis address for Asynq backend")
	fs.BoolVar(&cfg.memory, "memory", false, "Use in-memory backend")
	fs.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Queue consumer concurrency")
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

	return http.ListenAndServe(cfg.addr, control.NewServer(eng, runners).Handler())
}
