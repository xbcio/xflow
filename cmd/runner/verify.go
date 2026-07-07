package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/spf13/cobra"
)

func newVerifyCommand(opts commandOptions, cfg *runnerConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify runner configuration and server reachability",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			recordChangedFlags(cmd, cfg)
			resolved, err := resolveRunnerConfig(*cfg)
			if err != nil {
				return err
			}
			if err := verifyRunner(cmd.Context(), resolved); err != nil {
				return err
			}
			_, err = fmt.Fprintf(opts.out, "runner verified: %s\n", resolved.runnerID)
			return err
		},
	}
	bindRunnerFlags(cmd, cfg)
	return cmd
}

func verifyRunner(ctx context.Context, cfg runnerConfig) error {
	client := protocol.NewClient(cfg.serverURL, http.DefaultClient)
	if _, err := client.Register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     cfg.runnerID,
		Concurrency:  cfg.concurrency,
		Labels:       cloneStringMap(cfg.labels),
		Capabilities: cfg.capabilities,
	}); err != nil {
		return err
	}
	_, err := client.Heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID:  cfg.runnerID,
		Capacity:  cfg.concurrency,
		InFlight:  0,
		Timestamp: time.Now().Unix(),
	})
	return err
}
