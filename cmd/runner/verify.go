package main

import (
	"context"
	"fmt"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
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

// verifyRunner translates the resolved CLI/YAML config and hands it to the SDK,
// which owns the preflight itself.
//
// It used to build its own client (http.DefaultClient) and its own registration
// here. That discarded every connection flag this command binds — --transport,
// --grpc-target, --token, --tls-* — and registered a payload unlike the real
// one, so verify's verdict described a runner that would never start. The
// translation below is the same toSDKRunnerConfig the run command uses, which
// is what keeps the two commands answering about the same runner.
func verifyRunner(ctx context.Context, cfg runnerConfig) error {
	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		return err
	}
	return xflowsdk.VerifyRunner(ctx, sdkCfg)
}
