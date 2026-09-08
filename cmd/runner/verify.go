package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
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
			res, err := verifyRunner(cmd.Context(), resolved)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(opts.out, "runner verified: %s (supply encryption: %v)\n",
				res.RunnerID, res.SupplyKeyIssued)
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
func verifyRunner(ctx context.Context, cfg runnerConfig) (xflowsdk.VerifyResult, error) {
	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		return xflowsdk.VerifyResult{}, err
	}
	res, err := xflowsdk.VerifyRunner(ctx, sdkCfg)
	if err != nil {
		return xflowsdk.VerifyResult{}, err
	}
	// The same assertion --require-supply-encryption makes at run time, made
	// here instead so it fails on an operator's terminal rather than in a
	// CrashLoopBackOff. Keeping the two rules textually adjacent is why this
	// lives in verifyRunner and not in the RunE closure.
	if cfg.requireSupplyEncryption && !res.SupplyKeyIssued {
		// Returns the populated res, not VerifyResult{}, deliberately: the
		// error text below reads res.RunnerID. The current caller ignores res
		// on a non-nil error, so this is not yet load-bearing, but keep it if
		// you touch this branch — every other error return in this file and in
		// VerifyRunner zeroes the result, so this one reads like a mistake
		// until you reach the error text.
		return res, fmt.Errorf(
			"--require-supply-encryption is set but the control plane issued no supply encryption key for runner %q",
			res.RunnerID)
	}
	return res, nil
}
