package main

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

type commandOptions struct {
	runFunc func(runnerConfig) error
	out     io.Writer
	err     io.Writer
}

func newRootCommand(opts commandOptions) *cobra.Command {
	if opts.runFunc == nil {
		opts.runFunc = runWithSignals
	}
	if opts.out == nil {
		opts.out = os.Stdout
	}
	if opts.err == nil {
		opts.err = os.Stderr
	}

	cfg := defaultRunnerConfig()
	cfg.configPath = os.Getenv("XFLOW_RUNNER_CONFIG")
	root := &cobra.Command{
		Use:           "xflow-runner",
		Short:         "XFlow task runner",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			recordChangedFlags(cmd, &cfg)
			resolved, err := resolveRunnerConfig(cfg)
			if err != nil {
				return err
			}
			return opts.runFunc(resolved)
		},
	}
	root.SetOut(opts.out)
	root.SetErr(opts.err)
	root.PersistentFlags().StringVarP(&cfg.configPath, "config", "c", cfg.configPath, "Runner config file")
	bindRunnerFlags(root, &cfg)
	root.AddCommand(newRunCommand(opts, &cfg))
	return root
}

func executeRoot(args ...string) error {
	cmd := newRootCommand(commandOptions{})
	cmd.SetArgs(args)
	return cmd.Execute()
}
