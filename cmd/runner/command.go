package main

import (
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type commandOptions struct {
	runFunc func(runnerConfig) error
	out     io.Writer
	err     io.Writer
}

var legacyRunnerLongFlags = map[string]struct{}{
	"cap":                {},
	"concurrency":        {},
	"config":             {},
	"heartbeat-interval": {},
	"id":                 {},
	"log-format":         {},
	"log-level":          {},
	"poll-wait":          {},
	"server":             {},
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
		Use:              "xflow-runner",
		Short:            "XFlow task runner",
		SilenceUsage:     true,
		SilenceErrors:    true,
		TraverseChildren: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			recordChangedFlags(cmd, &cfg)
			resolved, err := resolveRunnerConfig(cfg)
			if err != nil {
				return err
			}
			return opts.runFunc(resolved)
		},
	}
	root.DisableFlagParsing = true
	root.SetOut(opts.out)
	root.SetErr(opts.err)
	root.Flags().StringVarP(&cfg.configPath, "config", "c", cfg.configPath, "Runner config file")
	bindRunnerFlags(root, &cfg)
	root.RunE = wrapLegacyRunnerCommand(root.RunE)

	run := newRunCommand(opts, &cfg)
	run.DisableFlagParsing = true
	run.Flags().StringVarP(&cfg.configPath, "config", "c", cfg.configPath, "Runner config file")
	run.RunE = wrapLegacyRunnerCommand(run.RunE)
	root.AddCommand(run)
	return root
}

func executeRoot(args ...string) error {
	return executeRootWithOptions(commandOptions{}, args...)
}

func executeRootWithOptions(opts commandOptions, args ...string) error {
	cmd := newRootCommand(opts)
	cmd.SetArgs(args)
	return cmd.Execute()
}

func wrapLegacyRunnerCommand(runE func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := cmd.Flags().Parse(normalizeLegacyRunnerArgs(args)); err != nil {
			return err
		}
		return runE(cmd, cmd.Flags().Args())
	}
}

func normalizeLegacyRunnerArgs(args []string) []string {
	normalized := make([]string, 0, len(args))
	passThrough := false
	for _, arg := range args {
		if passThrough {
			normalized = append(normalized, arg)
			continue
		}
		if arg == "--" {
			passThrough = true
			normalized = append(normalized, arg)
			continue
		}
		normalized = append(normalized, normalizeLegacyRunnerArg(arg))
	}
	return normalized
}

func normalizeLegacyRunnerArg(arg string) string {
	if !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "--") || len(arg) <= 2 {
		return arg
	}

	name, value, hasValue := strings.Cut(arg[1:], "=")
	if _, ok := legacyRunnerLongFlags[name]; !ok {
		return arg
	}
	if hasValue {
		return "--" + name + "=" + value
	}
	return "--" + name
}
