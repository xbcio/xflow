package runnerapp

import (
	"fmt"
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
	"poll-wait":          {},
	"server":             {},
}

// NewCommand creates a standalone runner command using profile. It exposes the
// same YAML, environment, and CLI surface as xflow-runner; a profile can add
// non-overridable deployment policy without introducing a second parser.
func NewCommand(profile Profile) (*cobra.Command, error) {
	return newRootCommandForProfile(commandOptions{}, profile)
}

// Execute runs the default xflow-runner command.
func Execute(args ...string) error {
	return ExecuteProfile(Profile{}, args...)
}

// ExecuteProfile runs a standalone runner command with profile.
func ExecuteProfile(profile Profile, args ...string) error {
	cmd, err := NewCommand(profile)
	if err != nil {
		return err
	}
	cmd.SetArgs(normalizeLegacyRunnerArgs(args))
	return cmd.Execute()
}

func newRootCommand(opts commandOptions) *cobra.Command {
	cmd, err := newRootCommandForProfile(opts, Profile{})
	if err != nil {
		panic(fmt.Sprintf("default runner profile: %v", err))
	}
	return cmd
}

func newRootCommandForProfile(opts commandOptions, profile Profile) (*cobra.Command, error) {
	profile, err := normalizeProfile(profile)
	if err != nil {
		return nil, err
	}
	if opts.runFunc == nil {
		opts.runFunc = runWithSignals
	}
	if opts.out == nil {
		opts.out = os.Stdout
	}
	if opts.err == nil {
		opts.err = os.Stderr
	}

	cfg := defaultRunnerConfigForProfile(profile)
	cfg.configPath = os.Getenv("XFLOW_RUNNER_CONFIG")
	root := &cobra.Command{
		Use:           profile.CommandName,
		Short:         profile.Short,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
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

	run := newRunCommand(opts, &cfg)
	root.AddCommand(run)
	root.AddCommand(newVerifyCommand(opts, &cfg))
	root.AddCommand(newConfigCommand(opts, &cfg))
	return root, nil
}

func executeRootWithOptions(opts commandOptions, args ...string) error {
	return executeRootWithOptionsAndProfile(opts, Profile{}, args...)
}

func executeRootWithOptionsAndProfile(opts commandOptions, profile Profile, args ...string) error {
	cmd, err := newRootCommandForProfile(opts, profile)
	if err != nil {
		return err
	}
	cmd.SetArgs(normalizeLegacyRunnerArgs(args))
	return cmd.Execute()
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
