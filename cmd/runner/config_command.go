package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newConfigCommand(opts commandOptions, cfg *runnerConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage runner configuration",
	}

	validate := &cobra.Command{
		Use:   "validate",
		Short: "Validate runner configuration",
		RunE: func(cmd *cobra.Command, _ []string) error {
			recordChangedFlags(cmd, cfg)
			resolved, err := resolveRunnerConfig(*cfg)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(opts.out, "runner config valid: %s\n", resolved.runnerID)
			return err
		},
	}
	validate.Flags().StringVarP(&cfg.configPath, "config", "c", cfg.configPath, "Runner config file")
	bindRunnerFlags(validate, cfg)

	sample := &cobra.Command{
		Use:   "sample",
		Short: "Print a sample runner configuration",
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := fmt.Fprint(opts.out, sampleRunnerConfigYAML())
			return err
		},
	}

	cmd.AddCommand(validate, sample)
	return cmd
}
