package main

import (
	"os"

	"github.com/spf13/cobra"
)

// resolveEnvFlag returns the effective value of a connection flag once cobra
// has parsed the command line: an explicitly set flag wins, then the
// environment variable, then the flag's literal default.
//
// The default must stay literal. pflag renders a non-zero default in --help,
// and a MySQL DSN or Redis URL routinely carries a password, so a default read
// from the environment would print that password to whoever runs --help. The
// environment is consulted here, on the command's run path, never at flag
// registration time.
func resolveEnvFlag(cmd *cobra.Command, name, envKey, fallback string) string {
	if cmd.Flags().Changed(name) {
		if v, err := cmd.Flags().GetString(name); err == nil {
			return v
		}
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return fallback
}
