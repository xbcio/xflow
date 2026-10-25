package xbcinfra

import (
	"fmt"
	"strings"
)

// RunnerArgs extracts the runner binary's --xbc-config option from args and, if
// it is present, installs the configured xbc logger as slog's default. It
// returns the remaining arguments for sdk/runner's own parser.
//
// The runner command owns its flag surface in sdk/runner, so this integration
// consumes its option before delegation rather than teaching the SDK about
// xbc. Both the separated form (--xbc-config path) and the joined form
// (--xbc-config=path) are accepted. Without the option, args are returned
// unchanged and no process-global logger is touched.
func RunnerArgs(args []string) ([]string, error) {
	path, remaining, err := extractConfigPath(args)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return remaining, nil
	}
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	if !cfg.HasLog() {
		return nil, fmt.Errorf("xbcinfra: %s configures no log section; remove --xbc-config or add one", path)
	}
	if err := InstallSlog(cfg.Log); err != nil {
		return nil, err
	}
	return remaining, nil
}

func extractConfigPath(args []string) (string, []string, error) {
	var (
		path      string
		remaining = make([]string, 0, len(args))
	)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--xbc-config":
			if path != "" {
				return "", nil, fmt.Errorf("xbcinfra: --xbc-config specified more than once")
			}
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("xbcinfra: --xbc-config requires a path")
			}
			path = args[i+1]
			i++
		case strings.HasPrefix(arg, "--xbc-config="):
			if path != "" {
				return "", nil, fmt.Errorf("xbcinfra: --xbc-config specified more than once")
			}
			path = strings.TrimPrefix(arg, "--xbc-config=")
		default:
			remaining = append(remaining, arg)
		}
	}
	if path == "" && containsConfigFlag(args) {
		return "", nil, fmt.Errorf("xbcinfra: --xbc-config requires a non-empty path")
	}
	return path, remaining, nil
}

func containsConfigFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--xbc-config" || strings.HasPrefix(arg, "--xbc-config=") {
			return true
		}
	}
	return false
}
