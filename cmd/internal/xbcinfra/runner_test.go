package xbcinfra

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"

	xbclog "github.com/xbcio/xbc/log"
)

func TestRunnerArgsWithoutConfigLeavesArgumentsAlone(t *testing.T) {
	args := []string{"run", "--server", "http://localhost:8080", "--id", "r1"}
	got, err := RunnerArgs(args)
	if err != nil {
		t.Fatalf("RunnerArgs() = %v", err)
	}
	if !reflect.DeepEqual(got, args) {
		t.Fatalf("RunnerArgs() = %#v, want %#v", got, args)
	}
}

func TestRunnerArgsExtractsAndInstallsTheLoggingConfig(t *testing.T) {
	restoreLogger(t)
	restoreSlog(t)

	path := writeConfig(t, "runner.yml", "log:\n  level: error\n")
	got, err := RunnerArgs([]string{"--xbc-config", path, "run", "--id", "r1"})
	if err != nil {
		t.Fatalf("RunnerArgs() = %v", err)
	}
	if want := []string{"run", "--id", "r1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RunnerArgs() = %#v, want %#v", got, want)
	}
	if !slog.Default().Enabled(t.Context(), slog.LevelError) {
		t.Fatal("the installed slog default does not enable error")
	}
	if slog.Default().Enabled(t.Context(), slog.LevelDebug) {
		t.Fatal("the installed slog default enables debug below its error level")
	}
	if !xbclog.L().Enabled(xbclog.ErrorLevel) {
		t.Fatal("the xbc facade was not initialized")
	}
}

func TestRunnerArgsAcceptsTheJoinedForm(t *testing.T) {
	restoreLogger(t)
	restoreSlog(t)

	path := writeConfig(t, "runner.yml", "log:\n  level: warn\n")
	got, err := RunnerArgs([]string{"--xbc-config=" + path, "verify"})
	if err != nil {
		t.Fatalf("RunnerArgs() = %v", err)
	}
	if want := []string{"verify"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RunnerArgs() = %#v, want %#v", got, want)
	}
}

func TestRunnerArgsRejectsMalformedConfigOptions(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing value", args: []string{"run", "--xbc-config"}, want: "requires a path"},
		{name: "empty joined value", args: []string{"--xbc-config="}, want: "non-empty path"},
		{
			name: "duplicate",
			args: []string{"--xbc-config=a.yml", "--xbc-config=b.yml"},
			want: "more than once",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RunnerArgs(tc.args)
			if err == nil {
				t.Fatal("RunnerArgs() accepted a malformed option")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestRunnerArgsRejectsConfigWithoutLogging(t *testing.T) {
	path := writeConfig(t, "runner.yml", "plugins:\n  metrics:\n    namespace: xflow\n")
	_, err := RunnerArgs([]string{"--xbc-config", path, "run"})
	if err == nil {
		t.Fatal("RunnerArgs() accepted a config with no log section")
	}
	if !strings.Contains(err.Error(), "no log section") {
		t.Fatalf("error = %v, want it to name the missing section", err)
	}
}

func TestRunnerArgsRejectsMissingFile(t *testing.T) {
	_, err := RunnerArgs([]string{"--xbc-config", t.TempDir() + "/missing.yml", "run"})
	if err == nil {
		t.Fatal("RunnerArgs() accepted a missing file")
	}
}

func restoreSlog(t *testing.T) {
	t.Helper()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
}
