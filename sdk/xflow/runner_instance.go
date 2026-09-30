package xflow

import (
	"os"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// DefaultRunnerInstanceUID returns the instance UID a runner sends on enroll and
// register when none is configured.
//
// POD_UID (injected through the Kubernetes downward API) comes first: it is
// stable across container restarts inside one pod, so a crash-looping
// container reclaims its own session immediately, yet differs between two
// pods, so replicas that ended up with the same runner ID are told apart.
//
// Outside Kubernetes the hostname stands in. A restarted process on the same
// host reclaims its session without waiting out the live window; two
// processes on one host that share a runner ID are not told apart, but the
// default runner ID already includes the PID for exactly that case.
//
// When neither POD_UID nor hostname is available, every call in this process
// shares one random fallback while another process gets a different value.
var (
	processRunnerInstanceUIDOnce sync.Once
	processRunnerInstanceUID     string
	runnerHostname               = os.Hostname
)

func DefaultRunnerInstanceUID() string {
	if uid := strings.TrimSpace(os.Getenv("POD_UID")); uid != "" {
		return "pod:" + uid
	}
	if host, err := runnerHostname(); err == nil && strings.TrimSpace(host) != "" {
		return "host:" + strings.TrimSpace(host)
	}
	processRunnerInstanceUIDOnce.Do(func() {
		processRunnerInstanceUID = "proc:" + uuid.NewString()
	})
	return processRunnerInstanceUID
}

func runnerInstanceUIDOrDefault(uid string) string {
	if uid = strings.TrimSpace(uid); uid != "" {
		return uid
	}
	return DefaultRunnerInstanceUID()
}
