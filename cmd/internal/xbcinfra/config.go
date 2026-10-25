package xbcinfra

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/xbcio/xbc/config"
	xbcmetrics "github.com/xbcio/xbc/extensions/observability/metrics"
	xbctracing "github.com/xbcio/xbc/extensions/observability/tracing"
	xbcgorm "github.com/xbcio/xbc/extensions/storage/gorm"
	xbclog "github.com/xbcio/xbc/log"
)

// EnvPrefix names the environment layer of the xbc configuration. It is
// deliberately distinct from xflow's own XFLOW_ namespace: xbc's loader treats
// every variable under its prefix as configuration and rejects one that names
// no declared section, so sharing XFLOW_ would make an unrelated variable such
// as XFLOW_MASTER_KEY a startup error. A value in the xbc file is therefore
// overridden with XFLOW_XBC_ plus its full path, for example
// XFLOW_XBC_PLUGINS_GORM_DEFAULT_DSN or XFLOW_XBC_LOG_LEVEL.
const EnvPrefix = "XFLOW_XBC_"

// GORMInstance is the single database instance this integration reads. xflow
// owns one durable store, so a second instance would have no consumer.
const GORMInstance = "default"

// Config is the resolved xbc-side infrastructure configuration. Each capability
// is present independently: a file that configures only logging leaves
// metrics, tracing, and the database on the binary's existing flags.
type Config struct {
	Log     xbclog.Config
	Metrics xbcmetrics.Config
	Tracing xbctracing.Config
	GORM    xbcgorm.Config

	logSet     bool
	metricsSet bool
	tracingSet bool
	gormSet    bool
}

// Load reads the xbc configuration file at path and returns the capabilities it
// configures. An empty path returns (nil, nil): --xbc-config is optional and
// its absence must not change the binary's behavior.
//
// The file is layered by xbc's loader: built-in defaults, then the file, then
// the profile overlay named by XFLOW_XBC_PROFILE, then the environment layer.
// A key outside the sections this package declares is an error rather than an
// ignored typo. Values for a section that is absent stay at xbc's defaults and
// do not activate that capability.
func Load(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	universe, err := newUniverse()
	if err != nil {
		return nil, err
	}
	environment, err := config.Load(config.Options{
		File:      path,
		Profile:   os.Getenv(EnvPrefix + "PROFILE"),
		EnvPrefix: EnvPrefix,
		Universe:  universe,
	})
	if err != nil {
		return nil, fmt.Errorf("xbcinfra: load %s: %w", path, err)
	}

	cfg := &Config{
		Log:     xbclog.DefaultConfig(),
		Metrics: xbcmetrics.DefaultConfig(),
		Tracing: xbctracing.DefaultConfig(),
		GORM:    xbcgorm.DefaultConfig(),
	}
	if environment.Exists("log") {
		if err := environment.Bind("log", &cfg.Log); err != nil {
			return nil, fmt.Errorf("xbcinfra: bind log: %w", err)
		}
		cfg.logSet = true
	}
	if environment.Exists("plugins.metrics") {
		if err := environment.Bind("plugins.metrics", &cfg.Metrics); err != nil {
			return nil, fmt.Errorf("xbcinfra: bind plugins.metrics: %w", err)
		}
		if cfg.Metrics.HTTP.Enabled {
			return nil, fmt.Errorf(
				"xbcinfra: plugins.metrics.http is not supported: xflow serves its registry through --metrics-addr; remove the section or disable it")
		}
		cfg.metricsSet = true
	}
	if environment.Exists("plugins.tracing") {
		if err := environment.Bind("plugins.tracing", &cfg.Tracing); err != nil {
			return nil, fmt.Errorf("xbcinfra: bind plugins.tracing: %w", err)
		}
		cfg.tracingSet = true
	}
	if environment.Exists("plugins.gorm." + GORMInstance) {
		if err := environment.Bind("plugins.gorm."+GORMInstance, &cfg.GORM); err != nil {
			return nil, fmt.Errorf("xbcinfra: bind plugins.gorm.%s: %w", GORMInstance, err)
		}
		cfg.gormSet = true
	}
	return cfg, nil
}

func newUniverse() (*config.Universe, error) {
	return config.NewUniverse(
		config.Section{
			Path:   "log",
			Owner:  "xflow",
			Kind:   config.SectionTyped,
			Schema: reflect.TypeOf(xbclog.Config{}),
		},
		config.Section{
			Path:  "plugins",
			Owner: "xflow",
			Kind:  config.SectionNamespace,
		},
		config.Section{
			Path:   "plugins.metrics",
			Owner:  "xflow",
			Kind:   config.SectionTyped,
			Schema: reflect.TypeOf(xbcmetrics.Config{}),
		},
		config.Section{
			Path:   "plugins.tracing",
			Owner:  "xflow",
			Kind:   config.SectionTyped,
			Schema: reflect.TypeOf(xbctracing.Config{}),
		},
		// Typed rather than instanced: xflow consumes exactly one durable
		// store, so any other instance name is a configuration error and
		// should say so instead of being silently ignored.
		config.Section{
			Path:   "plugins.gorm." + GORMInstance,
			Owner:  "xflow",
			Kind:   config.SectionTyped,
			Schema: reflect.TypeOf(xbcgorm.Config{}),
		},
	)
}

// HasLog reports whether the file configured the logging capability.
func (c *Config) HasLog() bool { return c != nil && c.logSet }

// HasMetrics reports whether the file configured the metrics capability.
func (c *Config) HasMetrics() bool { return c != nil && c.metricsSet }

// HasTracing reports whether the file configured the tracing capability.
func (c *Config) HasTracing() bool { return c != nil && c.tracingSet }

// HasGORM reports whether the file configured the durable database.
func (c *Config) HasGORM() bool { return c != nil && c.gormSet }
