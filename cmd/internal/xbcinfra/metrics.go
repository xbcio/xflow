package xbcinfra

import (
	"fmt"

	xbcmetrics "github.com/xbcio/xbc/extensions/observability/metrics"

	xflowmetrics "github.com/xbcio/xflow/observability/metrics"
)

// NewMetrics constructs xbc's private metrics registry from cfg and binds
// xflow's collector to it. The returned plugin owns the registry and is the
// gatherer a host endpoint serves; the collector is the value xflow's server
// and control plane accept.
//
// Registration reaches the registry through xbc's all-or-nothing Registerer via
// the standard prometheus.Registerer adapter, and gathering uses the registry
// directly because its Gather signature is already prometheus.Gatherer's. The
// process-wide Prometheus default registry is never touched.
func NewMetrics(cfg xbcmetrics.Config) (*xbcmetrics.Plugin, *xflowmetrics.Metrics, error) {
	plugin, err := xbcmetrics.New(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("xbcinfra: initialize metrics: %w", err)
	}
	return plugin, xflowmetrics.NewWithRegisterer(
		xbcmetrics.PrometheusRegisterer(plugin),
		plugin.Registry(),
	), nil
}
