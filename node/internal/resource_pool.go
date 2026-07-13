package internal

import (
	"context"

	"github.com/xbcio/xflow/internal/noderuntime"
)

type ResourcePool = noderuntime.ResourcePool
type SQLPoolConfig = noderuntime.SQLPoolConfig
type GRPCPoolConfig = noderuntime.GRPCPoolConfig
type ResourcePoolConfig = noderuntime.ResourcePoolConfig

func DefaultResourcePoolConfig() ResourcePoolConfig {
	return noderuntime.DefaultResourcePoolConfig()
}

func NewDefaultResourcePool(cfg ResourcePoolConfig) ResourcePool {
	return noderuntime.NewDefaultResourcePool(cfg)
}

func WithResourcePool(ctx context.Context, pool ResourcePool) context.Context {
	return noderuntime.WithResourcePool(ctx, pool)
}

func ResourcePoolFromContext(ctx context.Context) ResourcePool {
	return noderuntime.ResourcePoolFromContext(ctx)
}
