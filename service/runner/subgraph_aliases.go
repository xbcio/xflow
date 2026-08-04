package runner

import "github.com/xbcio/xflow/execution/subgraph"

// PackageCache and friends moved to execution/subgraph (Task 8): a node group
// is one consumer of sub-graph execution, so the package-validation cache
// that resolves a group's compiled sub-graph now lives below both node
// groups and map bodies rather than inside the runner. These aliases keep
// existing callers (cmd/runner, the group binary e2e test, and this
// package's own tests) compiling unchanged against the same underlying
// types -- there is no behavioral difference between referring to the type
// via runner.PackageCache or subgraph.PackageCache.
type (
	PackageCache           = subgraph.PackageCache
	PackageCacheConfig     = subgraph.PackageCacheConfig
	PackageValidationError = subgraph.PackageValidationError
	HandlerInventory       = subgraph.HandlerInventory
)

// NewPackageCache creates a bounded package cache. See subgraph.NewPackageCache.
var NewPackageCache = subgraph.NewPackageCache

// ErrPackageMissing is returned when the payload has no package and the
// cache does not contain the hash. See subgraph.ErrPackageMissing.
var ErrPackageMissing = subgraph.ErrPackageMissing
