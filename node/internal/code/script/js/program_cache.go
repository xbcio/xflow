package js

import (
	"github.com/dop251/goja"
	lru "github.com/hashicorp/golang-lru/v2"
)

// programCache is a thin LRU wrapper for compiled goja programs. Bounded by
// DefaultProgramCacheSize so a deployment with high script churn does not
// grow unboundedly. hashicorp/golang-lru/v2 is internally thread-safe — no
// extra mutex is needed.
type programCache struct {
	c *lru.Cache[string, *goja.Program]
}

func newProgramCache(size int) *programCache {
	c, err := lru.New[string, *goja.Program](size)
	if err != nil {
		// lru.New only errors on size <= 0; callers pass positive constants.
		panic(err)
	}
	return &programCache{c: c}
}

func (p *programCache) get(code string) (*goja.Program, bool) {
	return p.c.Get(code)
}

func (p *programCache) add(code string, prog *goja.Program) {
	p.c.Add(code, prog)
}

func (p *programCache) contains(code string) bool {
	return p.c.Contains(code)
}
