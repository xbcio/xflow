//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// blobE2EKeyText returns every byte a key's value holds as one string, for
// whatever type the key is, so that a substring search covers a hash field, a
// list element, a sorted-set member or a stream entry alike.
func blobE2EKeyText(ctx context.Context, rdb *redis.Client, key string) (string, error) {
	kind, err := rdb.Type(ctx, key).Result()
	if err != nil {
		return "", err
	}
	var parts []string
	switch kind {
	case "string":
		s, err := rdb.Get(ctx, key).Result()
		if err != nil && err != redis.Nil {
			return "", err
		}
		parts = append(parts, s)
	case "hash":
		m, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			return "", err
		}
		for f, v := range m {
			parts = append(parts, f, v)
		}
	case "list":
		l, err := rdb.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return "", err
		}
		parts = append(parts, l...)
	case "set":
		l, err := rdb.SMembers(ctx, key).Result()
		if err != nil {
			return "", err
		}
		parts = append(parts, l...)
	case "zset":
		l, err := rdb.ZRange(ctx, key, 0, -1).Result()
		if err != nil {
			return "", err
		}
		parts = append(parts, l...)
	case "stream":
		l, err := rdb.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return "", err
		}
		for _, m := range l {
			parts = append(parts, fmt.Sprint(m.Values))
		}
	}
	return strings.Join(parts, "\n"), nil
}
