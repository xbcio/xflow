package main

import "testing"

func TestParseServerConfigSupportsMemoryMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":9090", "-memory", "-concurrency", "3"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.addr != ":9090" || !cfg.memory || cfg.concurrency != 3 {
		t.Fatalf("config = %+v, want addr :9090 memory concurrency 3", cfg)
	}
}

func TestParseServerConfigSupportsRedisMode(t *testing.T) {
	cfg, err := parseServerConfig([]string{"-addr", ":8080", "-redis", "localhost:6379"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redis != "localhost:6379" || cfg.memory {
		t.Fatalf("config = %+v, want redis localhost:6379", cfg)
	}
}
