package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// deadLetterBody mirrors rstate.redisOutboxEntry so the test can seed a
// dead-letter entry with raw Redis writes (the CLI itself never builds keys).
type deadLetterBody struct {
	ID          string        `json:"id"`
	Task        deadLetterTask `json:"task"`
	AutoDepth   int           `json:"auto_depth,omitempty"`
	Activation  int           `json:"activation_id,omitempty"`
	AvailableAt int64         `json:"available_at_ms,omitempty"`
	CreatedAt   int64         `json:"created_at_ms,omitempty"`
}

type deadLetterTask struct {
	ExecutionID string `json:"execution_id"`
	NodeName    string `json:"node_name"`
	NodeIdx     int    `json:"node_idx"`
	Type        int    `json:"type"`
}

func seedDeadLetterRedis(t *testing.T, addr, execID, entryID string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	statusKey := "xflow:exec:{" + execID + "}:status"
	deadKey := "xflow:exec:{" + execID + "}:outbox:dead"
	deadBodyKey := "xflow:exec:{" + execID + "}:outbox:dead:body"
	if err := rdb.Set(ctx, statusKey, "running", time.Minute).Err(); err != nil {
		t.Fatalf("set status: %v", err)
	}
	body, _ := json.Marshal(deadLetterBody{
		ID:   entryID,
		Task: deadLetterTask{ExecutionID: execID, NodeName: "review", NodeIdx: 1, Type: 0},
	})
	if err := rdb.HSet(ctx, deadBodyKey, entryID, string(body)).Err(); err != nil {
		t.Fatalf("hset dead body: %v", err)
	}
	if err := rdb.ZAdd(ctx, deadKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: entryID}).Err(); err != nil {
		t.Fatalf("zadd dead: %v", err)
	}
}

func TestDeadLetterCLIListAndReplay(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	execID := "cli-dl-1"
	entryID := "execute/cli-dl-1/review/1"
	seedDeadLetterRedis(t, mr.Addr(), execID, entryID)

	// list
	var listOut bytes.Buffer
	if err := executeRootWith(&listOut, "dead-letter", "--redis-addr", mr.Addr(), "list", "--execution", execID); err != nil {
		t.Fatalf("dead-letter list: %v", err)
	}
	var listed []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(listOut.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("unmarshal list line %q: %v", line, err)
		}
		listed = append(listed, entry)
	}
	if len(listed) != 1 {
		t.Fatalf("listed %d entries, want 1: %s", len(listed), listOut.String())
	}
	if listed[0]["ID"] != entryID {
		t.Fatalf("listed id = %v, want %q", listed[0]["ID"], entryID)
	}

	// replay
	var replayOut bytes.Buffer
	if err := executeRootWith(&replayOut, "dead-letter", "--redis-addr", mr.Addr(), "replay",
		"--execution", execID, "--entry", entryID, "--reason", "operator triage", "--operator", "alice"); err != nil {
		t.Fatalf("dead-letter replay: %v", err)
	}
	var audit map[string]any
	if err := json.Unmarshal(replayOut.Bytes(), &audit); err != nil {
		t.Fatalf("unmarshal audit: %v", err)
	}
	if audit["outcome"] != "replayed" {
		t.Fatalf("audit outcome = %v, want replayed", audit["outcome"])
	}
	if audit["operator"] != "alice" || audit["reason"] != "operator triage" || audit["entry"] != entryID {
		t.Fatalf("audit missing fields: %+v", audit)
	}

	// second replay is an idempotent no-op (not_found)
	var replayOut2 bytes.Buffer
	if err := executeRootWith(&replayOut2, "dead-letter", "--redis-addr", mr.Addr(), "replay",
		"--execution", execID, "--entry", entryID, "--reason", "duplicate", "--operator", "bob"); err != nil {
		t.Fatalf("second dead-letter replay: %v", err)
	}
	var audit2 map[string]any
	if err := json.Unmarshal(replayOut2.Bytes(), &audit2); err != nil {
		t.Fatalf("unmarshal audit2: %v", err)
	}
	if audit2["outcome"] != "not_found" {
		t.Fatalf("second replay outcome = %v, want not_found", audit2["outcome"])
	}
}

func TestDeadLetterCLIReplayRequiresReason(t *testing.T) {
	var out bytes.Buffer
	err := executeRootWith(&out, "dead-letter", "replay", "--execution", "x", "--entry", "y")
	if err == nil {
		t.Fatal("replay without --reason = nil, want error")
	}
}

func TestDeadLetterCLIListRequiresExecution(t *testing.T) {
	var out bytes.Buffer
	err := executeRootWith(&out, "dead-letter", "list")
	if err == nil {
		t.Fatal("list without --execution = nil, want error")
	}
}
