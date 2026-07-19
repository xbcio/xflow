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
	statusKey := "xflow:tdefault:exec:{" + execID + "}:status"
	deadKey := "xflow:tdefault:exec:{" + execID + "}:outbox:dead"
	deadBodyKey := "xflow:tdefault:exec:{" + execID + "}:outbox:dead:body"
	deadMetaKey := "xflow:tdefault:exec:{" + execID + "}:outbox:dead:meta:" + entryID
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
	if err := rdb.HSet(ctx, deadMetaKey, "node", "review", "activation", "1").Err(); err != nil {
		t.Fatalf("hset dead meta: %v", err)
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
		"--execution", execID, "--entry", entryID, "--reason", "operator triage", "--request-id", "req-1"); err != nil {
		t.Fatalf("dead-letter replay: %v", err)
	}
	var res map[string]any
	if err := json.Unmarshal(replayOut.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal replay result: %v (out=%q)", err, replayOut.String())
	}
	if res["outcome"] != "replayed" {
		t.Fatalf("replay outcome = %v, want replayed", res["outcome"])
	}
	if res["audit_id"] == "" || res["execution"] != execID || res["node"] != "review" {
		t.Fatalf("replay result missing fields: %+v", res)
	}

	// second replay with the same --request-id returns already_replayed with the
	// original audit_id (response-loss recovery), not a fresh replay.
	var replayOut2 bytes.Buffer
	if err := executeRootWith(&replayOut2, "dead-letter", "--redis-addr", mr.Addr(), "replay",
		"--execution", execID, "--entry", entryID, "--reason", "retry after lost response", "--request-id", "req-1"); err != nil {
		t.Fatalf("second dead-letter replay: %v", err)
	}
	var res2 map[string]any
	if err := json.Unmarshal(replayOut2.Bytes(), &res2); err != nil {
		t.Fatalf("unmarshal replay2 result: %v (out=%q)", err, replayOut2.String())
	}
	if res2["outcome"] != "already_replayed" {
		t.Fatalf("second replay outcome = %v, want already_replayed", res2["outcome"])
	}
	if res2["audit_id"] != res["audit_id"] {
		t.Fatalf("second replay audit_id = %v, want original %v", res2["audit_id"], res["audit_id"])
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
