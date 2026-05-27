// Cluster example: order fulfillment pipeline running on the cluster engine
// (Redis + Asynq workers).
//
// DAG topology:
//
//	ParseOrder ──→ CheckInventory ──→ ProcessPayment ──main──→ DispatchOrder
//	                                        ╚──error──→ CancelOrder
//
// Key differences from the local example (basic_test.go):
//
//  1. All handlers must be registered via node.Register — direct TaskHandler
//     instances cannot be serialised and sent to remote workers.
//  2. Use NewCluster instead of WorkflowBuilder.Run; call engine.Stop() to
//     release connections.
//  3. wf.Run() is not available for cluster execution; use
//     engine.Submit() + engine.Wait() explicitly.
//
// Prerequisites: Redis running at localhost:6379
// Run:           go test ./sdk/examples/ -run TestOrderFulfillment -v -timeout 30s
package examples_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	xflow "github.com/xbcio/xflow/sdk"
	"github.com/xbcio/xflow/types"
)

const redisAddr = "localhost:6379"

// ── handlers ──────────────────────────────────────────────────────────────────
// All handlers are registered with unique type strings so they can be resolved
// by workers in a cluster deployment.

// fulfillmentParseHandler reads order data injected as input.Data for the
// source node and validates required fields.
type fulfillmentParseHandler struct{}

func (h *fulfillmentParseHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.fulfillment.parse", DisplayName: "Parse Order"}
}

func (h *fulfillmentParseHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	orderID, _ := input.Data["order_id"].(string)
	if orderID == "" {
		return nil, fmt.Errorf("parse order: order_id is required")
	}
	amount, ok := input.Data["amount"].(float64)
	if !ok || amount <= 0 {
		return nil, fmt.Errorf("parse order: amount must be a positive number")
	}
	customer, _ := input.Data["customer"].(string)
	return &node.Output{Data: map[string]any{
		"order_id": orderID,
		"amount":   amount,
		"customer": customer,
		"currency": "CNY",
	}}, nil
}

// fulfillmentInventoryHandler simulates an inventory reservation check.
// Always succeeds in this example; a real implementation would call a stock service.
type fulfillmentInventoryHandler struct{}

func (h *fulfillmentInventoryHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.fulfillment.inventory", DisplayName: "Check Inventory"}
}

func (h *fulfillmentInventoryHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	out := make(map[string]any, len(input.Data)+1)
	for k, v := range input.Data {
		out[k] = v
	}
	out["inventory_reserved"] = true
	return &node.Output{Data: out}, nil
}

// fulfillmentPaymentHandler processes payment for the order.
// Returns a business error for amounts that exceed the single-transaction limit,
// routing the order to CancelOrder via the "error" port.
type fulfillmentPaymentHandler struct{}

func (h *fulfillmentPaymentHandler) Descriptor() node.Descriptor {
	return node.Descriptor{
		Type:        "demo.fulfillment.payment",
		DisplayName: "Process Payment",
		Outputs: []node.PortSpec{
			{Name: "main", DisplayName: "Charged"},
			{Name: "error", DisplayName: "Payment Failed"},
		},
	}
}

func (h *fulfillmentPaymentHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	amount, _ := input.Data["amount"].(float64)
	const singleTxLimit = 10000.0

	if amount > singleTxLimit {
		return &node.Output{
			Data: map[string]any{
				"paid":   false,
				"amount": amount,
				"reason": fmt.Sprintf("%.2f CNY exceeds single-transaction limit %.2f", amount, singleTxLimit),
			},
			Error: &node.Error{
				Message:    "payment declined: exceeds single-transaction limit",
				StatusCode: 402,
			},
		}, nil
	}

	out := make(map[string]any, len(input.Data)+2)
	for k, v := range input.Data {
		out[k] = v
	}
	out["paid"] = true
	out["transaction_id"] = fmt.Sprintf("TXN-%s", input.Data["order_id"])
	return &node.Output{Data: out}, nil
}

// fulfillmentDispatchHandler marks the order as dispatched for delivery.
type fulfillmentDispatchHandler struct{}

func (h *fulfillmentDispatchHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.fulfillment.dispatch", DisplayName: "Dispatch Order"}
}

func (h *fulfillmentDispatchHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	if input.Data == nil || input.Data["paid"] != true {
		return &node.Output{Data: map[string]any{"status": "skipped"}}, nil
	}
	return &node.Output{Data: map[string]any{
		"status":      "dispatched",
		"order_id":    input.Data["order_id"],
		"tracking_no": fmt.Sprintf("SF%s", input.Data["transaction_id"]),
		"message":     "order dispatched for delivery",
	}}, nil
}

// fulfillmentCancelHandler cancels an order when payment fails.
// Connected to ProcessPayment's "error" output port.
type fulfillmentCancelHandler struct{}

func (h *fulfillmentCancelHandler) Descriptor() node.Descriptor {
	return node.Descriptor{Type: "demo.fulfillment.cancel", DisplayName: "Cancel Order"}
}

func (h *fulfillmentCancelHandler) Execute(_ context.Context, input *node.Input) (*node.Output, error) {
	if input.Data == nil || input.Data["paid"] != false {
		return &node.Output{Data: map[string]any{"status": "skipped"}}, nil
	}
	return &node.Output{Data: map[string]any{
		"status":   "cancelled",
		"order_id": input.Data["order_id"],
		"reason":   input.Data["reason"],
		"message":  "order cancelled; inventory reservation released",
	}}, nil
}

func init() {
	node.Register(&fulfillmentParseHandler{})
	node.Register(&fulfillmentInventoryHandler{})
	node.Register(&fulfillmentPaymentHandler{})
	node.Register(&fulfillmentDispatchHandler{})
	node.Register(&fulfillmentCancelHandler{})
}

// ── workflow factory ──────────────────────────────────────────────────────────

func buildOrderWorkflow() *xflow.WorkflowBuilder {
	wf := xflow.NewWorkflow("order-fulfillment")

	parse     := wf.AddNode("ParseOrder",     node.New(&fulfillmentParseHandler{}, nil))
	inventory := wf.AddNode("CheckInventory", node.New(&fulfillmentInventoryHandler{}, nil))
	payment   := wf.AddNode("ProcessPayment", node.New(&fulfillmentPaymentHandler{}, nil).OnError(node.OnErrorOutput))
	dispatch  := wf.AddNode("DispatchOrder",  node.New(&fulfillmentDispatchHandler{}, nil))
	cancel    := wf.AddNode("CancelOrder",    node.New(&fulfillmentCancelHandler{}, nil))

	wf.Connect(parse.Out("main"), inventory).
		Connect(inventory.Out("main"), payment).
		Connect(payment.Out("main"), dispatch). // payment succeeded → dispatch
		Connect(payment.Out("error"), cancel)   // payment failed   → cancel

	return wf
}

// ── engine helper ─────────────────────────────────────────────────────────────

// startClusterEngine creates a ClusterEngine connected to Redis.
// Skips the test if Redis is not available at redisAddr.
func startClusterEngine(t *testing.T) *xflow.Engine {
	t.Helper()
	// Quick TCP probe — avoids importing a Redis client just for availability check.
	conn, err := net.DialTimeout("tcp", redisAddr, time.Second)
	if err != nil {
		t.Skipf("skipping cluster test — Redis unavailable at %s: %v", redisAddr, err)
	}
	conn.Close()

	engine, err := xflow.NewCluster(redisAddr, nil)
	if err != nil {
		t.Skipf("skipping cluster test — NewCluster failed: %v", err)
	}
	t.Cleanup(engine.Stop)
	return engine
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestOrderFulfillment runs two order scenarios against the cluster engine.
func TestOrderFulfillment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	engine := startClusterEngine(t)

	t.Run("normal order — dispatched", func(t *testing.T) {
		params := map[string]any{
			"order_id": "ORD-001",
			"amount":   float64(299.99),
			"customer": "alice",
		}

		id, err := engine.Submit(ctx, buildOrderWorkflow(), params)
		if err != nil {
			t.Fatalf("Submit() error: %v", err)
		}
		t.Logf("execution id: %s", id)

		result, err := engine.Wait(ctx, id)
		if err != nil {
			t.Fatalf("Wait() error: %v", err)
		}
		if result.Status != types.StatusSuccess {
			t.Fatalf("status = %q, want success; error: %s", result.Status, result.Error)
		}

		dispatch := mustNodeOutput(t, result, "DispatchOrder")
		if dispatch["status"] != "dispatched" {
			t.Errorf("DispatchOrder.status = %q, want \"dispatched\"", dispatch["status"])
		}
		// CancelOrder is skipped (not reachable via payment.Out("main")); the engine
		// does not write output for skipped nodes, so it must be absent from the result.
		if _, ok := result.Output["CancelOrder"]; ok {
			t.Errorf("CancelOrder should be skipped (absent from output), but found: %v", result.Output["CancelOrder"])
		}
		t.Logf("outcome: %v", dispatch["message"])
	})

	t.Run("large order — payment declined, cancelled", func(t *testing.T) {
		params := map[string]any{
			"order_id": "ORD-002",
			"amount":   float64(50000),
			"customer": "bob",
		}

		id, err := engine.Submit(ctx, buildOrderWorkflow(), params)
		if err != nil {
			t.Fatalf("Submit() error: %v", err)
		}
		t.Logf("execution id: %s", id)

		result, err := engine.Wait(ctx, id)
		if err != nil {
			t.Fatalf("Wait() error: %v", err)
		}
		if result.Status != types.StatusSuccess {
			t.Fatalf("status = %q, want success; error: %s", result.Status, result.Error)
		}

		cancelOut := mustNodeOutput(t, result, "CancelOrder")
		if cancelOut["status"] != "cancelled" {
			t.Errorf("CancelOrder.status = %q, want \"cancelled\"", cancelOut["status"])
		}
		// DispatchOrder is skipped (not reachable via payment.Out("error")); the engine
		// does not write output for skipped nodes, so it must be absent from the result.
		if _, ok := result.Output["DispatchOrder"]; ok {
			t.Errorf("DispatchOrder should be skipped (absent from output), but found: %v", result.Output["DispatchOrder"])
		}
		t.Logf("outcome: %v", cancelOut["message"])
	})

	t.Run("invalid order — missing order_id", func(t *testing.T) {
		params := map[string]any{
			"amount":   float64(99),
			"customer": "charlie",
		}

		id, err := engine.Submit(ctx, buildOrderWorkflow(), params)
		if err != nil {
			t.Fatalf("Submit() error: %v", err)
		}

		result, err := engine.Wait(ctx, id)
		if err != nil {
			t.Fatalf("Wait() error: %v", err)
		}
		if result.Status != types.StatusFailed {
			t.Fatalf("status = %q, want failed", result.Status)
		}
		t.Logf("workflow failed as expected: %s", result.Error)
	})
}
