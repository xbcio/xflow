package types

import (
	"context"
	"testing"
)

func TestCloseFuncImplementsTriggerSubscription(t *testing.T) {
	called := false
	sub := CloseFunc(func(context.Context) error {
		called = true
		return nil
	})
	if err := sub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("close function was not called")
	}
}
