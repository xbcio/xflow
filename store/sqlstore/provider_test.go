package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type testSupplyEncryption struct{}

func (*testSupplyEncryption) Seal(plaintext []byte) ([]byte, error) {
	return append([]byte("sealed:"), plaintext...), nil
}

func (*testSupplyEncryption) Open(stored []byte) ([]byte, error) {
	return bytes.TrimPrefix(stored, []byte("sealed:")), nil
}

func TestWithSupplyEncryptionAcceptsNarrowContract(t *testing.T) {
	encryption := &testSupplyEncryption{}
	configured := &options{}

	WithSupplyEncryption(encryption)(configured)

	if configured.supplyAtRest != encryption {
		t.Fatal("WithSupplyEncryption did not retain the supplied implementation")
	}
}

func TestWithSupplyEncryptionTreatsTypedNilAsDisabled(t *testing.T) {
	var encryption *testSupplyEncryption
	configured := &options{}

	WithSupplyEncryption(encryption)(configured)

	if configured.supplyAtRest != nil {
		t.Fatalf("typed-nil encryption = %#v, want nil", configured.supplyAtRest)
	}
}

func TestProviderCheckReadinessPingsDatabase(t *testing.T) {
	databaseErr := errors.New("database unavailable")
	tests := []struct {
		name    string
		pingErr error
	}{
		{name: "healthy"},
		{name: "unavailable", pingErr: databaseErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector := &readinessConnector{pingErr: tt.pingErr}
			sqlDB := sql.OpenDB(connector)
			t.Cleanup(func() { _ = sqlDB.Close() })
			gormDB, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{DisableAutomaticPing: true})
			if err != nil {
				t.Fatalf("gorm.Open: %v", err)
			}

			err = New(gormDB).CheckReadiness(context.Background())
			if tt.pingErr == nil && err != nil {
				t.Fatalf("CheckReadiness: %v", err)
			}
			if tt.pingErr != nil && !errors.Is(err, tt.pingErr) {
				t.Fatalf("CheckReadiness error = %v, want wrapped %v", err, tt.pingErr)
			}
			if got := connector.pings.Load(); got != 1 {
				t.Fatalf("database pings = %d, want 1", got)
			}
		})
	}
}

func TestProviderCheckReadinessRequiresConfiguredDatabase(t *testing.T) {
	tests := []struct {
		name     string
		provider *Provider
	}{
		{name: "nil provider"},
		{name: "nil gorm database", provider: &Provider{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.provider.CheckReadiness(context.Background()); err == nil {
				t.Fatal("CheckReadiness error = nil, want configuration error")
			}
		})
	}
}

type readinessConnector struct {
	pingErr error
	pings   atomic.Int64
}

func (c *readinessConnector) Connect(context.Context) (driver.Conn, error) {
	return &readinessConn{connector: c}, nil
}

func (c *readinessConnector) Driver() driver.Driver {
	return readinessDriver{connector: c}
}

type readinessDriver struct {
	connector *readinessConnector
}

func (d readinessDriver) Open(string) (driver.Conn, error) {
	return &readinessConn{connector: d.connector}, nil
}

type readinessConn struct {
	connector *readinessConnector
}

func (*readinessConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (*readinessConn) Close() error                        { return nil }
func (*readinessConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c *readinessConn) Ping(ctx context.Context) error {
	c.connector.pings.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return c.connector.pingErr
	}
}
