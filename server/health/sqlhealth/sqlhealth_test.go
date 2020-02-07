package sqlhealth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/thatique/awan/server/health"
)

var _ = health.Checker((*Checker)(nil))

func TestCheck(t *testing.T) {
	connector := new(stubConnector)
	db := sql.OpenDB(connector)
	defer db.Close()

	check := New(db)
	defer check.Stop()
	if err := check.CheckHealth(); err == nil {
		t.Error("checker starts healthy")
	}
	connector.setHealthy(true)

	// Should eventually become healthy. Otherwise, stopped by test timeout.
	for {
		if err := check.CheckHealth(); err == nil {
			break
		}
	}
}

type stubConnector struct {
	mu      sync.RWMutex
	healthy bool
}

func (c *stubConnector) setHealthy(h bool) {
	c.mu.Lock()
	c.healthy = h
	c.mu.Unlock()
}

func (c *stubConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &stubConn{c}, nil
}

func (c *stubConnector) Driver() driver.Driver {
	return nil
}

type stubConn struct {
	c *stubConnector
}

func (conn *stubConn) Prepare(query string) (driver.Stmt, error) {
	panic("not implemented")
}

func (conn *stubConn) Close() error {
	return nil
}

func (conn *stubConn) Begin() (driver.Tx, error) {
	panic("not implemented")
}

func (conn *stubConn) Ping(ctx context.Context) error {
	conn.c.mu.RLock()
	healthy := conn.c.healthy
	conn.c.mu.RUnlock()
	if !healthy {
		return errors.New("unhealthy")
	}
	return nil
}
