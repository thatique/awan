package memsession

import (
	"context"
	"testing"
	"time"

	"github.com/thatique/awan/session"
	"github.com/thatique/awan/session/driver"
	"github.com/thatique/awan/session/drivertest"
)

func TestConformance(t *testing.T) {
	st := &storage{sessions: map[string]*driver.Session{}}
	drivertest.RunConformanceTests(t, st)
}

func TestLoadSession(t *testing.T) {
	sess := driver.NewSession("123456789-123456789-123456789-12", "auth-id", time.Now().UTC())
	sess.Values["foo"] = "bar"

	st := &storage{sessions: map[string]*driver.Session{}}
	st.Insert(context.Background(), sess)

	ss := session.NewServerSessionState(st)
	data, _, err := ss.Load(context.Background(), "123456789-123456789-123456789-12")
	if err != nil {
		t.Errorf("Load session failed with error: %v", err)
	}

	if v, ok := data["foo"]; !ok || v != "bar" {
		t.Errorf("Expected session data contains 'foo' key with value 'bar'. Got: %v", data)
	}

	if v, ok := data[ss.AuthKey]; !ok || v != "auth-id" {
		t.Errorf("Expected session data contains '%s' key with value 'auth-id'. Got: %v", ss.AuthKey, data)
	}
}
