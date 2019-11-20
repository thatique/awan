package health

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestNewHandler(t *testing.T) {
	s := httptest.NewServer(new(Handler))
	defer s.Close()
	code, err := check(s)
	if err != nil {
		t.Fatalf("GET %s: %v", s.URL, err)
	}
	if code != http.StatusOK {
		t.Errorf("got HTTP status %d; want %d", code, http.StatusOK)
	}
}

func TestChecker(t *testing.T) {
	c1 := &testChecker{err: errors.New("checker 1 down")}
	c2 := &testChecker{err: errors.New("checker 2 down")}
	h := new(Handler)
	h.Add(c1)
	h.Add(c2)
	s := httptest.NewServer(h)
	defer s.Close()

	t.Run("AllUnhealthy", func(t *testing.T) {
		code, err := check(s)
		if err != nil {
			t.Fatalf("GET %s: %v", s.URL, err)
		}
		if code != http.StatusInternalServerError {
			t.Errorf("got HTTP status %d; want %d", code, http.StatusInternalServerError)
		}
	})
	c1.set(nil)
	t.Run("Partialhealthy", func(t *testing.T) {
		code, err := check(s)
		if err != nil {
			t.Fatalf("GET %s: %v", s.URL, err)
		}
		if code != http.StatusInternalServerError {
			t.Errorf("got HTTP status %d; want %d", code, http.StatusInternalServerError)
		}
	})
	c2.set(nil)
	t.Run("Allhealthy", func(t *testing.T) {
		code, err := check(s)
		if err != nil {
			t.Fatalf("GET %s: %v", s.URL, err)
		}
		if code != http.StatusOK {
			t.Errorf("got HTTP status %d; want %d", code, http.StatusInternalServerError)
		}
	})
}

func check(s *httptest.Server) (code int, err error) {
	resp, err := http.Get(s.URL)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

type testChecker struct {
	mu  sync.Mutex
	err error
}

func (c *testChecker) CheckHealth() error {
	defer c.mu.Unlock()
	c.mu.Lock()
	return c.err
}

func (c *testChecker) set(e error) {
	defer c.mu.Unlock()
	c.mu.Lock()
	c.err = e
}
