package health

import (
	"io"
	"net/http"
)

// Handler is an HTTP handler that reports on the success of an aggregate
// of Checkers. The zero value is always healthy.
type Handler struct {
	checkers []Checker
}

// Checker wraps the CheckHealth method.
//
// Checkhealth returns nil if the resource is healthy, or a non-nil
// error if the resource is not healthy. Checkhealth must be safe to call
// from multiple goroutine.
type Checker interface {
	CheckHealth() error
}

// Checker func is an adapter type to allow the use of ordinary functions as
// health checks. If f is a function with the appropriate signature,
// CheckerFunc(f) is a Checker that calls f.
type CheckerFunc func() error

// CheckHealth call f().
func (f CheckerFunc) CheckHealth() error {
	return f()
}

// Add adds a new check to the handler.
func (h *Handler) Add(c Checker) {
	h.checkers = append(h.checkers, c)
}

// ServerHTTP returns 200 if it is healthy, 500 otherwise
func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	for _, c := range h.checkers {
		if err := c.CheckHealth(); err != nil {
			writeUnhealthy(w)
			return
		}
	}
	writeHealthy(w)
}

func writeHeaders(statusLen string, w http.ResponseWriter) {
	w.Header().Set("Content-Length", statusLen)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func writeUnhealthy(w http.ResponseWriter) {
	const (
		status    = "unhealthy"
		statusLen = "9"
	)

	writeHeaders(statusLen, w)
	w.WriteHeader(http.StatusInternalServerError)
	io.WriteString(w, status)
}

// HandleLive is an http.HandlerFunc that handles liveness checks by
// immediately responding with an HTTP 200 status.
func HandleLive(w http.ResponseWriter, _ *http.Request) {
	writeHealthy(w)
}

func writeHealthy(w http.ResponseWriter) {
	const (
		status    = "ok"
		statusLen = "2"
	)

	writeHeaders(statusLen, w)
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, status)
}
