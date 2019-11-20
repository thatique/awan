package driver

import (
	"context"
	"net/http"
)

// Server dispatches requests to an http.Handler.
type Server interface {
	// ListenAndServe listens on the TCP network address add and then
	// calls Serve with handler to handle requests on incoming connections.
	// The addr argument will be a non-empty string specifying "host:port".
	// The http.Handler will always be non-nill.
	// Drivers must block until serving is done (or returns an error if serving
	// can't occur for some reason), serve requests to the given http.Handler
	// and be interruptable by Shutdown.
	// Drivers should use the given address if they serve using TCP directly
	ListenAndServe(addr string, h http.Handler) error
	// Shutdown gracefully shuts down the server without interrupting
	// any active connections. If the provided context expires before
	// the shutdown is complete, Shutdown returns the context's error,
	// otherwise it returns any error returned from closing the Server's
	// underlying Listener(s).
	Shutdown(ctx context.Context) error
}
