package server

import (
	"context"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/thatique/awan/server/driver"
	"github.com/thatique/awan/server/health"
	"github.com/thatique/awan/server/httplistener"
	"github.com/thatique/awan/server/requestlog"

	"go.opencensus.io/trace"
)

// Server is a preconfigured HTTP server with diagnostic hooks
// The zero value is a server with the default options
type Server struct {
	reqlog        requestlog.Logger
	handler       http.Handler
	healthHandler health.Handler
	te            trace.Exporter
	sampler       trace.Sampler
	once          sync.Once
	driver        driver.Server
}

// Options is set of optional parameters
type Options struct {
	// RequestLogger specifies the logger that will be used to log request
	RequestLogger requestlog.Logger

	// HealthChecks specifies the health checks to be run when the
	// /healthz/readiness endpoint is requested
	HealthChecks []health.Checker

	// TraceExporter exports sampled trace spans.
	TraceExporter trace.Exporter

	// DefaultSamplingPolicy is a function that takes a
	// trace.SamplingParameters struct and returns a trus or fale decision
	// about whether it should be sampled and exported
	DefaultSamplingPolicy trace.Sampler

	// Driver serve HTTP requests
	Driver driver.Server
}

// New create a new server. New(nil, nil) is the same as new(Server)
func New(h http.Handler, opts *Options) *Server {
	srv := &Server{handler: h}
	if opts != nil {
		srv.reqlog = opts.RequestLogger
		srv.te = opts.TraceExporter
		for _, c := range opts.HealthChecks {
			srv.healthHandler.Add(c)
		}
		srv.sampler = opts.DefaultSamplingPolicy
		srv.driver = opts.Driver
	}
	return srv
}

func (srv *Server) init() {
	srv.once.Do(func() {
		if srv.te != nil {
			trace.RegisterExporter(srv.te)
		}
		if srv.sampler != nil {
			trace.ApplyConfig(trace.Config{DefaultSampler: srv.sampler})
		}
		if srv.driver == nil {
			srv.driver = NewDefaultDriver()
		}
		if srv.handler == nil {
			srv.handler = http.DefaultServeMux
		}
	})
}

// ListenAndServe is a wrapper to use wherever http.ListenAndServe is used.
// It wraps the passed-in http.Handler with a handler that handles tracing and
// request logging. If the handler is nil, then http.DefaultServerMux will be used
// A configured RequestLogger will log all requests except HealthChecks
func (srv *Server) ListenAndServe(addr string) error {
	srv.init()

	hr := "/healthz"
	hcMux := http.NewServeMux()
	hcMux.HandleFunc(path.Join(hr, "liveness"), health.HandleLive)
	hcMux.Handle(path.Join(hr, "readiness"), &srv.healthHandler)

	mux := http.NewServeMux()
	mux.Handle(hr, hcMux)
	h := srv.handler
	if srv.reqlog != nil {
		h = requestlog.NewHandler(srv.reqlog, h)
	}
	h = http.Handler(handler{h})
	mux.Handle("/", h)

	return srv.driver.ListenAndServe(addr, mux)
}

// Shutdown gracefully shuts down the server without interrupting any active connections
func (srv *Server) Shutdown(ctx context.Context) error {
	if srv.driver == nil {
		return nil
	}
	return srv.driver.Shutdown(ctx)
}

// handler wrap a http.Handler to handles tracing through OpenCensus for users.
type handler struct {
	h http.Handler
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), r.URL.Host+r.URL.Path)
	defer span.End()

	r = r.WithContext(ctx)
	h.h.ServeHTTP(w, r)
}

// DefaultDriver implements the driver.Server interface. The zero value is a valid http.Server.
type DefaultDriver struct {
	Net    string // either tcp or unix
	Server http.Server
}

// NewDefaultDriver creates a driver with an http.Server with default timeouts.
func NewDefaultDriver() *DefaultDriver {
	return &DefaultDriver{
		Net: "tcp",
		Server: http.Server{
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}
}

// ListenAndServe sets the address and handler on DefaultDriver's http.Server,
// then calls ListenAndServe on it.
func (dd *DefaultDriver) ListenAndServe(addr string, h http.Handler) error {
	ln, err := httplistener.NewListener(dd.Net, addr)
	if err != nil {
		return err
	}
	dd.Server.Handler = h
	return dd.Server.Serve(ln)
}

// Shutdown gracefully shuts down the server without interrupting any active connections,
// by calling Shutdown on DefaultDriver's http.Server
func (dd *DefaultDriver) Shutdown(ctx context.Context) error {
	return dd.Server.Shutdown(ctx)
}
