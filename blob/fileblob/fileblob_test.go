package fileblob

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/thatique/awan/blob/driver"
	"github.com/thatique/awan/blob/drivertest"
)

type harness struct {
	dir       string
	prefix    string
	server    *httptest.Server
	urlSigner URLSigner
	closer    func()
}

func newHarness(ctx context.Context, t *testing.T, prefix string) (drivertest.Harness, error) {
	dir := filepath.Join(os.TempDir(), "awan-fileblob")
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	if prefix != "" {
		if err := os.MkdirAll(filepath.Join(dir, prefix), os.ModePerm); err != nil {
			return nil, err
		}
	}
	h := &harness{dir: dir, prefix: prefix}

	localServer := httptest.NewServer(http.HandlerFunc(h.serveSignedURL))
	h.server = localServer

	u, err := url.Parse(h.server.URL)
	if err != nil {
		return nil, err
	}
	h.urlSigner = NewURLSignerHMAC(u, []byte("I'm a secret key"))
	h.closer = func() { _ = os.RemoveAll(dir); localServer.Close() }

	return h, nil
}

func (h *harness) serveSignedURL(w http.ResponseWriter, r *http.Request) {
	objKey, err := h.urlSigner.KeyFromURL(r.Context(), r.URL)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("error with objKey"))
		return
	}

	allowedMethod := r.URL.Query().Get("method")
	if allowedMethod == "" {
		allowedMethod = http.MethodGet
	}
	if allowedMethod != r.Method {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(fmt.Sprintf("allowedMethod: %s != request method: %s. %s", allowedMethod, r.Method, r.URL.Query().Encode())))
		return
	}

	bucket, err := OpenBucket(h.dir, &Options{})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer bucket.Close()

	switch r.Method {
	case http.MethodGet:
		reader, err := bucket.NewReader(r.Context(), objKey, nil)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer reader.Close()
		io.Copy(w, reader)
	case http.MethodPut:
		writer, err := bucket.NewWriter(r.Context(), objKey, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.Copy(writer, r.Body)
		if err := writer.Close(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	case http.MethodDelete:
		if err := bucket.Delete(r.Context(), objKey); err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	default:
		w.WriteHeader(http.StatusForbidden)
	}
}

func (h *harness) HTTPClient() *http.Client {
	return &http.Client{}
}

func (h *harness) MakeDriver(ctx context.Context) (driver.Bucket, error) {
	opts := &Options{
		URLSigner: h.urlSigner,
	}
	drv, err := openBucket(h.dir, opts)
	if err != nil {
		return nil, err
	}
	if h.prefix == "" {
		return drv, nil
	}
	return driver.NewPrefixedBucket(drv, h.prefix), nil
}

func (h *harness) Close() {
	h.closer()
}

func TestConformance(t *testing.T) {
	newHarnessNoPrefix := func(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
		return newHarness(ctx, t, "")
	}
	drivertest.RunConformanceTests(t, newHarnessNoPrefix)
}

func TestConformanceWithPrefix(t *testing.T) {
	const prefix = "some/prefix/dir/"
	newHarnessWithPrefix := func(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
		return newHarness(ctx, t, prefix)
	}
	drivertest.RunConformanceTests(t, newHarnessWithPrefix)
}
