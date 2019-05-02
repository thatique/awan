package mailer

import (
	"context"
	"net/url"

	"github.com/thatique/awan/openurl"
)

// TransportURLOpener is interface that can create `Transport` by the provided
// URL.
type TransportURLOpener interface {
	// OpeOpenTransportURL returns a Transport using provided `url.URL`
	OpenTransportURL(ctx context.Context, u *url.URL) (*Transport, error)
}

// URLMux is a URL opener multiplexer. It matches the scheme of the URLs
// against a set of registered schemes and calls the opener that matches the
// URL's scheme.
//
// The zero value is a multiplexer with no registered schemes.
type URLMux struct {
	schemes openurl.SchemeMap
}

// RegisterTransport registers the opener with the given scheme. If an opener
// already exists for the scheme, RegisterTransport panics.
func (mux *URLMux) RegisterTransport(scheme string, opener TransportURLOpener) {
	mux.schemes.Register("mailer", "Transport", scheme, opener)
}

// OpenTransport dispatches the URL to the opener that is registered with the
// URL's scheme. OpenTransport is safe to call from multiple goroutines.
func (mux *URLMux) OpenTransport(ctx context.Context, urlstr string) (*Transport, error) {
	opener, u, err := mux.schemes.FromString("Transport", urlstr)
	if err != nil {
		return nil, err
	}
	return opener.(TransportURLOpener).OpenTransportURL(ctx, u)
}

// OpenTransportURL dispatches the URL to the opener that is registered with the
// URL's scheme. OpenTransportURL is safe to call from multiple goroutines.
func (mux *URLMux) OpenTransportURL(ctx context.Context, u *url.URL) (*Transport, error) {
	opener, err := mux.schemes.FromURL("Transport", u)
	if err != nil {
		return nil, err
	}
	return opener.(TransportURLOpener).OpenTransportURL(ctx, u)
}

var defaultURLMux = new(URLMux)

// DefaultURLMux returns the URLMux used by OpenTransport.
//
// Driver packages can use this to register their TransportURLOpener on the mux.
func DefaultURLMux() *URLMux {
	return defaultURLMux
}

// OpenTransport opens the Keeper identified by the URL given.
// See the URLOpener documentation in provider-specific subpackages for
// details on supported URL formats
func OpenTransport(ctx context.Context, urlstr string) (*Transport, error) {
	return defaultURLMux.OpenTransport(ctx, urlstr)
}
