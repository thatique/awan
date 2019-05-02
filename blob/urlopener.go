package blob

import (
	"context"
	"net/url"

	"github.com/thatique/awan/openurl"
)

// BucketURLOpener represents types that can open buckets based on a URL.
// The opener must not modify the URL argument. OpenBucketURL must be safe to
// call from multiple goroutines.
//
// This interface is generally implemented by types in driver packages.
type BucketURLOpener interface {
	OpenBucketURL(ctx context.Context, u *url.URL) (*Bucket, error)
}

// URLMux is a URL opener multiplexer. It matches the scheme of the URLs
// against a set of registered schemes and calls the opener that matches the
// URL's scheme.
// The zero value is a multiplexer with no registered schemes.
type URLMux struct {
	schemes openurl.SchemeMap
}

// BucketSchemes returns a sorted slice of the registered Bucket schemes.
func (mux *URLMux) BucketSchemes() []string { return mux.schemes.Schemes() }

// ValidBucketScheme returns true iff scheme has been registered for Buckets.
func (mux *URLMux) ValidBucketScheme(scheme string) bool { return mux.schemes.ValidScheme(scheme) }

// RegisterBucket registers the opener with the given scheme. If an opener
// already exists for the scheme, RegisterBucket panics.
func (mux *URLMux) RegisterBucket(scheme string, opener BucketURLOpener) {
	mux.schemes.Register("blob", "Bucket", scheme, opener)
}

// OpenBucket calls OpenBucketURL with the URL parsed from urlstr.
// OpenBucket is safe to call from multiple goroutines.
func (mux *URLMux) OpenBucket(ctx context.Context, urlstr string) (*Bucket, error) {
	opener, u, err := mux.schemes.FromString("Bucket", urlstr)
	if err != nil {
		return nil, err
	}
	return opener.(BucketURLOpener).OpenBucketURL(ctx, u)
}

// OpenBucketURL dispatches the URL to the opener that is registered with the
// URL's scheme. OpenBucketURL is safe to call from multiple goroutines.
func (mux *URLMux) OpenBucketURL(ctx context.Context, u *url.URL) (*Bucket, error) {
	opener, err := mux.schemes.FromURL("Bucket", u)
	if err != nil {
		return nil, err
	}
	return opener.(BucketURLOpener).OpenBucketURL(ctx, u)
}

var defaultURLMux = new(URLMux)

// DefaultURLMux returns the URLMux used by OpenBucket.
//
// Driver packages can use this to register their BucketURLOpener on the mux.
func DefaultURLMux() *URLMux {
	return defaultURLMux
}

// OpenBucket opens the bucket identified by the URL given.
// See the URLOpener documentation in provider-specific subpackages for
// details on supported URL formats, and https://godoc.org/gocloud.dev#hdr-URLs
// for more information.
func OpenBucket(ctx context.Context, urlstr string) (*Bucket, error) {
	return defaultURLMux.OpenBucket(ctx, urlstr)
}