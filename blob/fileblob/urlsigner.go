package fileblob

import (
	"context"
	"net/url"

	"github.com/thatique/awan/blob/driver"
)

// URLSigner defines an interface for creating and verifying a signed URL for
// objects in a fileblob bucket. Signed URLs are typically used for granting
// access to an otherwise-protected resource without requiring further
// authentication, and callers should take care to restrict the creation of
// signed URLs as is appropriate for their application.
type URLSigner interface {
	// URLFromKey defines how the bucket's object key will be turned
	// into a signed URL. URLFromKey must be safe to call from multiple goroutines.
	URLFromKey(ctx context.Context, key string, opts *driver.SignedURLOptions) (*url.URL, error)

	// KeyFromURL must be able to validate a URL returned from URLFromKey.
	// KeyFromURL must only return the object if if the URL is
	// both unexpired and authentic. KeyFromURL must be safe to call from
	// multiple goroutines. Implementations of KeyFromURL should not modify
	// the URL argument.
	KeyFromURL(ctx context.Context, surl *url.URL) (string, error)
}
