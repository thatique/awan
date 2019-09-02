package fileblob

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/url"
	"strconv"
	"time"

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

// URLSignerHMAC signs URLs by adding the object key, expiration time, and a
// hash-based message authentication code (HMAC) into the query parameters.
// Values of URLSignerHMAC with the same secret key will accept URLs produced by
// others as valid.
type URLSignerHMAC struct {
	baseURL   *url.URL
	secretKey []byte
}

// NewURLSignerHMAC creates a URLSignerHMAC. If the secret key is empty,
// then NewURLSignerHMAC panics.
func NewURLSignerHMAC(baseURL *url.URL, secretKey []byte) *URLSignerHMAC {
	if len(secretKey) == 0 {
		panic("creating URLSignerHMAC: secretKey is required")
	}
	uc := new(url.URL)
	*uc = *baseURL
	return &URLSignerHMAC{
		baseURL:   uc,
		secretKey: secretKey,
	}
}

// URLFromKey creates a signed URL by copying the baseURL and appending the
// object key, expiry, and signature as a query params.
func (h *URLSignerHMAC) URLFromKey(ctx context.Context, key string, opts *driver.SignedURLOptions) (*url.URL, error) {
	sURL := new(url.URL)
	*sURL = *h.baseURL

	q := sURL.Query()
	q.Set("obj", key)
	q.Set("expiry", strconv.FormatInt(time.Now().Add(opts.Expiry).Unix(), 10))
	q.Set("method", opts.Method)
	q.Set("signature", h.getMAC(q))
	sURL.RawQuery = q.Encode()

	return sURL, nil
}

func (h *URLSignerHMAC) getMAC(q url.Values) string {
	signedVals := url.Values{}
	signedVals.Set("obj", q.Get("obj"))
	signedVals.Set("expiry", q.Get("expiry"))
	signedVals.Set("method", q.Get("method"))
	msg := signedVals.Encode()

	hsh := hmac.New(sha256.New, h.secretKey)
	hsh.Write([]byte(msg))
	return base64.RawURLEncoding.EncodeToString(hsh.Sum(nil))
}

// KeyFromURL checks expiry and signature, and returns the object key
// only if the signed URL is both authentic and unexpired.
func (h *URLSignerHMAC) KeyFromURL(ctx context.Context, sURL *url.URL) (string, error) {
	q := sURL.Query()

	exp, err := strconv.ParseInt(q.Get("expiry"), 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return "", errors.New("retrieving blob key from URL: key cannot be retrieved")
	}

	if !h.checkMAC(q) {
		return "", errors.New("retrieving blob key from URL: key cannot be retrieved")
	}
	return q.Get("obj"), nil
}

func (h *URLSignerHMAC) checkMAC(q url.Values) bool {
	mac := q.Get("signature")
	expected := h.getMAC(q)
	// This compares the Base-64 encoded MACs
	return hmac.Equal([]byte(mac), []byte(expected))
}
