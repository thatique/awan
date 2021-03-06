// +build !go1.11

package httputil

import (
	"net/http"
)

// DefaultCookieOptions is a default value for CookieOptions
var DefaultCookieOptions = &CookieOptions{
	Path:     "/",
	Secure:   false,
	HTTPOnly: true,
}

// NewCookieFromOptions create http.Cookie
func NewCookieFromOptions(name, value string, maxAge int, options *CookieOptions) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     options.Path,
		Domain:   options.Domain,
		MaxAge:   maxAge,
		Secure:   options.Secure,
		HttpOnly: options.HTTPOnly,
	}
}
