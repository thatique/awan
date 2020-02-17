package httputil

import (
	"net/http"
)

// CookieOptions store configuration for setting a HTTP cookie
type CookieOptions struct {
	Path     string
	Domain   string
	Secure   bool
	HttpOnly bool
}

// DefaultCookieOptions is a default value for CookieOptions
var DefaultCookieOptions = &CookieOptions{
	Path:     "/",
	Secure:   false,
	HttpOnly: true,
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
		HttpOnly: options.HttpOnly,
	}
}
