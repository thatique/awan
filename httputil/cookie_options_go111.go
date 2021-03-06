// +build go1.11

package httputil

import "net/http"

// CookieOptions store configuration for creating HTTP Cookie
// these fields are a subseet of http.Cookie fields
type CookieOptions struct {
	Path     string
	Domain   string
	Secure   bool
	HTTPOnly bool
	// Default to http.SameSiteDefaultMode
	SameSite http.SameSite
}
