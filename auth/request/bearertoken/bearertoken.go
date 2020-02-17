package bearertoken

import (
	"errors"
	"net/http"
	"strings"

	"github.com/thatique/awan/auth/authenticator"
)

var InvalidToken = errors.New("invalid bearer token")

type Authenticator struct {
	auth authenticator.Token
}

func New(auth authenticator.Token) *Authenticator {
	return &Authenticator{auth: auth}
}

func (au *Authenticator) AuthenticateRequest(r *http.Request) (*authenticator.Response, bool, error) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	// if not present ignore it
	if auth == "" {
		return nil, false, nil
	}

	parts := strings.Split(auth, " ")
	if len(parts) < 2 || strings.ToLower(parts[0]) != "bearer" {
		return nil, false, nil
	}

	token := parts[1]

	if len(token) == 0 {
		return nil, false, nil
	}

	resp, ok, err := au.auth.AuthenticateToken(r.Context(), token)
	// if we authenticated successfully, go ahead and remove the bearer token so that no one
	// is ever tempted to use it inside of the API server
	if ok {
		r.Header.Del("Authorization")
	}

	// If the token authenticator didn't error, provide a default error
	if !ok && err == nil {
		err = InvalidToken
	}

	return resp, ok, err
}
