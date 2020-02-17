package union

import (
	"context"

	"github.com/thatique/awan/auth/authenticator"
	"github.com/thatique/awan/verr"
)

// unionAuthRequestHandler authenticates requests using a chain of authenticator.Requests
type unionAuthTokenHandler struct {
	// Handlers is a chain of token authenticators to delegate to
	Handlers []authenticator.Token
	// FailOnError determines whether an error returns short-circuits the chain
	FailOnError bool
}

// New returns a request authenticator that validates credentials using a chain of authenticator.Request objects.
// The entire chain is tried until one succeeds. If all fail, an aggregate error is returned.
func New(authTokenHandlers ...authenticator.Token) authenticator.Token {
	if len(authTokenHandlers) == 1 {
		return authTokenHandlers[0]
	}
	return &unionAuthTokenHandler{Handlers: authTokenHandlers, FailOnError: false}
}

// NewFailOnError returns a request authenticator that validates credentials using a chain of authenticator.Request objects.
// The first error short-circuits the chain.
func NewFailOnError(authTokenHandlers ...authenticator.Token) authenticator.Token {
	if len(authTokenHandlers) == 1 {
		return authTokenHandlers[0]
	}
	return &unionAuthTokenHandler{Handlers: authTokenHandlers, FailOnError: true}
}

// AuthenticateToken authenticates the token using a chain of authenticator.Token objects.
func (authHandler *unionAuthTokenHandler) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	var errlist []error
	for _, currAuthTokenHandler := range authHandler.Handlers {
		info, ok, err := currAuthTokenHandler.AuthenticateToken(ctx, token)
		if err != nil {
			if authHandler.FailOnError {
				return info, ok, err
			}
			errlist = append(errlist, err)
			continue
		}

		if ok {
			return info, ok, err
		}
	}

	return nil, false, verr.NewAggregate(errlist)
}
