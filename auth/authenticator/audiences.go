package authenticator

import "context"

// Audiences is a container for the Audiences of a token
type Audiences []string

// The key type is unexported to prevent collisons
type key int

const (
	audiencesKey key = iota
)

// WithAudiences returns a context that stores a request's expected audiences.
func WithAudiences(ctx context.Context, auds Audiences) context.Context {
	return context.WithValue(ctx, audiencesKey, auds)
}

// AudiencesFrom returns a request's expected audiences stored in the request context.
func AudiencesFrom(ctx context.Context) (Audiences, bool) {
	auds, ok := ctx.Value(audiencesKey).(Audiences)
	return auds, ok
}
