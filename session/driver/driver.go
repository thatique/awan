package driver

import (
	"context"
	"fmt"
)

// Storage for server-side sessions.
type Storage interface {
	// Get the session for the given session ID. Returns nil if it not exists
	// rather than returning error
	Get(ctx context.Context, id string) (*Session, error)
	// Delete the session with given session ID. Does not do anything if the session
	// is not found.
	Delete(ctx context.Context, id string) error
	// Delete all sessions of the given auth ID. Does not do anything if there
	// are no sessions of the given auth ID.
	DeleteAllOfAuthId(ctx context.Context, authID string) error
	// Insert a new session. return 'SessionAlreadyExists' error when there already
	// exists a session with the same session ID. We only call this method after
	// generating a fresh session ID
	Insert(ctx context.Context, sess *Session) error
	// Replace the contents of a session. Return 'SessionDoesNotExist' if
	// there is no session with the given  session ID
	Replace(ctx context.Context, sess *Session) error
}

// SessionAlreadyExists returned as `error` when there already exists a session
// with the same session ID in `Insert` operation
type SessionAlreadyExists struct {
	ID string
}

// Error implements error interface
func (err SessionAlreadyExists) Error() string {
	return fmt.Sprintf("There is already exists a session with the same session ID: %s", err.ID)
}

// SessionDoesNotExist returned as `error` if there is no session with given session
// ID in `Replace` operation
type SessionDoesNotExist struct {
	ID string
}

// Error implements error interface
func (err SessionDoesNotExist) Error() string {
	return fmt.Sprintf("There is no session with the given session ID: %s", err.ID)
}
