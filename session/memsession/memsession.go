package memsession

import (
	"context"
	"sync"

	"github.com/thatique/awan/session"
	"github.com/thatique/awan/session/driver"
)

// NewServerSessionState create server session backed by memsession
func NewServerSessionState(keyPairs ...[]byte) *session.ServerSessionState {
	return session.NewServerSessionState(&storage{sessions: map[string]*driver.Session{}}, keyPairs...)
}

// Storage  implements driver's storage interface that record all operations
// performed. This is intended for testing purpose
type storage struct {
	mu       sync.Mutex
	sessions map[string]*driver.Session
}

// Get the session for the given session ID
func (s *storage) Get(ctx context.Context, id string) (*driver.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if v, ok := s.sessions[id]; ok {
		return v, nil
	}

	return nil, nil
}

// Delete a session by id
func (s *storage) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[id]; ok {
		delete(s.sessions, id)
	}

	return nil
}

// DeleteAllOfAuthID Delete all sessions of the given auth ID
func (s *storage) DeleteAllOfAuthID(ctx context.Context, authID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nmap := make(map[string]*driver.Session)
	for k, sess := range s.sessions {
		if sess.AuthID != authID {
			nmap[k] = sess
		}
	}

	s.sessions = nmap

	return nil
}

// Insert a session to the storage
func (s *storage) Insert(ctx context.Context, sess *driver.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[sess.ID]; ok {
		return driver.SessionAlreadyExists{ID: sess.ID}
	}

	s.sessions[sess.ID] = sess
	return nil
}

// Replace a session with current data
func (s *storage) Replace(ctx context.Context, sess *driver.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[sess.ID]; ok {
		s.sessions[sess.ID] = sess
		return nil
	}

	return driver.SessionDoesNotExist{ID: sess.ID}
}
