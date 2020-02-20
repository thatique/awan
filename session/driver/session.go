package driver

import (
	"time"

	"github.com/google/go-cmp/cmp"
)

// Session represent of a saved session
type Session struct {
	// ID of session
	ID string
	// AuthID is authentication ID
	AuthID string
	// Valuse contains the user-data for the session
	Values map[interface{}]interface{}
	// CreatedAt is creation timestamp of this session
	CreatedAt time.Time
	// AccessedAt is last time this session accessed
	AccessedAt time.Time
}

// Equal return true if two session equal
func (sess *Session) Equal(other *Session) bool {
	return sess.ID == other.ID && sess.AuthID == other.ID && cmp.Diff(sess.Values, other.Values) == ""
}

// ExpireAt return session's expiration time with the given idle and absolute timeout
// in second.
func (sess *Session) ExpireAt(idleTimeout, absoluteTimeout int) time.Time {
	var (
		idle, absolute time.Time
	)

	if idleTimeout != 0 {
		idle = sess.AccessedAt.Add(time.Second * time.Duration(idleTimeout))
	}
	if absoluteTimeout != 0 {
		absolute = sess.CreatedAt.Add(time.Second * time.Duration(absoluteTimeout))
	}

	if idle.IsZero() {
		return absolute
	}
	if absolute.IsZero() || idle.Before(absolute) {
		return idle
	}

	return absolute
}

// MaxAge returns number of seconds until this session expires. A zero or negative
/// number will expire the session immediately
func (sess *Session) MaxAge(idleTimeout, absoluteTimeout int, now time.Time) int {
	expires := sess.ExpireAt(idleTimeout, absoluteTimeout)

	if expires.IsZero() {
		return 0
	}
	if expires.Before(now) {
		return -1
	}

	return int(expires.Sub(now).Seconds())
}

// IsSessionExpired return true if the session expired or not
func (sess *Session) IsSessionExpired(idleTimeout, absoluteTimeout int, now time.Time) bool {
	expires := sess.ExpireAt(idleTimeout, absoluteTimeout)

	if !expires.IsZero() && expires.After(now) {
		return false
	}
	return true
}
