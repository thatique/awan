package session

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/gorilla/securecookie"
	"github.com/thatique/awan/httputil"
	"github.com/thatique/awan/internal/trace"
	"github.com/thatique/awan/session/driver"
)

const pkgName = "github.com/thatique/awan/session"

var (
	latencyMeasure = trace.LatencyMeasure(pkgName)

	// OpenCensusViews are predefined views for OpenCensus metrics.
	// The views include counts and latency distributions for API method calls.
	// See the example at https://godoc.org/go.opencensus.io/stats/view for usage.
	OpenCensusViews = trace.Views(pkgName, latencyMeasure)
)

// ForceInvalidate describe how the session will be invalidated
type ForceInvalidate int

const (
	// DontForceInvalidate is default value for ForceInvalidate
	DontForceInvalidate ForceInvalidate = iota
	// CurrentSessionID tell the package to invalidate the current session,
	// and then migrate the session data to new session
	CurrentSessionID
	// AllSessionIDsOfLoggedUser invalidate all sessions of logged in user except
	// the current one
	AllSessionIDsOfLoggedUser
)

const (
	// ForceInvalidateKey is the key used to set session invalidation mode
	ForceInvalidateKey = "_forceinvalidate_"
)

// ServerSessionState hold some state in order to work, this struct hold all info
// needed.
type ServerSessionState struct {
	cookieName string
	storage    driver.Storage
	tracer     *trace.Tracer

	AuthKey         string
	CookieOptions   *httputil.CookieOptions
	Codecs          []securecookie.Codec
	IdleTimeout     int
	AbsoluteTimeout int
}

// SaveSessionToken hold data when the session loaded, this needed in save operation
type SaveSessionToken struct {
	sess *driver.Session
	now  time.Time
}

// NewServerSessionState construct a server session state
func NewServerSessionState(storage driver.Storage, keyPairs ...[]byte) *ServerSessionState {
	return &ServerSessionState{
		cookieName: "awan:session",
		storage:    storage,
		tracer: &trace.Tracer{
			Package:        pkgName,
			Provider:       trace.ProviderName(storage),
			LatencyMeasure: latencyMeasure,
		},
		Codecs:          securecookie.CodecsFromPairs(keyPairs...),
		IdleTimeout:     604800,  // 7 days
		AbsoluteTimeout: 5184000, // 60 days
		AuthKey:         "_authID",
		CookieOptions: &httputil.CookieOptions{
			Path:     "/",
			HTTPOnly: true,
		},
	}
}

// SetCookieName set a cookie name for the session
func (ss *ServerSessionState) SetCookieName(name string) error {
	if !httputil.IsCookieNameValid(name) {
		return fmt.Errorf("awan:session: invalid character in cookie name: %s", name)
	}
	ss.cookieName = name
	return nil
}

// Load session values based the provided cookieValue
func (ss *ServerSessionState) Load(ctx context.Context, cookieValue string) (data map[interface{}]interface{}, token *SaveSessionToken, err error) {
	ctx = ss.tracer.Start(ctx, "Load")
	defer func() { ss.tracer.End(ctx, err) }()

	var (
		now = time.Now().UTC()
	)

	if cookieValue != "" {
		sess, err := ss.storage.Get(ctx, cookieValue)
		if err == nil && sess != nil {
			if !sess.IsSessionExpired(ss.IdleTimeout, ss.AbsoluteTimeout, now) {
				return recomposeSession(ss.AuthKey, sess.AuthID, sess.Values), &SaveSessionToken{now: now, sess: sess}, err
			}
		}
	}

	data = make(map[interface{}]interface{})

	return data, &SaveSessionToken{now: now, sess: nil}, err
}

// Save the session data into storage, invalidate if needed
func (ss *ServerSessionState) Save(ctx context.Context, token *SaveSessionToken, data map[interface{}]interface{}) (sess *driver.Session, err error) {
	ctx = ss.tracer.Start(ctx, "Save")
	defer func() { ss.tracer.End(ctx, err) }()

	outputDecomp := decomposeSession(ss.AuthKey, data)
	sess, err = ss.invalidateIfNeeded(ctx, token.sess, outputDecomp)
	if err != nil {
		return nil, err
	}

	return ss.saveSessionOnDb(ctx, token.now, sess, outputDecomp)
}

// Invalidates an old session ID if needed. Returns the 'Session' that should be
// replaced when saving the session, if any.
//
// Currently we invalidate whenever the auth ID has changed (login, logout, different user)
// in order to prevent session fixation attacks.  We also invalidate when asked to via
// `forceInvalidate`
func (ss *ServerSessionState) invalidateIfNeeded(ctx context.Context, session *driver.Session, decomposed *decomposedSession) (sess *driver.Session, err error) {
	ctx = ss.tracer.Start(ctx, "invalidateIfNeeded")
	defer func() { ss.tracer.End(ctx, err) }()

	var (
		authID string
	)

	if session != nil && session.AuthID != "" {
		authID = session.AuthID
	}

	invalidateCurrent := decomposed.forceInvalidation != DontForceInvalidate || decomposed.authID != authID
	invalidateOthers := decomposed.forceInvalidation == AllSessionIDsOfLoggedUser && decomposed.authID != ""

	if invalidateCurrent && session != nil {
		err = ss.storage.Delete(ctx, session.ID)
		if err != nil {
			return nil, err
		}
	}

	if invalidateOthers && session != nil {
		err = ss.storage.DeleteAllOfAuthID(ctx, session.AuthID)
		if err != nil {
			return nil, err
		}
	}

	if invalidateCurrent {
		return nil, err
	}

	return session, err
}

func (ss *ServerSessionState) saveSessionOnDb(ctx context.Context, now time.Time, sess *driver.Session, dec *decomposedSession) (*driver.Session, error) {
	var err error

	ctx = ss.tracer.Start(ctx, "saveSessionOnDb")
	defer func() { ss.tracer.End(ctx, err) }()

	if sess == nil && dec.authID == "" && len(dec.decomposed) == 0 {
		return nil, err
	}

	if sess == nil {
		id := GenerateSessionID()
		sess = driver.NewSession(id, dec.authID, now)
		sess.Values = dec.decomposed

		err = ss.storage.Insert(ctx, sess)

		return sess, err
	}

	nsess := driver.NewSession(sess.ID, dec.authID, now)
	nsess.CreatedAt = sess.CreatedAt
	nsess.Values = dec.decomposed

	err = ss.storage.Replace(ctx, nsess)

	return nsess, err
}

type decomposedSession struct {
	authID            string
	forceInvalidation ForceInvalidate
	decomposed        map[interface{}]interface{}
}

func decomposeSession(authKey string, sess map[interface{}]interface{}) *decomposedSession {
	var (
		authID = ""
		force  = DontForceInvalidate
	)
	if v, ok := sess[authKey]; ok {
		delete(sess, authKey)
		authID = v.(string)
	}
	if v, ok := sess[ForceInvalidateKey]; ok {
		delete(sess, ForceInvalidateKey)
		force = v.(ForceInvalidate)
	}

	return &decomposedSession{
		authID:            authID,
		forceInvalidation: force,
		decomposed:        sess,
	}
}

func recomposeSession(authKey, authID string, sess map[interface{}]interface{}) map[interface{}]interface{} {
	if authID != "" {
		sess[authKey] = authID
	}
	return sess
}

// GenerateSessionID securely
func GenerateSessionID() string {
	return base64.URLEncoding.EncodeToString(
		securecookie.GenerateRandomKey(18))
}
