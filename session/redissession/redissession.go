package redissession

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/thatique/awan/session"
	"github.com/thatique/awan/session/driver"
)

// default session's expiration: 30 days
const defaultSessionExpire = 86400 * 30

// Option for storage
type Option func(s *storage)

// DefaultExpire set storage default expire
func DefaultExpire(expire int) Option {
	return func(s *storage) {
		s.defaultExpire = expire
	}
}

// Prefix set prefix to be used in storage
func Prefix(p string) Option {
	return func(s *storage) {
		s.prefix = p
	}
}

// Serializer set serializer to be used in storage
func Serializer(se driver.Serializer) Option {
	return func(s *storage) {
		s.serializer = se
	}
}

// IdleTimeout set default idle timeout for session
func IdleTimeout(idle int) Option {
	return func(s *storage) {
		s.idleTimeout = idle
	}
}

// AbsoluteTimeout set absolute timeout
func AbsoluteTimeout(idle int) Option {
	return func(s *storage) {
		s.idleTimeout = idle
	}
}

// Storage implements driver's storage interface backed by Redis
type storage struct {
	pool                         *redis.Pool
	defaultExpire                int
	prefix                       string
	serializer                   driver.Serializer
	idleTimeout, absoluteTimeout int
}

// NewServerSessionState create new server session backed by redis
func NewServerSessionState(pool *redis.Pool, keyPairs [][]byte, options ...Option) (*session.ServerSessionState, error) {
	rs := &storage{
		pool:            pool,
		serializer:      driver.GobSerializer,
		defaultExpire:   604800,  // 7 days
		idleTimeout:     604800,  // 7 days
		absoluteTimeout: 5184000, // 60 days
	}
	for _, option := range options {
		option(rs)
	}
	_, err := rs.ping()
	return session.NewServerSessionState(rs, keyPairs...), err
}

func (rs *storage) Get(ctx context.Context, id string) (*driver.Session, error) {
	conn, err := rs.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	data, err := redis.Values(conn.Do("HGETALL", rs.prefix+id))
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	var sess = new(sessionHash)
	if err = redis.ScanStruct(data, sess); err != nil {
		return nil, err
	}
	return sess.toSession(id, rs.serializer)
}

func (rs *storage) Delete(ctx context.Context, id string) error {
	conn, err := rs.getConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	key := rs.prefix + id
	authID, err := redis.String(conn.Do("HGET", key, "AuthID"))
	if err != nil {
		if err == redis.ErrNil {
			return nil
		}
		return err
	}

	conn.Send("MULTI")
	conn.Send("DEL", key)
	if authID != "" {
		if err = conn.Send("SREM", rs.authKey(authID), key); err != nil {
			return err
		}
	}

	_, err = conn.Do("EXEC")
	return err
}

func (rs *storage) DeleteAllOfAuthID(ctx context.Context, authID string) error {
	conn, err := rs.getConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	authKey := rs.authKey(authID)
	sessionIDs, err := redis.Strings(conn.Do("SMEMBERS", authKey))
	if err != nil {
		return err
	}
	_, err = conn.Do("DEL", redis.Args{}.Add(authKey).AddFlat(sessionIDs)...)

	return err
}

func (rs *storage) Insert(ctx context.Context, sess *driver.Session) error {
	conn, err := rs.getConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	key := rs.prefix + sess.ID
	exist, err := redis.Bool(conn.Do("EXISTS", key, "AuthID"))
	if err != nil {
		return err
	}
	if exist {
		return driver.SessionAlreadyExists{ID: sess.ID}
	}

	sh, err := newSessionHashFrom(sess, rs.serializer)
	if err != nil {
		return err
	}

	conn.Send("MULTI")
	// HMSET
	conn.Send("HMSET", redis.Args{}.Add(key).AddFlat(sh)...)
	conn.Send("EXPIRE", key, rs.getExpire(sess))

	authKey := rs.authKey(sess.AuthID)
	if authKey != "" {
		conn.Send("SADD", authKey, key)
	}

	_, err = conn.Do("EXEC")
	return err
}

func (rs *storage) Replace(ctx context.Context, sess *driver.Session) error {
	conn, err := rs.getConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	key := rs.prefix + sess.ID
	oldAuthID, err := redis.String(conn.Do("HGET", key, "AuthID"))
	if err != nil {
		if err == redis.ErrNil {
			return nil
		}
		return err
	}

	sh, err := newSessionHashFrom(sess, rs.serializer)
	if err != nil {
		return err
	}

	conn.Send("MULTI")
	// HMSET
	conn.Send("HMSET", redis.Args{}.Add(key).AddFlat(sh)...)
	conn.Send("EXPIRE", key, rs.getExpire(sess))

	authID := rs.authKey(sess.AuthID)
	if authID != oldAuthID {
		if oldAuthID != "" {
			conn.Send("SREM", oldAuthID, key)
		}

		if authID != "" {
			conn.Send("SADD", authID, key)
		}
	}

	_, err = conn.Do("EXEC")
	return err
}

func (rs *storage) authKey(authID string) string {
	if authID != "" {
		return rs.prefix + ":auth:" + authID
	}
	return ""
}

func (rs *storage) getConn() (redis.Conn, error) {
	conn := rs.pool.Get()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	return conn, nil
}

func (rs *storage) ping() (bool, error) {
	conn, err := rs.getConn()
	if err != nil {
		return false, err
	}
	defer conn.Close()

	data, err := conn.Do("PING")
	if err != nil || data == nil {
		return false, err
	}
	return (data == "PONG"), nil
}

func (rs *storage) getExpire(sess *driver.Session) int {
	expire := sess.MaxAge(rs.idleTimeout, rs.absoluteTimeout, time.Now().UTC())
	if expire <= 0 {
		return rs.defaultExpire
	}
	return expire
}

// Copy of Session field, except value to be used in "HMSET" and "HMGETALL"
type sessionHash struct {
	// Value of authentication ID, separate from rest
	AuthID string
	// Values contains the user-data for the session.
	Values []byte
	// When this session was created in UTC
	CreatedAt string
	// When this session was last accessed in UTC
	AccessedAt string
}

func newSessionHashFrom(sess *driver.Session, serializer driver.Serializer) (*sessionHash, error) {
	var sh = new(sessionHash)

	sh.AuthID = sess.AuthID
	sh.CreatedAt = sess.CreatedAt.Format(time.UnixDate)
	sh.AccessedAt = sess.AccessedAt.Format(time.UnixDate)

	bytes, err := serializer.Serialize(sess)
	if err != nil {
		return nil, err
	}

	sh.Values = bytes
	return sh, nil
}

func (sh *sessionHash) toSession(id string, serializer driver.Serializer) (*driver.Session, error) {
	createdAt, err := time.Parse(time.UnixDate, sh.CreatedAt)
	if err != nil {
		return nil, err
	}

	sess := driver.NewSession(id, sh.AuthID, createdAt)

	accessedAt, err := time.Parse(time.UnixDate, sh.AccessedAt)
	if err != nil {
		return nil, err
	}
	sess.AccessedAt = accessedAt

	err = serializer.Deserialize(sh.Values, sess)
	if err != nil {
		return nil, err
	}

	sess.ID = id
	sess.AuthID = sh.AuthID

	return sess, nil
}

// Lua script for inserting session
//
// KEYS[1] - session's ID
// KEYS[2] - Auth key
// ARGV[1] - Expiration in seconds
// ARGV... - Session Data
var insertScript = redis.NewScript(2, `
	-- now insert session data
	local sessions = {}
	for i = 2, #ARGV, 1 do
		sessions[#sessions + 1] = ARGV[i]
	end
	redis.call('HMSET', KEYS[1], unpack(sessions))
	-- expire if needed
	if(ARGV[1] ~= '') then
		redis.call('EXPIRE', KEYS[1], ARGV[1])
	end
	if(KEYS[2] ~= '') then
		redis.call('SADD', KEYS[2], KEYS[1])
	end
	return true
`)
